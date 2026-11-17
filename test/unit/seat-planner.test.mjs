import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { EventEmitter } from 'node:events'
import { PoolScheduler, normalizePoolRouting } from '../../src/lib/pool/pool-scheduler.mjs'
import { SeatPlanner, budgetAllows, rankSeatCandidates } from '../../src/lib/pool/seat-planner.mjs'
import { servePoolSeatStream } from '../../src/lib/admin/pool-seat-stream.mjs'
import { AccountQuota } from '../../src/lib/pool/account-quota.mjs'
import { FailoverRunner } from '../../src/lib/pool/failover-runner.mjs'
import { StickyRouter } from '../../src/lib/pool/sticky-router.mjs'
import { configureSlotCapacity } from '../../src/lib/vm/slot-capacity.mjs'

// These tests cover other behaviour on the legacy fixed 20 native slots;
// plan-based capacity has its own tests (slot-capacity*.test.mjs).
configureSlotCapacity({ enabled: false })

const MODEL = 'claude-test'

function project(specs) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'kin-seat-planner-'))
  const vms = path.join(root, 'vms')
  fs.mkdirSync(vms, { recursive: true })
  specs.forEach(({ id, policy = {}, owner = null }, index) => {
    const accountId = `account-${index + 1}`
    const vm = {
      id,
      name: id,
      status: 'running',
      schedulable: true,
      proxy_cli_enabled: true,
      proxy: { id: `proxy-${id}`, url: `socks5h://127.0.0.1:${11001 + index}` },
      runtime: { worker_socket: path.join(vms, id, 'run', 'worker.sock') },
      ...(owner ? { owner_user_id: owner } : {}),
      policy: { maxConcurrency: 4, concurrencyOverride: true, weight: 1, ...policy },
      claude: {
        account_uuid: accountId,
        account_tier: 'max',
        access_token: `access-${accountId}`,
        refresh_token: `refresh-${accountId}`,
        expires_at: Math.floor(Date.now() / 1000) + 3600,
      },
    }
    fs.writeFileSync(path.join(vms, `${id}.json`), JSON.stringify(vm))
  })
  fs.writeFileSync(path.join(vms, 'active.json'), JSON.stringify({ active_vm: specs[0].id }))
  return root
}

class RuntimeRepo {
  constructor(accounts = null) {
    this.accounts = accounts
  }
  schedulingSnapshot(ids) {
    return new Map(
      ids.map((id) => [
        id,
        {
          state: this.get(id),
          account: this.accounts?.get(id) ? { status: 'active', schedulable: true, ...this.accounts.get(id) } : null,
        },
      ]),
    )
  }
  writeHeartbeat(input) {
    this.upsert(input)
  }
  states = new Map()
  get(id) {
    return this.states.get(id) || null
  }
  clearExpired() {}
  upsert(state) {
    this.states.set(state.account_id, { ...(this.states.get(state.account_id) || {}), ...state })
  }
  markCooldown() {}
}

function scheduler(root, config = {}) {
  const pool = new PoolScheduler({
    projectRoot: root,
    runtimeRepo: new RuntimeRepo(),
    accountQuota: { canAccept: () => ({ ok: true }) },
    workerHealth: async () => ({ ok: true, credential: { generation: 1, has_access: true } }),
    config: { seat_grace_ms: 0, ...config },
  })
  pool.config.sticky_wait_timeout_ms = config.sticky ?? 2000
  pool.config.fallback_wait_timeout_ms = config.fallback ?? 2000
  return pool
}

function setup(t, specs, config) {
  const root = project(specs)
  t.after(() => fs.rmSync(root, { recursive: true, force: true }))
  return scheduler(root, config)
}

async function waitFor(check, ms = 2000) {
  const until = Date.now() + ms
  while (!check()) {
    if (Date.now() > until) throw new Error('waitFor timed out')
    await new Promise((resolve) => setTimeout(resolve, 5))
  }
}

const seat = (pool, session, extra = {}) =>
  pool.selectAndReserve({ model: MODEL, seatKey: `seat:sess:${session}`, ...extra })
/** Planner key for a platform-scope session seat. */
const planned = (session) => `platform|seat:sess:${session}`

function perVm(holds) {
  const out = {}
  for (const hold of holds) out[hold.vmId] = (out[hold.vmId] || 0) + 1
  return out
}

test('5 devices on 2×2 seats: 4 seat now, the 5th queues and the 6th waits behind it', async (t) => {
  const pool = setup(t, [
    { id: 'vm-01', policy: { sessionSlots: 2 } },
    { id: 'vm-02', policy: { sessionSlots: 2 } },
  ])
  const held = []
  for (const device of ['d1', 'd2', 'd3', 'd4']) held.push(await seat(pool, device))
  assert.ok(held.every((hold) => hold.ok))
  assert.deepEqual(perVm(held), { 'vm-01': 2, 'vm-02': 2 })
  const fifth = seat(pool, 'd5')
  await waitFor(() => pool.planner.globalQueue.length === 1)
  const sixth = seat(pool, 'd6')
  await waitFor(() => pool.planner.globalQueue.length === 2)
  assert.deepEqual(
    pool.planner.globalQueue.map((ticket) => ticket.seatKey),
    [planned('d5'), planned('d6')],
  )
  held[0].release()
  pool.planner.free(pool.planner.seatOf(planned('d1')))
  const got5 = await fifth
  assert.equal(got5.ok, true)
  assert.equal(got5.vmId, held[0].vmId)
  assert.equal(got5.selectionReason, 'queued')
  assert.deepEqual(
    pool.planner.globalQueue.map((ticket) => ticket.seatKey),
    [planned('d6')],
  )
  held[1].release()
  pool.planner.free(pool.planner.seatOf(planned('d2')))
  const got6 = await sixth
  assert.equal(got6.ok, true)
  for (const hold of [...held.slice(2), got5, got6]) hold.release()
  for (const id of ['d3', 'd4', 'd5', 'd6']) pool.planner.free(pool.planner.seatOf(planned(id)))
})

test('the same device under another owner scope is another seat and cannot free it', async (t) => {
  const pool = setup(t, [
    { id: 'vm-01', policy: { sessionSlots: 1 } },
    { id: 'vm-02', policy: { sessionSlots: 1 }, owner: 'user-7' },
  ])
  const platform = await seat(pool, 'shared')
  assert.equal(platform.vmId, 'vm-01')
  const tenant = await seat(pool, 'shared', { ownerScope: { type: 'user', userId: 'user-7' } })
  assert.equal(tenant.ok, true)
  assert.equal(tenant.vmId, 'vm-02')
  assert.equal(pool.planner.openCount('vm-01'), 1)
  assert.equal(pool.planner.openCount('vm-02'), 1)
  assert.equal(pool.planner.seatOf(planned('shared')).vmId, 'vm-01')
  platform.release()
  tenant.release()
})

test('one device with 3 concurrent requests holds 1 seat; VM concurrency 2 releases them in arrival order', async (t) => {
  const pool = setup(t, [{ id: 'vm-01', policy: { sessionSlots: 2, maxConcurrency: 2 } }])
  const a = await seat(pool, 'x')
  const b = await seat(pool, 'x')
  const order = []
  const c = seat(pool, 'x').then((hold) => (order.push('c'), hold))
  await waitFor(() => (pool.planner.vmQueues.get('vm-01') || []).length === 1)
  const d = seat(pool, 'x').then((hold) => (order.push('d'), hold))
  await waitFor(() => (pool.planner.vmQueues.get('vm-01') || []).length === 2)
  assert.equal(pool.planner.openCount('vm-01'), 1)
  const snap = pool.seatSnapshot()
  assert.equal(snap.seats['vm-01'].seats_used, 1)
  assert.equal(snap.seats['vm-01'].conc_waiting, 2)
  a.release()
  const gotC = await c
  assert.deepEqual(order, ['c'])
  b.release()
  const gotD = await d
  assert.deepEqual(order, ['c', 'd'])
  assert.equal(pool.planner.openCount('vm-01'), 1)
  gotC.release()
  gotD.release()
})

test('seat cap comes from the VM override, else inference.session_slots, clamped to 20', async (t) => {
  const pool = setup(t, [
    { id: 'vm-01', policy: { sessionSlots: 3 } },
    { id: 'vm-02' },
    { id: 'vm-03', policy: { sessionSlots: 50 } },
  ])
  pool.config.default_session_slots = 5
  const caps = Object.fromEntries((await pool.eligibleCandidates({ model: MODEL })).map((c) => [c.vmId, c.seatCap]))
  assert.deepEqual(caps, { 'vm-01': 3, 'vm-02': 5, 'vm-03': 20 })
  pool.config.default_session_slots = 99
  const clamped = await pool.eligibleCandidates({ model: MODEL })
  assert.equal(clamped.find((c) => c.vmId === 'vm-02').seatCap, 20)
})

test('after the request ends the session frees the slot; sticky TTL does not keep it', async (t) => {
  const pool = setup(t, [{ id: 'vm-01', policy: { sessionSlots: 1 } }], { seat_grace_ms: 0 })
  pool.planner.stickyTtlMs = () => 86_400_000
  const owner = await seat(pool, '01a17519-60b0-7218-9d67-d31c47ceedbc')
  assert.equal(owner.ok, true)
  assert.equal(owner.vmId, 'vm-01')
  owner.release()
  assert.equal(pool.planner.openCount('vm-01'), 0)
  assert.equal(pool.seatSnapshot().seats['vm-01'], undefined)
  const again = await seat(pool, '01a17519-60b0-7218-9d67-d31c47ceedbc', { allowWait: false })
  assert.equal(again.ok, true)
  assert.equal(again.vmId, 'vm-01')
  again.release()
  const other = await seat(pool, 'other-session', { allowWait: false })
  assert.equal(other.ok, true)
  assert.equal(other.vmId, 'vm-01')
  assert.equal(pool.planner.openCount('vm-01'), 1)
  other.release()
  assert.equal(pool.planner.openCount('vm-01'), 0)
})

test('a restarted scheduler does not restore an ended session seat from sticky VM memory', async (t) => {
  const pool = setup(t, [{ id: 'vm-01', policy: { sessionSlots: 1 } }])
  const sessionA = '01a17519-60b0-7218-9d67-d31c47ceedbc'
  const held = await seat(pool, sessionA)
  assert.equal(held.vmId, 'vm-01')
  held.release()
  const expiresAt = Date.now() + 60_000
  const rows = {
    [`sess:${sessionA}`]: {
      account_id: held.accountId,
      vm_id: 'vm-01',
      expires_at: expiresAt,
      slot_index: held.slotIndex,
    },
    'dev2:same-device': { account_id: held.accountId, vm_id: 'vm-01', expires_at: expiresAt, slot_index: null },
    'family2:parent': { account_id: held.accountId, vm_id: 'vm-01', expires_at: expiresAt, slot_index: null },
    'sess:expired': { account_id: held.accountId, vm_id: 'vm-01', expires_at: Date.now() - 1, slot_index: 0 },
  }
  const restarted = new PoolScheduler({
    projectRoot: pool.projectRoot,
    runtimeRepo: new RuntimeRepo(),
    accountQuota: { canAccept: () => ({ ok: true }) },
    workerHealth: async () => ({ ok: true, credential: { generation: 1, has_access: true } }),
    stickyRouter: {
      config: { ttl_seconds: 86400 },
      repo: { all: () => rows },
      resolve: (key) => {
        const ent = rows[key]
        if (!ent || Date.now() > ent.expires_at) return null
        return { accountId: ent.account_id, vmId: ent.vm_id }
      },
    },
    config: { seat_grace_ms: 0 },
  })
  assert.equal(restarted.planner.openCount('vm-01'), 0)
  assert.equal(restarted.seatSnapshot().seats['vm-01'], undefined)
  const other = await restarted.selectAndReserve({ model: MODEL, seatKey: 'seat:sess:other-session', allowWait: false })
  assert.equal(other.ok, true)
  assert.equal(other.vmId, 'vm-01')
  other.release()
  const again = await restarted.selectAndReserve({
    model: MODEL,
    seatKey: `seat:sess:${sessionA}`,
    stickyKey: `sess:${sessionA}`,
    allowWait: false,
  })
  assert.equal(again.ok, true)
  assert.equal(again.vmId, 'vm-01')
  assert.equal(again.selectionReason, 'sticky')
  again.release()
  assert.equal(restarted.planner.openCount('vm-01'), 0)
})

test('a restarted scheduler does not turn a seatless probe sticky row into a seat', async (t) => {
  const pool = setup(t, [{ id: 'vm-01', policy: { sessionSlots: 1 } }])
  const probe = await seat(pool, 'probe-session', { skipSessionSlot: true, allowWait: false })
  assert.equal(probe.ok, true)
  assert.equal(pool.planner.openCount('vm-01'), 0)
  probe.release()
  const expiresAt = Date.now() + 60_000
  const rows = {
    'sess:probe-session': {
      account_id: probe.accountId,
      vm_id: 'vm-01',
      expires_at: expiresAt,
      slot_index: null,
    },
  }
  const restarted = new PoolScheduler({
    projectRoot: pool.projectRoot,
    runtimeRepo: new RuntimeRepo(),
    accountQuota: { canAccept: () => ({ ok: true }) },
    workerHealth: async () => ({ ok: true, credential: { generation: 1, has_access: true } }),
    stickyRouter: { config: { ttl_seconds: 86400 }, repo: { all: () => rows } },
    config: { seat_grace_ms: 0 },
  })
  assert.equal(restarted.planner.openCount('vm-01'), 0)
  assert.equal(restarted.seatSnapshot().seats['vm-01'], undefined)
  const next = await restarted.selectAndReserve({
    model: MODEL,
    seatKey: 'seat:sess:real-session',
    allowWait: false,
  })
  assert.equal(next.ok, true)
  assert.equal(next.vmId, 'vm-01')
  next.release()
  restarted.planner.free(restarted.planner.seatOf(planned('real-session')))
})

test('one device balances independent sessions; a full pool queues globally without a device wait', async (t) => {
  const pool = setup(
    t,
    [
      { id: 'vm-01', policy: { sessionSlots: 2 } },
      { id: 'vm-02', policy: { sessionSlots: 2 } },
    ],
    { sticky: 80, fallback: 80 },
  )
  const first = await seat(pool, 'sess-a', { deviceVmId: 'vm-01' })
  assert.equal(first.ok, true)
  assert.equal(first.vmId, 'vm-01')
  assert.equal(first.selectionReason, 'device-affinity')
  const sibling = await seat(pool, 'sess-b', { deviceVmId: 'vm-01' })
  assert.equal(sibling.ok, true)
  assert.equal(sibling.vmId, 'vm-02')
  assert.equal(pool.planner.openCount('vm-01'), 1)
  assert.equal(pool.planner.openCount('vm-02'), 1)
  const foreign = await seat(pool, 'sess-other', { deviceVmId: 'vm-02', allowWait: false })
  assert.equal(foreign.ok, true)
  assert.equal(foreign.vmId, 'vm-02')
  const last = await seat(pool, 'sess-last', { deviceVmId: 'vm-01', allowWait: false })
  assert.equal(last.vmId, 'vm-01')
  const crowded = seat(pool, 'sess-c', { deviceVmId: 'vm-01' })
  await waitFor(() => pool.planner.globalQueue.some((ticket) => ticket.seatKey === planned('sess-c')))
  assert.equal(pool.planner.vmQueues.get('vm-01')?.length || 0, 0)
  foreign.release()
  const got = await crowded
  assert.equal(got.ok, true)
  assert.equal(got.vmId, 'vm-02')
  assert.notEqual(got.selectionReason, 'device-affinity')
  first.release()
  sibling.release()
  last.release()
  got.release()
  assert.equal(pool.planner.openCount('vm-01'), 0)
  assert.equal(pool.planner.openCount('vm-02'), 0)
})

test('concurrent independent sessions balance before the device sticky write completes', async (t) => {
  const root = project([
    { id: 'vm-01', policy: { sessionSlots: 2 } },
    { id: 'vm-02', policy: { sessionSlots: 2 } },
  ])
  t.after(() => fs.rmSync(root, { recursive: true, force: true }))
  const sticky = new StickyRouter({ dataDir: root, config: { sticky: { enabled: true, ttl_seconds: 600 } } })
  const pool = new PoolScheduler({
    projectRoot: root,
    runtimeRepo: new RuntimeRepo(),
    accountQuota: { canAccept: () => ({ ok: true }) },
    workerHealth: async () => ({ ok: true, credential: { generation: 1, has_access: true } }),
    stickyRouter: sticky,
    config: { seat_grace_ms: 0 },
  })
  const original = pool.eligibleCandidates.bind(pool)
  pool.eligibleCandidates = async (args) => {
    await new Promise((resolve) => setTimeout(resolve, 30))
    return original(args)
  }
  const runner = new FailoverRunner({ scheduler: pool, stickyRouter: sticky })
  const deviceKey = sticky.canonicalDeviceKey('machine-1')
  let releaseReplies
  const replies = new Promise((resolve) => {
    releaseReplies = resolve
  })
  t.after(() => releaseReplies())
  const run = (sessionId) =>
    runner.run({
      requestId: sessionId,
      canonicalBody: { model: MODEL },
      model: MODEL,
      stickyKey: sticky.canonicalSessionKey(sessionId),
      seatKey: `seat:sess:${sessionId}`,
      deviceKey,
      stickyDeviceId: 'machine-1',
      callAttempt: async () => {
        await replies
        return {
          ok: true,
          status: 200,
          terminalState: 'verified',
          body: { type: 'message', content: [{ type: 'text', text: 'ok' }], stop_reason: 'end_turn', usage: {} },
        }
      },
    })
  const running = Promise.all([run('sess-a'), run('sess-b')])
  await waitFor(() => pool.planner.seats.size === 2)
  assert.equal(pool.planner.openCount('vm-01'), 1)
  assert.equal(pool.planner.openCount('vm-02'), 1)
  releaseReplies()
  const [a, b] = await running
  assert.equal(a.ok && b.ok, true)
  assert.notEqual(a.vmId, b.vmId)
  assert.equal(pool.planner.openCount(a.vmId), 0)
  assert.equal(pool.planner.openCount(a.vmId === 'vm-01' ? 'vm-02' : 'vm-01'), 0)
  assert.ok([a.vmId, b.vmId].includes(sticky.resolve(deviceKey)?.vmId))
  assert.equal(sticky.resolve(sticky.canonicalSessionKey('sess-a')).vmId, a.vmId)
  assert.equal(sticky.resolve(sticky.canonicalSessionKey('sess-b')).vmId, b.vmId)
})

test('an expired or unbound machine affinity does not send the next session back to the old VM', async (t) => {
  const root = project([
    { id: 'vm-01', policy: { sessionSlots: 2 } },
    { id: 'vm-02', policy: { sessionSlots: 2 } },
  ])
  t.after(() => fs.rmSync(root, { recursive: true, force: true }))
  const sticky = new StickyRouter({ dataDir: root, config: { sticky: { enabled: true, ttl_seconds: 600 } } })
  const pool = scheduler(root, { seat_grace_ms: 0 })
  pool.stickyRouter = sticky
  const deviceKey = sticky.canonicalDeviceKey('machine-1')
  sticky.bindDeviceAffinity(deviceKey, { accountId: 'account-1', vmId: 'vm-01' }, { countHit: false })
  sticky.repo.upsert(deviceKey, {
    ...sticky.repo.get(deviceKey),
    expires_at: Date.now() - 1,
  })
  assert.equal(pool.machineHome(deviceKey), '')
  const next = await pool.selectAndReserve({
    model: MODEL,
    seatKey: 'seat:sess:after-expiry',
    deviceKey,
    allowWait: false,
  })
  assert.equal(next.ok, true)
  assert.notEqual(next.selectionReason, 'device-affinity')
  next.release()
  pool.planner.free(pool.planner.seatOf(planned('after-expiry')))
  sticky.bindDeviceAffinity(deviceKey, { accountId: 'account-2', vmId: 'vm-02' }, { countHit: false })
  sticky.unbind(deviceKey)
  assert.equal(pool.machineHome(deviceKey), '')
  const moved = await pool.selectAndReserve({
    model: MODEL,
    seatKey: 'seat:sess:after-unbind',
    deviceKey,
    allowWait: false,
  })
  assert.equal(moved.ok, true)
  assert.notEqual(moved.selectionReason, 'device-affinity')
  assert.equal(sticky.resolve(deviceKey)?.vmId, moved.vmId)
  moved.release()
  pool.planner.free(pool.planner.seatOf(planned('after-unbind')))
})

test('budget gate: 5% headroom, 2% reserve, 2 open seats → no new seat, open seats still served', async (t) => {
  assert.equal(budgetAllows(0.05, 2, 0.02), false)
  assert.equal(budgetAllows(0.05, 1, 0.02), true)
  assert.equal(budgetAllows(null, 9, 0.02), true)
  const planner = new SeatPlanner({ reservePct: 0.02 })
  planner.open('a', { vmId: 'vm-1', cap: 4 })
  assert.equal(planner.canOpen('vm-1', 4, 0.05), true)
  planner.open('b', { vmId: 'vm-1', cap: 4 })
  assert.equal(planner.canOpen('vm-1', 4, 0.05), false)

  const pool = setup(t, [{ id: 'vm-01', policy: { sessionSlots: 4 } }])
  const check = pool.checkEligibility.bind(pool)
  pool.checkEligibility = async (args) => ({ ...(await check(args)), headroom: 0.05 })
  const d1 = await seat(pool, 'd1')
  const d2 = await seat(pool, 'd2')
  assert.equal(d1.ok && d2.ok, true)
  const d3 = await seat(pool, 'd3', { allowWait: false })
  assert.equal(d3.ok, false)
  const d1Again = await seat(pool, 'd1', { allowWait: false })
  assert.equal(d1Again.ok, true)
  assert.equal(d1Again.selectionReason, 'seat-held')
  for (const hold of [d1, d2, d1Again]) hold.release()
})

test('balanced spreads 4 devices 2/2, fill packs 4/0; headroom then vmId break ties', async (t) => {
  const specs = [
    { id: 'vm-01', policy: { sessionSlots: 4, maxConcurrency: 8 } },
    { id: 'vm-02', policy: { sessionSlots: 4, maxConcurrency: 8 } },
  ]
  const balanced = setup(t, specs)
  const spread = []
  for (const device of ['b1', 'b2', 'b3', 'b4']) spread.push(await seat(balanced, device))
  assert.deepEqual(perVm(spread), { 'vm-01': 2, 'vm-02': 2 })
  assert.equal(spread[0].selectionReason, 'balanced')
  for (const hold of spread) hold.release()

  const fill = setup(t, specs, { strategy: 'fill' })
  const packed = []
  for (const device of ['f1', 'f2', 'f3', 'f4']) packed.push(await seat(fill, device))
  assert.deepEqual(perVm(packed), { 'vm-01': 4 })
  assert.equal(packed[0].selectionReason, 'fill')
  for (const hold of packed) hold.release()

  const openOf = () => 0
  const tie = [
    { vmId: 'vm-a', seatCap: 4, headroom: 0.3 },
    { vmId: 'vm-b', seatCap: 4, headroom: 0.5 },
  ]
  assert.equal(rankSeatCandidates(tie, { strategy: 'balanced', openOf })[0].vmId, 'vm-b')
  assert.equal(rankSeatCandidates(tie, { strategy: 'fill', openOf })[0].vmId, 'vm-b')
  const prio = [{ ...tie[0], priority: 9 }, tie[1]]
  assert.equal(rankSeatCandidates(prio, { strategy: 'balanced', openOf })[0].vmId, 'vm-a')
  const even = [
    { vmId: 'vm-b', seatCap: 4, headroom: 0.4 },
    { vmId: 'vm-a', seatCap: 4, headroom: 0.4 },
  ]
  assert.equal(rankSeatCandidates(even, { strategy: 'balanced', openOf })[0].vmId, 'vm-a')
})

test('a granted global ticket keeps its original fallback deadline for native admission', async (t) => {
  const pool = setup(t, [{ id: 'vm-01', policy: { sessionSlots: 1 } }], { fallback: 1000, sticky: 5000 })
  const held = await seat(pool, 'held')
  const started = Date.now()
  const pending = seat(pool, 'queued', { deadline: started + 8000 })
  await waitFor(() => pool.planner.pending() === 1)
  const ticketDeadline = pool.planner.globalQueue[0].deadline
  held.release()
  const granted = await pending
  assert.equal(granted.ok, true)
  assert.equal(granted.slotWaitDeadlineMs, ticketDeadline)
  granted.release()
  const noWait = await seat(pool, 'no-wait', { allowWait: false })
  assert.equal(noWait.ok, true)
  assert.ok(noWait.slotWaitDeadlineMs <= Date.now())
  noWait.release()
})

test('queue timeout is pool_queue_timeout', async (t) => {
  const pool = setup(t, [{ id: 'vm-01', policy: { sessionSlots: 1 } }], { fallback: 80 })
  const held = await seat(pool, 'd1')
  const started = Date.now()
  const late = await seat(pool, 'd2')
  assert.equal(late.ok, false)
  assert.equal(late.reason, 'pool_queue_timeout')
  assert.ok(Date.now() - started >= 70)
  assert.equal(pool.planner.pending(), 0)
  held.release()
})

test('queue_max=2: the 3rd waiter is refused at once; a freed place admits the next', async (t) => {
  const pool = setup(t, [{ id: 'vm-01', policy: { sessionSlots: 1 } }], { queue_max: 2 })
  const held = await seat(pool, 'd1')
  const cancel = new AbortController()
  const second = seat(pool, 'd2', { signal: cancel.signal })
  const third = seat(pool, 'd3')
  await waitFor(() => pool.queuedTotal() === 2)
  const started = Date.now()
  await assert.rejects(seat(pool, 'd4'), { code: 'pool_wait_queue_full' })
  assert.ok(Date.now() - started < 200)
  cancel.abort()
  await assert.rejects(second, { code: 'selection_cancelled' })
  assert.equal(pool.queuedTotal(), 1)
  const fifth = seat(pool, 'd5')
  await waitFor(() => pool.queuedTotal() === 2)
  held.release()
  pool.planner.free(pool.planner.seatOf(planned('d1')))
  const got3 = await third
  got3.release()
  pool.planner.free(pool.planner.seatOf(planned('d3')))
  const got5 = await fifth
  got5.release()
  pool.planner.free(pool.planner.seatOf(planned('d5')))
})

test('pool routing normalization: queue_max clamps, legacy strategy, dropped per-account cap', () => {
  assert.equal(normalizePoolRouting({}).queue_max, 50)
  assert.equal(normalizePoolRouting({ queue_max: 0 }).queue_max, 1)
  assert.equal(normalizePoolRouting({ queue_max: 1000 }).queue_max, 999)
  for (const legacy of ['weighted-round-robin', 'round-robin', 'lru', 'fill-first', 'nope']) {
    assert.equal(normalizePoolRouting({ strategy: legacy }).strategy, 'balanced')
  }
  assert.equal(normalizePoolRouting({ strategy: 'fill' }).strategy, 'fill')
  const next = normalizePoolRouting({
    max_waiters_per_account: 100,
    seat_grace_ms: 999999,
    seat_budget_reserve_pct: 0.9,
  })
  assert.equal(Object.hasOwn(next, 'max_waiters_per_account'), false)
  assert.equal(next.seat_grace_ms, 120000)
  assert.equal(next.seat_budget_reserve_pct, 0.5)
  assert.equal(normalizePoolRouting({}).seat_budget_reserve_pct, 0.02)
})

test('seatless and pinned requests bypass the planner', async (t) => {
  const pool = setup(t, [{ id: 'vm-01', policy: { sessionSlots: 1 } }])
  const probe = await seat(pool, 'p', { skipSessionSlot: true, allowWait: false })
  const pinned = await seat(pool, 'q', { pinVmId: 'vm-01', allowWait: false })
  assert.equal(probe.ok && pinned.ok, true)
  assert.equal(pool.planner.openCount('vm-01'), 0)
  probe.release()
  pinned.release()
})

test('failover off a VM frees the old seat at once and opens one on the new VM', async (t) => {
  const pool = setup(
    t,
    [
      { id: 'vm-01', policy: { sessionSlots: 1 } },
      { id: 'vm-02', policy: { sessionSlots: 1 } },
    ],
    { seat_grace_ms: 30000 },
  )
  const first = await seat(pool, 'dev')
  const home = first.vmId
  first.release()
  assert.equal(pool.planner.openCount(home), 0)
  const other = home === 'vm-01' ? 'vm-02' : 'vm-01'
  const moved = await seat(pool, 'dev', { excluded: new Set([first.accountId]), allowWait: false })
  assert.equal(moved.ok, true)
  assert.equal(moved.vmId, other)
  assert.equal(moved.seatMoved, false)
  assert.equal(pool.planner.openCount(home), 0)
  assert.equal(pool.planner.seatOf(planned('dev')).vmId, other)
  moved.release()
  assert.equal(pool.planner.openCount(other), 0)
})

test('a rehomed concurrency wait starts at the move and stays on that VM', async (t) => {
  const pool = setup(
    t,
    [
      { id: 'vm-01', policy: { sessionSlots: 1, maxConcurrency: 1 } },
      { id: 'vm-02', policy: { sessionSlots: 1, maxConcurrency: 1 } },
    ],
    { fallback: 80, sticky: 500 },
  )
  const heldA = await seat(pool, 'a')
  const heldB = await seat(pool, 'b')
  const home = heldA.vmId
  const first = seat(pool, 'c')
  const second = seat(pool, 'c')
  await waitFor(() => pool.planner.globalQueue.length === 2)
  heldA.release()
  pool.planner.free(pool.planner.seatOf(planned('a')))
  const gotFirst = await first
  assert.equal(gotFirst.ok, true)
  assert.equal(gotFirst.vmId, home)
  await waitFor(() => (pool.planner.vmQueues.get(home) || []).some((ticket) => ticket.kind === 'conc'))
  await new Promise((resolve) => setTimeout(resolve, 180))
  assert.equal(pool.planner.globalQueue.length, 0)
  assert.equal(
    (pool.planner.vmQueues.get(home) || []).some((ticket) => ticket.seatKey === planned('c')),
    true,
  )
  assert.equal(pool.planner.seatOf(planned('c')).vmId, home)
  gotFirst.release()
  const gotSecond = await second
  assert.equal(gotSecond.ok, true)
  assert.equal(gotSecond.vmId, home)
  assert.equal(gotSecond.reason, undefined)
  gotSecond.release()
  heldB.release()
  pool.planner.free(pool.planner.seatOf(planned('b')))
  pool.planner.free(pool.planner.seatOf(planned('c')))
})

test('a follow-up already queued on the VM keeps the slot after the current request ends', async (t) => {
  const pool = setup(t, [{ id: 'vm-01', policy: { sessionSlots: 2, maxConcurrency: 1 } }], {
    sticky: 5000,
    fallback: 5000,
  })
  const heldA = await seat(pool, 'a')
  const heldB = await seat(pool, 'b')
  const waitingC = seat(pool, 'c')
  await waitFor(() => pool.planner.globalQueue.length === 1)
  const againA = seat(pool, 'a')
  await waitFor(() => (pool.planner.vmQueues.get('vm-01') || []).some((ticket) => ticket.seatKey === planned('a')))
  heldA.release()
  const gotA = await againA
  assert.equal(gotA.ok, true)
  assert.equal(gotA.vmId, 'vm-01')
  assert.equal(gotA.selectionReason, 'queued')
  assert.equal(pool.planner.seatOf(planned('a'))?.live, true)
  assert.deepEqual(
    pool.planner.globalQueue.map((ticket) => ticket.seatKey),
    [planned('c')],
  )
  // B's request ended while A was waiting for concurrency, so its slot is already gone.
  assert.equal(heldB.ok, false)
  gotA.release()
  assert.equal(pool.planner.seatOf(planned('a')), null)
  const gotC = await waitingC
  assert.equal(gotC.ok, true)
  assert.equal(gotC.vmId, 'vm-01')
  gotC.release()
  assert.equal(pool.planner.openCount('vm-01'), 0)
})

test('a sticky seat wait leaves a home VM that stopped being eligible', async (t) => {
  const pool = setup(
    t,
    [
      { id: 'vm-01', policy: { sessionSlots: 1, maxConcurrency: 2 } },
      { id: 'vm-02', policy: { sessionSlots: 1, maxConcurrency: 2 } },
    ],
    { sticky: 8000, fallback: 8000 },
  )
  const heldA = await seat(pool, 'a', { deviceVmId: 'vm-01' })
  const heldB = await seat(pool, 'b', { deviceVmId: 'vm-02' })
  assert.equal(heldA.vmId, 'vm-01')
  assert.equal(heldB.vmId, 'vm-02')
  pool.stickyRouter = {
    resolve: (key) => (key === 'sess:c' ? { vmId: 'vm-01', accountId: 'account-1' } : null),
    unbind() {},
  }
  const waiting = seat(pool, 'c', { stickyKey: 'sess:c', deviceVmId: 'vm-01' })
  await waitFor(() => (pool.planner.vmQueues.get('vm-01') || []).some((ticket) => ticket.seatKey === planned('c')))
  pool.runtimeRepo.upsert({ account_id: 'account-1', rate_limit_reset_at: Date.now() + 60_000 })
  await waitFor(() => pool.planner.globalQueue.some((ticket) => ticket.seatKey === planned('c')), 4500)
  assert.equal(
    (pool.planner.vmQueues.get('vm-01') || []).some((ticket) => ticket.seatKey === planned('c')),
    false,
  )
  heldB.release()
  pool.planner.free(pool.planner.seatOf(planned('b')))
  const got = await waiting
  assert.equal(got.ok, true)
  assert.equal(got.vmId, 'vm-02')
  assert.equal(got.selectionReason, 'queued')
  got.release()
  heldA.release()
  pool.planner.free(pool.planner.seatOf(planned('a')))
  pool.planner.free(pool.planner.seatOf(planned('c')))
})

test('a queued sticky session is not rehomed when another VM gains priority', async (t) => {
  const pool = setup(
    t,
    [
      { id: 'vm-01', policy: { priority: 1, sessionSlots: 1 } },
      { id: 'vm-02', policy: { priority: 1, sessionSlots: 1 } },
    ],
    { sticky: 5000, fallback: 5000 },
  )
  const stickyKey = 'p:anthropic:sess:continuation'
  pool.stickyRouter = {
    resolve: (key) => (key === stickyKey ? { vmId: 'vm-01', accountId: 'account-1' } : null),
  }
  const held = await seat(pool, 'occupant', { deviceVmId: 'vm-01' })
  assert.equal(held.vmId, 'vm-01')
  const pending = seat(pool, 'continuation', { stickyKey, deadline: Date.now() + 4000 })
  const getTicket = () =>
    pool.planner.vmQueues.get('vm-01')?.find((ticket) => ticket.seatKey === planned('continuation'))
  await waitFor(() => !!getTicket()?.wake)
  const ticket = getTicket()
  const otherVmFile = path.join(pool.projectRoot, 'vms', 'vm-02.json')
  const otherVm = JSON.parse(fs.readFileSync(otherVmFile, 'utf8'))
  otherVm.policy.priority = 10
  fs.writeFileSync(otherVmFile, JSON.stringify(otherVm))
  ticket.wake()
  await waitFor(() => ticket.candidates.get('vm-02')?.priority === 10)
  assert.equal(ticket.kind, 'seat')
  held.release()
  const resumed = await pending
  assert.equal(resumed.ok, true)
  assert.equal(resumed.vmId, 'vm-01')
  resumed.release()
})

test('seat stream frames carry per-VM seats, global queue and queue_max', async (t) => {
  const pool = setup(t, [{ id: 'vm-01', policy: { sessionSlots: 1 } }])
  let changes = 0
  pool.on('change', () => changes++)
  const req = new EventEmitter()
  const res = new EventEmitter()
  const frames = []
  res.write = (chunk) => frames.push(String(chunk))
  servePoolSeatStream({ req, res, getScheduler: () => pool, writeSSEHeaders() {}, throttleMs: 1, pingMs: 60_000 })
  const held = await seat(pool, 'd1')
  await waitFor(() => frames.length >= 2)
  assert.ok(changes > 0)
  const last = JSON.parse(frames.at(-1).split('data: ')[1])
  assert.equal(last.seats['vm-01'].seats_used, 1)
  assert.equal(last.seats['vm-01'].inflight, 1)
  assert.equal(last.global_queue_depth, 0)
  assert.equal(last.queue_max, 50)
  assert.equal(Object.hasOwn(last, 'holds'), false)
  const before = frames.length
  held.release()
  await waitFor(() => frames.length > before)
  const after = JSON.parse(frames.at(-1).split('data: ')[1])
  assert.equal(after.seats['vm-01']?.inflight ?? 0, 0, 'release pushes a frame with the request gone')
  req.emit('close')
})

function gatedQuota(root) {
  const tier = { limit_5h: 0.8, limit_7d: 0.8, max_concurrency: 4 }
  return new AccountQuota({
    dataDir: fs.mkdtempSync(path.join(root, 'quota-')),
    config: {
      quota: { block_on_5h: true, block_on_7d: true, critical_band: 0.05, gate_overshoot: 0.05 },
      tiers: { default: tier, pro: tier, max: tier },
    },
  })
}

function gatedSetup(t, specs) {
  const root = project(specs)
  t.after(() => fs.rmSync(root, { recursive: true, force: true }))
  const quota = gatedQuota(root)
  specs.forEach((vm, index) => quota.ensure({ account_id: `account-${index + 1}`, vm_id: vm.id }))
  const pool = new PoolScheduler({
    projectRoot: root,
    runtimeRepo: new RuntimeRepo(quota.repo),
    accountQuota: quota,
    workerHealth: async () => ({ ok: true, credential: { generation: 1, has_access: true } }),
    config: { seat_grace_ms: 30000 },
  })
  pool.config.sticky_wait_timeout_ms = 2000
  pool.config.fallback_wait_timeout_ms = 2000
  return { pool, quota }
}

function setUtil5h(quota, accountId, value) {
  quota.ingestHeaders(accountId, {
    'anthropic-ratelimit-unified-5h-utilization': String(value),
    'anthropic-ratelimit-unified-5h-status': 'allowed',
  })
}

test('near the gate a seated device waits on its home, one call at a time, and never moves', async (t) => {
  const { pool, quota } = gatedSetup(t, [
    { id: 'vm-01', policy: { sessionSlots: 4 } },
    { id: 'vm-02', policy: { sessionSlots: 4 } },
  ])
  const first = await seat(pool, 'dev', { deviceVmId: 'vm-01' })
  assert.equal(first.vmId, 'vm-01')
  // 76% of an 80% line: inside the 5-point band, below the line.
  setUtil5h(quota, first.accountId, 0.76)
  const second = seat(pool, 'dev')
  await waitFor(() => (pool.planner.vmQueues.get('vm-01') || []).some((ticket) => ticket.seatKey === planned('dev')))
  assert.equal(quota.inflight.get(first.accountId), 1, 'the second call does not run beside the first')
  first.release()
  const got = await second
  assert.equal(got.ok, true)
  assert.equal(got.vmId, 'vm-01')
  got.release()
  assert.equal(pool.planner.openCount('vm-01'), 0)
  // The dialog ended, so the next turn opens a seat again. Inside the overshoot
  // the old home is draining and an open VM is preferred.
  setUtil5h(quota, first.accountId, 0.83)
  const third = await seat(pool, 'dev', { deviceVmId: 'vm-01', allowWait: false })
  assert.equal(third.vmId, 'vm-02')
  assert.equal(third.seatMoved, false)
  third.release()
  assert.equal(pool.planner.openCount('vm-02'), 0)
})

test('at line plus overshoot a seated device leaves and its old seat frees at once', async (t) => {
  const { pool, quota } = gatedSetup(t, [
    { id: 'vm-01', policy: { sessionSlots: 4 } },
    { id: 'vm-02', policy: { sessionSlots: 4 } },
  ])
  const first = await seat(pool, 'dev', { deviceVmId: 'vm-01' })
  first.release()
  setUtil5h(quota, first.accountId, 0.85)
  const moved = await seat(pool, 'dev', { allowWait: false })
  assert.equal(moved.ok, true)
  assert.equal(moved.vmId, 'vm-02')
  assert.equal(moved.seatMoved, false)
  assert.equal(pool.planner.openCount('vm-01'), 0)
  moved.release()
  assert.equal(pool.planner.openCount('vm-02'), 0)
})

test('a new device takes an open VM before a draining one, and a draining one when nothing else is open', async (t) => {
  const { pool, quota } = gatedSetup(t, [
    { id: 'vm-01', policy: { sessionSlots: 1 } },
    { id: 'vm-02', policy: { sessionSlots: 1 } },
  ])
  setUtil5h(quota, 'account-1', 0.78)
  const fresh = await seat(pool, 'new', { deviceVmId: 'vm-01', allowWait: false })
  assert.equal(fresh.vmId, 'vm-02', 'sticky home in the band does not beat an open VM')
  const last = await seat(pool, 'other', { allowWait: false })
  assert.equal(last.vmId, 'vm-01', 'the draining VM still takes a device when it is the only seat left')
  fresh.release()
  last.release()
})

test('new seats exhaust the highest priority tier before using a lower affinity VM', async (t) => {
  const { pool, quota } = gatedSetup(t, [
    { id: 'vm-01', policy: { priority: 1, sessionSlots: 1 } },
    { id: 'vm-02', policy: { priority: 10, sessionSlots: 1 } },
    { id: 'vm-03', policy: { priority: 10, sessionSlots: 1 } },
  ])
  setUtil5h(quota, 'account-2', 0.78)
  const options = { deviceVmId: 'vm-01', allowWait: false }
  const first = await seat(pool, 'first', options)
  assert.equal(first.vmId, 'vm-03')
  const next = await seat(pool, 'next', options)
  assert.equal(next.vmId, 'vm-02')
  const fallback = await seat(pool, 'fallback', options)
  assert.equal(fallback.vmId, 'vm-01')
  assert.equal(fallback.selectionReason, 'device-affinity')
  for (const hold of [first, next, fallback]) hold.release()
})

test('a released seat does not hold a new turn on its draining own VM', async (t) => {
  const { pool, quota } = gatedSetup(t, [
    { id: 'vm-01', policy: { priority: 10, sessionSlots: 1 } },
    { id: 'vm-02', policy: { priority: 1, sessionSlots: 1 } },
  ])
  pool.stickyRouter = { resolve: (key) => (key === 'conv-drain' ? { vmId: 'vm-02', accountId: 'account-2' } : null) }
  const high = await seat(pool, 'high')
  const low = await seat(pool, 'low')
  assert.equal(high.vmId, 'vm-01')
  assert.equal(low.vmId, 'vm-02')
  setUtil5h(quota, 'account-2', 0.78)
  const queued = seat(pool, 'drain', { stickyKey: 'conv-drain' })
  await waitFor(() => pool.planner.globalQueue.length + (pool.planner.vmQueues.get('vm-02') || []).length === 1)
  assert.equal(pool.planner.globalQueue.length, 1, 'no session-home wait on a draining VM')
  high.release()
  const got = await queued
  assert.equal(got.ok, true)
  assert.equal(got.vmId, 'vm-01')
  got.release()
  low.release()
})

test('a global waiter places its new seat by priority, not by the VM pumped first', async (t) => {
  const pool = setup(t, [
    { id: 'vm-01', policy: { priority: 1, sessionSlots: 1 } },
    { id: 'vm-02', policy: { priority: 10, sessionSlots: 1 } },
  ])
  const pump = pool.planner.pump.bind(pool.planner)
  const high = await seat(pool, 'high')
  const low = await seat(pool, 'low')
  const queued = seat(pool, 'queued')
  await waitFor(() => pool.planner.globalQueue.length === 1)
  pool.planner.pump = () => {}
  low.release()
  high.release()
  pool.planner.pump = pump
  pool.planner.pump('vm-01')
  const granted = await queued
  assert.equal(granted.vmId, 'vm-02')
  granted.release()
})

test('independent sessions on one device balance by occupied seat ratio', async (t) => {
  const pool = setup(t, [
    { id: 'vm-01', policy: { sessionSlots: 2 } },
    { id: 'vm-02', policy: { sessionSlots: 2 } },
  ])
  const first = await seat(pool, 'independent-a', { deviceVmId: 'vm-01', allowWait: false })
  t.after(() => first.release())
  const second = await seat(pool, 'independent-b', { deviceVmId: 'vm-01', allowWait: false })
  t.after(() => second.release?.())
  assert.equal(first.vmId, 'vm-01', 'device affinity breaks an initial occupancy tie')
  assert.equal(second.vmId, 'vm-02', 'a device is not a parent/root relationship')
  assert.equal(pool.planner.openCount('vm-01'), 1)
  assert.equal(pool.planner.openCount('vm-02'), 1)
})

test('independent session does not wait on a full device home while another VM is free', async (t) => {
  const pool = setup(t, [
    { id: 'vm-01', policy: { sessionSlots: 1 } },
    { id: 'vm-02', policy: { sessionSlots: 1 } },
  ])
  const first = await seat(pool, 'full-device-home', { deviceVmId: 'vm-01', allowWait: false })
  t.after(() => first.release())
  const second = await seat(pool, 'independent-overflow', { deviceVmId: 'vm-01', allowWait: false })
  t.after(() => second.release?.())
  assert.equal(second.ok, true)
  assert.equal(second.vmId, 'vm-02')
  assert.equal(pool.planner.seatOf(planned('full-device-home')).vmId, 'vm-01')
})

test('independent device preference does not override actual unequal seat capacities', async (t) => {
  const pool = setup(t, [
    { id: 'vm-01', policy: { sessionSlots: 2 } },
    { id: 'vm-02', policy: { sessionSlots: 4 } },
  ])
  const first = await seat(pool, 'capacity-a', { deviceVmId: 'vm-01', allowWait: false })
  const second = await seat(pool, 'capacity-b', { deviceVmId: 'vm-02', allowWait: false })
  t.after(() => {
    first.release()
    second.release()
  })
  const third = await seat(pool, 'capacity-c', { deviceVmId: 'vm-01', allowWait: false })
  t.after(() => third.release?.())
  assert.equal(third.vmId, 'vm-02', '1/4 is less occupied than 1/2')
})

test('trusted family home outranks device affinity and uses a distinct child seat', async (t) => {
  const pool = setup(t, [
    { id: 'vm-01', policy: { sessionSlots: 2 } },
    { id: 'vm-02', policy: { sessionSlots: 2 } },
  ])
  const parent = await seat(pool, 'family-parent', { deviceVmId: 'vm-02', allowWait: false })
  t.after(() => parent.release())
  const child = await seat(pool, 'family-child', {
    deviceVmId: 'vm-01',
    familyVmId: parent.vmId,
    allowWait: false,
  })
  t.after(() => child.release?.())
  assert.equal(child.vmId, parent.vmId)
  assert.notEqual(child.slotIndex, parent.slotIndex)
  assert.equal(pool.planner.openCount(parent.vmId), 2)
})

test('trusted family with a full home cannot spill a child to another account', async (t) => {
  const pool = setup(t, [
    { id: 'vm-01', policy: { sessionSlots: 1 } },
    { id: 'vm-02', policy: { sessionSlots: 1 } },
  ])
  const parent = await seat(pool, 'full-family-parent', { deviceVmId: 'vm-01', allowWait: false })
  t.after(() => parent.release())
  const child = await seat(pool, 'full-family-child', { familyVmId: parent.vmId, allowWait: false })
  t.after(() => child.release?.())
  assert.equal(child.ok, false)
  assert.equal(pool.planner.openCount('vm-02'), 0)
  assert.equal(pool.planner.seatOf(planned('full-family-parent')).vmId, parent.vmId)
})

test('trusted family times out on its account rather than entering a global spill queue', async (t) => {
  const pool = setup(
    t,
    [
      { id: 'vm-01', policy: { priority: 1, sessionSlots: 1 } },
      { id: 'vm-02', policy: { priority: 10, sessionSlots: 1 } },
    ],
    { sticky: 40, fallback: 200 },
  )
  const parent = await seat(pool, 'waiting-parent', { familyVmId: 'vm-01', allowWait: false })
  t.after(() => parent.release())
  const child = await seat(pool, 'waiting-child', { familyVmId: 'vm-01', deadline: Date.now() + 150 })
  assert.equal(child.ok, false)
  assert.equal(child.reason, 'pool_queue_timeout')
  assert.equal(pool.planner.openCount('vm-02'), 0)
  assert.equal(pool.planner.seatOf(planned('waiting-parent')).vmId, 'vm-01')
})

test('global device waiter ranks free seats by occupancy, not by which VM was pumped first', async (t) => {
  const pool = setup(t, [
    { id: 'vm-01', policy: { sessionSlots: 2 } },
    { id: 'vm-02', policy: { sessionSlots: 4 } },
  ])
  const held = []
  for (let i = 0; i < 6; i++) held.push(await seat(pool, `global-held-${i}`, { allowWait: false }))
  t.after(() => {
    for (const hold of held) hold.release()
  })
  const queued = seat(pool, 'global-device', { deviceVmId: 'vm-01' })
  await waitFor(() => pool.planner.globalQueue.length === 1)
  const pump = pool.planner.pump
  pool.planner.pump = () => {}
  for (const vmId of ['vm-01', 'vm-02']) {
    for (const hold of held.filter((hold) => hold.vmId === vmId).slice(1)) hold.release()
  }
  // A live new ticket retains its module's private rank helper if method
  // installation is rolled back; no new prototype helper is required.
  pool.enqueueTicket = () => {
    throw new Error('a retained ticket must not be recreated')
  }
  assert.equal(Object.hasOwn(PoolScheduler.prototype, 'rankNewSeats'), false)
  pool.planner.pump = pump
  pool.planner.pump('vm-01')
  const granted = await queued
  t.after(() => granted.release())
  assert.equal(granted.vmId, 'vm-02', '1/4 beats 1/2 even though device home was pumped first')
})

test('suspended v130 homes calls preserve explicit family and ambiguous device homes after method switch', async (t) => {
  const pool = setup(t, [
    { id: 'vm-01', policy: { sessionSlots: 2 } },
    { id: 'vm-02', policy: { sessionSlots: 2 } },
  ])
  const candidates = await pool.eligibleCandidates({ model: MODEL })
  let resume
  const gate = new Promise((resolve) => {
    resume = resolve
  })
  const suspended = async () => {
    await gate
    // Exact arguments emitted by the original selectSeat body after await.
    return pool.openNewSeat(planned('legacy-family'), candidates, {
      avoided: () => false,
      homes: [
        { vmId: 'vm-01', reason: 'device-affinity' },
        { vmId: 'vm-02', reason: 'family-affinity' },
      ],
      stickyHome: null,
      deviceKey: '',
    })
  }
  const pending = suspended()
  resume()
  const family = await pending
  t.after(() => family.reservation.release())
  assert.equal(family.candidate.vmId, 'vm-02')
  const ambiguous = pool.openNewSeat(planned('legacy-ambiguous'), candidates, {
    avoided: () => false,
    homes: [{ vmId: 'vm-02', reason: 'device-affinity' }],
  })
  t.after(() => ambiguous.reservation.release())
  assert.equal(ambiguous.candidate.vmId, 'vm-02', 'old deduplicated family cannot be guessed to be independent')
  const full = pool.openNewSeat(planned('legacy-full'), candidates, {
    avoided: () => false,
    homes: [{ vmId: 'vm-02', reason: 'device-affinity' }],
  })
  assert.equal(full, null, 'an ambiguous suspended frame retains the original home-full behaviour')
})

test('a new selectSeat frame supplies an account home to the original openNewSeat call shape on rollback', async (t) => {
  const pool = setup(t, [
    { id: 'vm-01', policy: { sessionSlots: 2 } },
    { id: 'vm-02', policy: { sessionSlots: 2 } },
  ])
  const current = pool.openNewSeat
  pool.openNewSeat = function (seatKey, candidates, opts) {
    // The original method ignores new options, but honours stickyHome before homes.
    assert.equal(opts.stickyHome.vmId, 'vm-02')
    return current.call(this, seatKey, candidates, {
      avoided: opts.avoided,
      homes: opts.homes,
      stickyHome: opts.stickyHome,
      deviceKey: opts.deviceKey,
    })
  }
  const child = await seat(pool, 'rollback-child', { deviceVmId: 'vm-01', familyVmId: 'vm-02', allowWait: false })
  t.after(() => child.release())
  assert.equal(child.vmId, 'vm-02')
})

function capacityHistory() {
  return {
    model: MODEL,
    metadata: {
      user_id: JSON.stringify({
        session_id: 'capacity-turn',
        device_id: 'capacity-device',
        account_uuid: 'caller-identity',
      }),
    },
    max_tokens: 32,
    thinking: { type: 'disabled' },
    messages: [
      { role: 'user', content: 'The sentinel is violet.' },
      { role: 'assistant', content: 'ACK' },
      { role: 'user', content: 'Which sentinel?' },
    ],
  }
}

function capacityQuota(body = capacityHistory()) {
  return { body }
}

async function capacityPool(t, extraSpecs = []) {
  const pool = setup(
    t,
    [{ id: 'vm-01', policy: { sessionSlots: 1 } }, { id: 'vm-02', policy: { sessionSlots: 1 } }, ...extraSpecs],
    { sticky: 45000 },
  )
  const original = pool.eligibleCandidates.bind(pool)
  pool.eligibleCandidates = async (args) => {
    const candidates = await original(args)
    for (const c of candidates) {
      c.dataPath = 'direct'
      c.vm.dataplane = 'wrap'
    }
    return candidates
  }
  const blocker = await seat(pool, 'capacity-blocker', { deviceVmId: 'vm-01', allowWait: false })
  t.after(() => blocker.release())
  const sticky = new StickyRouter({
    dataDir: pool.projectRoot,
    config: { sticky: { enabled: true, ttl_seconds: 600 } },
  })
  sticky.bind('sess:capacity-turn', { accountId: 'account-1', vmId: 'vm-01', sessionId: 'original-cli-session' })
  sticky.bind('family2:capacity-turn', { accountId: 'account-1', vmId: 'vm-01' })
  pool.stickyRouter = sticky
  return pool
}

test('capacity replay borrows after half remaining budget despite direct singleton and preserves home', async (t) => {
  const pool = await capacityPool(t)
  const home = pool.stickyRouter.resolve('sess:capacity-turn')
  const family = pool.stickyRouter.resolve('family2:capacity-turn')
  const spilled = new Set()
  const start = Date.now()
  const result = await seat(pool, 'capacity-turn', {
    stickyKey: 'sess:capacity-turn',
    familyVmId: 'vm-01',
    quotaRequest: capacityQuota(),
    capacityBorrowAllowed: true,
    spilled,
    deadline: start + 240,
  })
  t.after(() => result.release?.())
  assert.equal(result.ok, true)
  assert.equal(result.vmId, 'vm-02')
  assert.equal(result.selectionReason, 'capacity-spill')
  assert.ok(Date.now() - start >= 100)
  assert.ok(Date.now() - start < 240)
  assert.equal(spilled.has('account-1'), true)
  assert.deepEqual(pool.stickyRouter.resolve('sess:capacity-turn'), home)
  assert.deepEqual(pool.stickyRouter.resolve('family2:capacity-turn'), family)
})

test('capacity replay rejects opaque, thread, tool, unknown, lineage and untrusted caller paths', async (t) => {
  for (const kind of [
    'thread',
    'container',
    'tool',
    'thinking',
    'redacted',
    'encrypted',
    'server-tool',
    'tool-result',
    'org-state',
    'unknown',
    'family',
    'missing-context',
    'turn-control',
    'crag',
  ]) {
    await t.test(kind, async (t) => {
      const pool = await capacityPool(t)
      const quota = capacityQuota()
      if (kind === 'thread') quota.body.thread = { type: 'continue', previous_message_id: 'msg_prior' }
      if (kind === 'container') quota.body.container = 'server-state'
      if (kind === 'tool') quota.body.messages[1].content = [{ type: 'tool_use', id: 'tool1', name: 'Read', input: {} }]
      if (kind === 'thinking')
        quota.body.messages[1].content = [{ type: 'thinking', thinking: 'opaque', signature: 'org-bound' }]
      if (kind === 'redacted') quota.body.messages[1].content = [{ type: 'redacted_thinking', data: 'opaque' }]
      if (kind === 'encrypted')
        quota.body.messages[1].content = [{ type: 'text', text: 'visible', encrypted_content: 'opaque' }]
      if (kind === 'server-tool') quota.body.tools = [{ type: 'web_search_20250305', name: 'web_search' }]
      if (kind === 'tool-result')
        quota.body.messages[2].content = [{ type: 'tool_result', tool_use_id: 'tool1', content: 'result' }]
      if (kind === 'org-state')
        quota.body.metadata = { user_id: JSON.stringify({ session_id: 'capacity-turn', org_uuid: 'org-bound' }) }
      if (kind === 'unknown') quota.body.future_server_state = 'opaque'
      // Trusted lineage rejection is exercised through runOnce below.
      // Omitted internal permission retains the old caller contract.
      if (kind === 'turn-control') quota.body.messages[1].clear_at = 'server-turn'
      if (kind === 'crag') {
        const original = pool.eligibleCandidates.bind(pool)
        pool.eligibleCandidates = async (args) => {
          const cs = await original(args)
          for (const c of cs) c.vm.dataplane = 'crag'
          return cs
        }
      }
      const result = await seat(pool, 'capacity-turn', {
        stickyKey: 'sess:capacity-turn',
        familyVmId: 'vm-01',
        quotaRequest: quota,
        capacityBorrowAllowed: !['family', 'missing-context'].includes(kind),
        deadline: Date.now() + 60,
      })
      t.after(() => result.release?.())
      assert.equal(result.ok, false)
      assert.equal(pool.planner.openCount('vm-02'), 0)
    })
  }
})

test('capacity replay never splits a held seat and respects total deadline when all seats stay full', async (t) => {
  const pool = await capacityPool(t)
  const held = await seat(pool, 'capacity-turn', { deviceVmId: 'vm-02', allowWait: false })
  t.after(() => held.release())
  const result = await seat(pool, 'capacity-turn', {
    stickyKey: 'sess:capacity-turn',
    quotaRequest: capacityQuota(),
    capacityBorrowAllowed: true,
    deadline: Date.now() + 70,
  })
  t.after(() => result.release?.())
  assert.equal(result.vmId, 'vm-02')
  assert.notEqual(result.selectionReason, 'capacity-spill')
  result.release()
  held.release()
  // Fill both VMs; global phase must use the original remaining budget.
  const other = await seat(pool, 'capacity-other', { deviceVmId: 'vm-02', allowWait: false })
  assert.equal(other.ok, true)
  t.after(() => other.release())
  const start = Date.now()
  const timed = await seat(pool, 'capacity-wait', {
    stickyKey: 'sess:capacity-turn',
    familyVmId: 'vm-01',
    quotaRequest: capacityQuota(),
    capacityBorrowAllowed: true,
    deadline: start + 100,
  })
  assert.equal(timed.ok, false)
  assert.ok(Date.now() - start < 180)
})

test('capacity replay through real runner preserves session family device and outbound home on all bind phases', async (t) => {
  const pool = await capacityPool(t)
  const sticky = pool.stickyRouter
  sticky.bind('dev2:capacity-device', { accountId: 'account-1', vmId: 'vm-01' })
  const runner = new FailoverRunner({ scheduler: pool, stickyRouter: sticky, config: { total_retry_deadline_ms: 220 } })
  let executions = 0
  const home = sticky.resolve('sess:capacity-turn')
  const family = sticky.resolve('family2:capacity-turn')
  const device = sticky.resolve('dev2:capacity-device')
  const rowsBefore = structuredClone(sticky.repo.all())
  const result = await runner.run({
    requestId: 'capacity-real-runner',
    canonicalBody: capacityHistory(),
    model: MODEL,
    stickyKey: 'sess:capacity-turn',
    stickyKeys: ['sess:capacity-turn'],
    seatKey: 'seat:sess:capacity-turn',
    familyKey: 'family2:capacity-turn',
    familyVmId: 'vm-01',
    deviceKey: 'dev2:capacity-device',
    quotaRequest: capacityQuota(),
    capacityBorrowAllowed: true,
    applyAttempt: async (body, selected, extra) => {
      assert.equal(selected.selectionReason, 'capacity-spill')
      assert.equal(extra.stickyBound.sessionId, 'original-cli-session')
      assert.deepEqual(sticky.resolve('sess:capacity-turn'), home)
      return { body, meta: { sessionId: 'borrowed-cold-session' } }
    },
    callAttempt: async ({ candidate }) => {
      executions++
      assert.equal(candidate.vmId, 'vm-02')
      assert.deepEqual(sticky.resolve('sess:capacity-turn'), home)
      return {
        ok: true,
        status: 200,
        terminalState: 'verified',
        body: { type: 'message', content: [{ type: 'text', text: 'violet' }], stop_reason: 'end_turn', usage: {} },
      }
    },
  })
  assert.equal(result.ok, true)
  assert.equal(executions, 1)
  assert.deepEqual(sticky.resolve('sess:capacity-turn'), home)
  assert.deepEqual(sticky.resolve('family2:capacity-turn'), family)
  assert.deepEqual(sticky.resolve('dev2:capacity-device'), device)
  assert.deepEqual(sticky.repo.all(), rowsBefore)
  assert.equal(pool.planner.seatOf(planned('capacity-turn')), null)
})

test('capacity replay global phase does not jump an earlier global waiter', async (t) => {
  const pool = await capacityPool(t)
  const blocker = await seat(pool, 'capacity-vm2-blocker', { deviceVmId: 'vm-02', allowWait: false })
  assert.equal(blocker.ok, true)
  t.after(() => blocker.release())
  const earlierPromise = seat(pool, 'capacity-earlier', { deadline: Date.now() + 700 })
  await waitFor(() => pool.planner.globalQueue.length === 1)
  let borrowedFinished = false
  const borrowedPromise = seat(pool, 'capacity-turn', {
    stickyKey: 'sess:capacity-turn',
    familyVmId: 'vm-01',
    quotaRequest: capacityQuota(),
    capacityBorrowAllowed: true,
    deadline: Date.now() + 600,
  }).then((result) => {
    borrowedFinished = true
    return result
  })
  await waitFor(() => pool.planner.globalQueue.length === 2)
  blocker.release()
  const earlier = await earlierPromise
  t.after(() => earlier.release?.())
  assert.equal(earlier.ok, true)
  assert.equal(earlier.vmId, 'vm-02')
  assert.equal(borrowedFinished, false)
  earlier.release()
  const borrowed = await borrowedPromise
  t.after(() => borrowed.release?.())
  assert.equal(borrowed.ok, true)
  assert.equal(borrowed.selectionReason, 'capacity-spill')
})

test('capacity replay failure reports borrowed credential death without deleting original home or family DB rows', async (t) => {
  const pool = await capacityPool(t)
  const sticky = pool.stickyRouter
  const before = structuredClone(sticky.repo.all())
  const runner = new FailoverRunner({ scheduler: pool, stickyRouter: sticky, config: { total_retry_deadline_ms: 220 } })
  const result = await runner.run({
    requestId: 'borrowed-credential-failure',
    canonicalBody: capacityHistory(),
    model: MODEL,
    stickyKey: 'sess:capacity-turn',
    seatKey: 'seat:sess:capacity-turn',
    familyKey: 'family2:capacity-turn',
    familyVmId: 'vm-01',
    callAttempt: async () => ({
      ok: false,
      status: 401,
      terminalState: 'rejected',
      body: { error: { message: 'OAuth access token has been revoked' } },
    }),
  })
  assert.equal(result.ok, false)
  assert.equal(result.policy.reason, 'oauth_revoked')
  assert.deepEqual(sticky.repo.all(), before)
})

test('capacity replay zero-execution retry resumes global FIFO without a second home wait', async (t) => {
  const pool = await capacityPool(t, [{ id: 'vm-03', policy: { sessionSlots: 1 } }])
  const before = structuredClone(pool.stickyRouter.repo.all())
  const selections = []
  const tickets = []
  const select = pool.selectAndReserve.bind(pool)
  const enqueue = pool.enqueueTicket.bind(pool)
  pool.selectAndReserve = async (args) => {
    selections.push({
      allowed: args.capacityBorrowAllowed,
      continuation: args.capacityBorrowContinuation,
      deadline: args.deadline,
    })
    return select(args)
  }
  pool.enqueueTicket = (args) => {
    tickets.push(args.kind)
    return enqueue(args)
  }
  const runner = new FailoverRunner({
    scheduler: pool,
    stickyRouter: pool.stickyRouter,
    config: { total_retry_deadline_ms: 700, same_account_retry_delay_ms: 0 },
  })
  const served = []
  const result = await runner.run({
    requestId: 'capacity-zero-execution-retry',
    canonicalBody: capacityHistory(),
    model: MODEL,
    stickyKey: 'sess:capacity-turn',
    seatKey: 'seat:sess:capacity-turn',
    familyKey: 'family2:capacity-turn',
    familyVmId: 'vm-01',
    callAttempt: async ({ candidate }) => {
      served.push(candidate.vmId)
      assert.deepEqual(pool.stickyRouter.repo.all(), before)
      if (served.length === 1)
        return {
          ok: false,
          status: 503,
          upstreamExecutions: 0,
          terminalState: 'rejected',
          body: { error: { type: 'api_error', code: 'slot_busy', message: 'no free slot' } },
        }
      return {
        ok: true,
        status: 200,
        terminalState: 'verified',
        body: { type: 'message', content: [{ type: 'text', text: 'violet' }], stop_reason: 'end_turn', usage: {} },
      }
    },
  })
  assert.equal(result.ok, true)
  assert.deepEqual(served, ['vm-02', 'vm-03'])
  assert.equal(result.attemptCount, 1, 'zero-execution miss does not count as an upstream execution')
  assert.equal(tickets.filter((kind) => kind === 'seat').length, 1, 'only the first selection may wait on home')
  assert.ok(tickets.slice(1).every((kind) => kind === 'global'))
  assert.ok(tickets.includes('global'), 'retry really enters the global FIFO, not just a broad immediate scan')
  assert.equal(selections[0].allowed, true)
  assert.equal(selections[0].continuation, false)
  assert.ok(selections.slice(1).every((item) => item.allowed === false && item.continuation === true))
  assert.ok(selections.every((item) => item.deadline === selections[0].deadline))
  assert.deepEqual(pool.stickyRouter.repo.all(), before)
  assert.equal(pool.planner.seatOf(planned('capacity-turn')), null)
})
