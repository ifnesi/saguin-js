/**
 * Taking a slice of what a filter reaches.
 *
 * What saguin promises, and what these measure against. A subscriber may
 * declare `topic_hash(partitions, index)` in a `saguin-filter` User
 * Property **on the SUBSCRIBE packet**, and the broker then delivers a
 * message only where the topic's share is one of the indices declared.
 * The share is FNV-1a over the topic's UTF-8 bytes **followed by a mixing
 * step**, then a modulus. Repeating the property is an OR, which is how
 * one member holds a failed peer's share as well as its own. A subscriber
 * that declares nothing is served everything.
 *
 * RFC 0003 writes the hash out in full with worked examples, so an
 * implementation can be checked against the document rather than against
 * a running broker. That is the first test here, and it is what makes
 * `partition` usable as an oracle in the rest.
 */

import assert from 'node:assert/strict'
import { test } from 'node:test'

import {
  DECLARATION, declarations, partition, topicHash,
} from '../src/index.js'
import { caught, rejected, site, useBroker, useClients } from './fixtures.js'

const running = useBroker()
const clients = useClients(running)

// -- the hash, with no broker in it -----------------------------------------

test("the RFC's worked examples are true", () => {
  // **Both halves, because the document gives both.** An implementation
  // that stops after FNV-1a agrees with the third column and with nothing
  // else, which is exactly the mistake the table is written to let
  // somebody find.
  const examples = [
    ['iot/depot/events/dev-1', 9321193355118713112n, 16961261177379703000n, 1, 0],
    ['iot/water/w-7/inspect', 14147658934179859562n, 16406880391913018636n, 2, 4],
    ['orders/resize/thumbnails/42', 15020795679369718730n, 10596179872049748585n, 0, 1],
    ['a', 12638187200555641996n, 198367012849983736n, 1, 0],
  ]
  for (const [topic, fnv, mixed, byThree, byEight] of examples) {
    assert.equal(topicHash(topic), fnv, `${topic}: FNV-1a disagrees with the document`)
    assert.equal(partition(topic, 3), byThree, topic)
    assert.equal(partition(topic, 8), byEight, topic)
    assert.equal(Number(mixed % 3n), byThree,
      `${topic}: the document's mixed value disagrees with its own columns`)
    assert.equal(Number(mixed % 8n), byEight, topic)
  }
  assert.equal(examples.length, 4, 'the RFC writes four; this checked fewer')
})

test('the mixing step is applied', () => {
  // `topicHash` alone is not the answer to "which member holds this", and
  // a `partition` that forgot the mixing step would agree with it.
  //
  // **Over many topics rather than one**, because the two agree on one
  // topic in eight by chance.
  const topics = Array.from({ length: 200 }, (_, n) => `iot/site-${n % 7}/events/dev-${n}`)
  const differ = topics.filter(
    (one) => partition(one, 8) !== Number(topicHash(one) % 8n),
  ).length
  assert.ok(differ > topics.length / 2,
    `${topics.length - differ} of ${topics.length} topics land where the bare ` +
      'FNV-1a value puts them, so the mixing step is not being applied')
})

test('a topic scheme repeating an identifier still spreads', () => {
  // **The defect the mixing step exists for.** A topic carrying an
  // identifier twice cancels it out of FNV-1a's low bits, so without the
  // mixing step every such topic lands in a strict subset of the shares.
  const topics = Array.from({ length: 300 },
    (_, n) => `devices/d-${n}/messages/devicebound/d-${n}`)
  const shares = new Set(topics.map((one) => partition(one, 8)))
  assert.equal(shares.size, 8,
    `topics repeating their identifier reached only ${shares.size} of 8 shares`)
})

test('the hash is over bytes and not characters', () => {
  // Two topics differing only outside ASCII must hash differently, which
  // they do only if the hash reads the UTF-8 bytes.
  assert.notEqual(topicHash('iot/café'), topicHash('iot/cafe'))
  // And the same text hashes the same however it was built.
  assert.equal(topicHash('iot/café'), topicHash('iot/caf' + 'é'))
})

test('partition refuses a count that is not one or more', () => {
  for (const count of [0, -1, 1.5, '8', null]) {
    const refused = caught(() => partition('a', count))
    assert.match(refused.message, /whole number of 1 or more/)
  }
})

test('what a declaration puts on the packet', () => {
  assert.deepEqual(declarations({ topicHash: [8, 1] }),
    [[DECLARATION, 'topic_hash(8, 1)']])
  // Repeated for a member holding two slices, which the broker reads as
  // an OR: a member covering a failed peer's share.
  assert.deepEqual(declarations({ topicHash: [[8, 1], [8, 5]] }), [
    [DECLARATION, 'topic_hash(8, 1)'],
    [DECLARATION, 'topic_hash(8, 5)'],
  ])
  // A count of 1 is legal and degenerate: one slice holding everything.
  assert.deepEqual(declarations({ topicHash: [1, 0] }),
    [[DECLARATION, 'topic_hash(1, 0)']])
  assert.deepEqual(declarations(), [])
  // A call this library has not heard of goes through unchanged, so a
  // broker that has grown one does not wait for a release here.
  assert.deepEqual(declarations({ saguinFilter: 'header(region, emea, eq, 1)' }),
    [[DECLARATION, 'header(region, emea, eq, 1)']])
})

test('a declaration that cannot be right is refused here', () => {
  // Each of these is refused by every saguin there will ever be, so
  // refusing them at the call turns a reason code arriving later on a
  // SUBACK into a sentence naming the mistake.
  const cases = [
    [{ topicHash: [1, 8] }, /not below the 1 partitions/],
    [{ topicHash: [8, 8] }, /not below the 8 partitions/],
    [{ topicHash: [0, 0] }, /outside 1 to 2147483647/],
    [{ topicHash: [[8, 1], [4, 1]] }, /same number of partitions/],
    [{ topicHash: [] }, /declare nothing/],
    [{ topicHash: ['8', 1] }, /pair of whole numbers/],
    [{ saguinFilter: [] }, /declare nothing/],
    [{ saguinFilter: [''] }, /non-empty call/],
  ]
  for (const [declared, said] of cases) {
    const refused = caught(() => declarations(declared))
    assert.match(refused.message, said, JSON.stringify(declared))
  }
})

// -- against a running broker -----------------------------------------------

test('members split an append channel exactly once and in order', async () => {
  const where = site()
  const producer = await clients.producer()
  const topics = Array.from({ length: 12 }, (_, n) => `dev-${n}`)

  const members = []
  for (const index of [0, 1, 2]) {
    const member = await clients.producer({
      clientId: `member-${index}-${where}`, durable: true,
    })
    members.push({
      index,
      records: await member.append.consume('events', {
        key: [where], topicHash: [3, index], timeout: 4000,
      }),
    })
  }

  for (const device of topics) {
    await producer.append.publish('events', { key: [where, device], value: device })
  }

  const got = new Map()
  for (const member of members) {
    const mine = []
    for await (const record of member.records) mine.push(record)
    got.set(member.index, mine)
  }

  // **Every record exactly once**, and each where `partition` says it
  // belongs rather than wherever it turned up.
  const seen = [...got.values()].flat()
  assert.equal(seen.length, topics.length,
    `${seen.length} records for ${topics.length} topics`)
  for (const [index, records] of got) {
    for (const record of records) {
      assert.equal(partition(record.topic, 3), index,
        `${record.topic} went to member ${index}`)
    }
  }
})

test('a member declaring an index it does not hold receives nothing', async () => {
  const where = site()
  const producer = await clients.producer()
  const topic = 'only-one'
  const mine = partition(`iot/${where}/events/${topic}`, 8)
  const theirs = (mine + 1) % 8

  const member = await clients.producer({
    clientId: `elsewhere-${where}`, durable: true,
  })
  const records = await member.append.consume('events', {
    key: [where], topicHash: [8, theirs], timeout: 3000,
  })
  await producer.append.publish('events', { key: [where, topic], value: 'x' })

  const got = []
  for await (const record of records) got.push(record)
  assert.deepEqual(got, [],
    `a record whose share is ${mine} reached a member holding ${theirs}`)
})

test('a subscriber declaring nothing still gets everything', async () => {
  // Every client that has never heard of this, which is the reason the
  // declaration is a subscriber's to make.
  const where = site()
  const producer = await clients.producer()
  const member = await clients.producer({ clientId: `everything-${where}`, durable: true })
  const records = await member.append.consume('events', {
    key: [where], timeout: 3000,
  })

  for (const device of ['a', 'b', 'c', 'd']) {
    await producer.append.publish('events', { key: [where, device], value: device })
  }
  const got = []
  for await (const record of records) got.push(record.payload.toString())
  assert.deepEqual(got.sort(), ['a', 'b', 'c', 'd'])
})

test('a member can hold two slices at once', async () => {
  // Repeating the property is an OR, which is how a member covers a
  // failed peer's share as well as its own.
  const where = site()
  const producer = await clients.producer()
  const member = await clients.producer({ clientId: `two-slices-${where}`, durable: true })
  const records = await member.append.consume('events', {
    key: [where], topicHash: [[4, 0], [4, 1]], timeout: 3000,
  })

  const devices = Array.from({ length: 16 }, (_, n) => `dev-${n}`)
  for (const device of devices) {
    await producer.append.publish('events', { key: [where, device], value: device })
  }

  const got = []
  for await (const record of records) got.push(record.topic)
  const wanted = devices
    .map((one) => `iot/${where}/events/${one}`)
    .filter((one) => partition(one, 4) === 0 || partition(one, 4) === 1)
  assert.deepEqual(got.sort(), wanted.sort())
})

test('members split a latest channel on both halves', async () => {
  // A member sent the whole of current state and then only its share of
  // the changes would hold a copy that starts complete and drifts.
  const where = site()
  const producer = await clients.producer()
  const devices = Array.from({ length: 10 }, (_, n) => `d-${n}`)
  for (const device of devices) {
    await producer.latest.set('state', { key: [where, device], value: 'before' })
  }

  const member = await clients.producer({ clientId: `state-slice-${where}`, durable: true })
  const records = await member.latest.consume('state', {
    key: [where], topicHash: [2, 0], timeout: 3000,
  })
  for (const device of devices) {
    await producer.latest.set('state', { key: [where, device], value: 'after' })
  }

  const got = []
  for await (const record of records) got.push(record)
  assert.ok(got.length, 'the member was served nothing at all')
  for (const record of got) {
    assert.equal(partition(record.topic, 2), 0,
      `${record.topic} is not in share 0, and it arrived ` +
        `${record.isCatchUp ? 'as catch-up' : 'as a change'}`)
  }
  // Both halves reached it: the state it caught up on and the changes.
  assert.ok(got.some((one) => one.isCatchUp), 'no catch-up value reached the member')
  assert.ok(got.some((one) => !one.isCatchUp), 'no change reached the member')
})

test('broadcast takes a slice through the ordinary subscribe', async () => {
  // Broadcast has no verb, so the one place a broadcast subscriber can
  // say this is the call it already makes.
  const where = site()
  const producer = await clients.producer()
  const member = await clients.producer({ clientId: `broadcast-slice-${where}` })
  const arrived = []
  member.on('message', (record) => arrived.push(record.topic))
  await member.subscribe(`broadcast/${where}/#`, { qos: 1, topicHash: [2, 0] })

  const topics = Array.from({ length: 10 }, (_, n) => `broadcast/${where}/d-${n}`)
  for (const topic of topics) await producer.publish(topic, 'x')
  await new Promise((resolve) => setTimeout(resolve, 800))

  assert.ok(arrived.length, 'the subscriber was served nothing at all')
  for (const topic of arrived) {
    assert.equal(partition(topic, 2), 0, `${topic} is not in share 0`)
  }
  assert.ok(arrived.length < topics.length, 'every topic arrived, so no slice was taken')
})

test('a slice of a queue is refused before the broker is asked', async () => {
  // A queue already hands each job to one worker, so a slice of one is a
  // second mechanism dividing one stream.
  const worker = await clients.producer({ clientId: `no-slice-${site()}`, durable: true })
  const refused = await rejected(worker.queue.fetch('tasks', { topicHash: [3, 0] }))
  assert.match(refused.message, /queue cannot be sliced/)
})

test('the broker refuses a declaration it cannot read', async () => {
  // The half this library does not check, because only the broker knows
  // which calls it has: a call it does not recognise is refused with its
  // own sentence rather than ignored.
  const member = await clients.producer({ clientId: `nonsense-${site()}` })
  const refused = await rejected(member.subscribe(`broadcast/${site()}/#`, {
    saguinFilter: 'nonsense(1, 2)',
  }))
  assert.ok(refused.reasonCodes[0] >= 0x80, String(refused.reasonCodes))
  assert.ok(refused.reasonString, 'the broker explained itself and it was dropped')
})
