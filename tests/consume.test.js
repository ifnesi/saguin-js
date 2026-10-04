/**
 * Reading a channel, and keeping a place in it.
 *
 * What saguin promises, and what these measure against. A durable client
 * (clean start off, a session expiry that is not zero, the same client id)
 * has a **position** stored beside the channel's own records, and the
 * position advances when the client acknowledges a record. So a consumer
 * that stops half way is served the record it never acknowledged again,
 * and one that comes back after a week reads on from where it stopped
 * rather than from a queue that overflowed.
 *
 * A `latest` channel answers a subscription with the current value of
 * every topic the filter reaches, with the RETAIN flag on, and every
 * change after that without it.
 *
 * `$saguin/consumer/<channel>/seek` moves a stored position and answers
 * the offset it landed on. `0` is the retention floor, `-1` the next
 * offset, and a duration or an RFC 3339 moment is a time.
 */

import assert from 'node:assert/strict'
import { test } from 'node:test'

import { Client, SubscriptionRefused } from '../src/index.js'
import { rejected, site, useBroker, useClients } from './fixtures.js'

const running = useBroker()
const clients = useClients(running)

/** Wait for something to become true, or fail saying what did not. */
async function waitUntil(it, said, timeout = 10_000) {
  const deadline = Date.now() + timeout
  while (Date.now() < deadline) {
    if (it()) return
    await new Promise((resolve) => setTimeout(resolve, 50))
  }
  throw new Error(said)
}

/** Read this many records and stop, leaving the loop the way an
 * application would. */
async function take(reader, count) {
  const got = []
  for await (const record of reader) {
    got.push(record)
    if (got.length >= count) break
  }
  return got
}

test('consuming by name subscribes to every alternative', async () => {
  // A braced level left open is one filter per spelling rather than a
  // `+`, which would also reach topics beside the channel.
  const where = site()
  const producer = await clients.producer()
  await producer.publish(`iot/${where}/device/temp`, '1')
  await producer.publish(`iot/${where}/sensor/humidity`, '2')

  const reading = await clients.producer({
    clientId: 'by-name-' + where, durable: true,
  })
  const records = await reading.append.consume('readings', {
    key: [where], timeout: 15_000,
  })
  const got = await take(records, 2)

  assert.deepEqual(got.map((r) => r.payload.toString()).sort(), ['1', '2'])
  assert.deepEqual(new Set(got.map((r) => r.topic)), new Set([
    `iot/${where}/device/temp`, `iot/${where}/sensor/humidity`,
  ]))
  // What the broker stamped, read through the record type: an offset, a
  // receipt time, and the publisher's id.
  assert.equal(typeof got[0].offset, 'number')
  assert.ok(got[0].timestamp instanceof Date)
  assert.ok(Math.abs(Date.now() - got[0].timestamp.getTime()) < 60_000)
  assert.match(got[0].id, /^[0-9a-f-]{36}$/)
})

test('a record left unacknowledged comes back', async () => {
  // The position advances on the acknowledgement, and a record is
  // acknowledged when the loop asks for the next one. So a reader that
  // takes one record and stops has acknowledged nothing, and the same
  // record is served again on the next connection.
  const where = site()
  const name = 'again-' + where
  const producer = await clients.producer()
  await producer.append.publish('events', { key: [where, 'thing'], value: 'once' })

  const first = new Client(name, { durable: true })
  await first.start(running.broker.url)
  const records = await first.append.consume('events', {
    key: [where], timeout: 15_000,
  })
  const one = await records.next()
  assert.equal(one.value.payload.toString(), 'once')
  // Closed without ever asking for the next record, so nothing was
  // acknowledged. `end(true)` is a link that went rather than a polite
  // goodbye.
  await first.mqtt.endAsync(true)

  const second = new Client(name, { durable: true })
  await second.start(running.broker.url)
  const again = await second.append.consume('events', {
    key: [where], timeout: 15_000,
  })
  const back = await again.next()
  assert.equal(back.value.payload.toString(), 'once',
    'the record was acknowledged before the application had read it')
  await second.close()
})

test('a record the loop moved past does not come back', async () => {
  // The other half of the same rule: what the loop asked past is
  // acknowledged, the position advanced, and it is not served again.
  const where = site()
  const name = 'moved-' + where
  const producer = await clients.producer()
  await producer.append.publish('events', { key: [where, 'thing'], value: 'a' })
  await producer.append.publish('events', { key: [where, 'thing'], value: 'b' })

  const first = new Client(name, { durable: true })
  await first.start(running.broker.url)
  const records = await first.append.consume('events', {
    key: [where], timeout: 15_000,
  })
  const got = await take(records, 2)
  assert.deepEqual(got.map((r) => r.payload.toString()), ['a', 'b'])
  // Leaving the loop acknowledges nothing further: 'b' is still in hand,
  // so only 'a' has been acknowledged.
  await first.close()

  const second = new Client(name, { durable: true })
  await second.start(running.broker.url)
  const again = await second.append.consume('events', {
    key: [where], timeout: 6000,
  })
  const left = await take(again, 5)
  assert.deepEqual(left.map((r) => r.payload.toString()), ['b'],
    'the acknowledged record was served again, or the held one was not')
  await second.close()
})

test('a client with no position starts where start says', async () => {
  // `start` is the broker's own vocabulary and is passed through: an
  // integer offset, or a string holding a duration or an RFC 3339 moment.
  const where = site()
  const producer = await clients.producer()
  for (const n of ['1', '2', '3']) {
    await producer.append.publish('events', { key: [where, 'thing'], value: n })
  }

  // -1 is the channel's next offset: only what arrives after this.
  const tail = await clients.producer({ clientId: 'tail-' + where, durable: true })
  const records = await tail.append.consume('events', {
    key: [where], start: -1, timeout: 6000,
  })
  await producer.append.publish('events', { key: [where, 'thing'], value: '4' })
  const after = await take(records, 1)
  assert.deepEqual(after.map((r) => r.payload.toString()), ['4'])

  // 0 is the retention floor: everything still held.
  const floor = await clients.producer({ clientId: 'floor-' + where, durable: true })
  const all = await floor.append.consume('events', {
    key: [where], start: 0, timeout: 10_000,
  })
  const got = await take(all, 4)
  assert.deepEqual(got.map((r) => r.payload.toString()), ['1', '2', '3', '4'])
})

test('seek moves a stored position and says where it landed', async () => {
  const where = site()
  const producer = await clients.producer()
  for (const n of ['1', '2', '3']) {
    await producer.append.publish('events', { key: [where, 'thing'], value: n })
  }

  const reading = await clients.producer({ clientId: 'seeker-' + where, durable: true })
  const landed = await reading.append.seek('events', 0)
  assert.equal(typeof landed, 'number')

  const records = await reading.append.consume('events', {
    key: [where], timeout: 10_000,
  })
  const got = await take(records, 3)
  assert.deepEqual(got.map((r) => r.payload.toString()), ['1', '2', '3'])

  // And a client that keeps no place has no position to move.
  const passing = await clients.producer()
  const refused = await rejected(passing.append.seek('events', 0))
  assert.match(refused.message, /durable/)
})

test('a latest channel answers the current value first, then the changes',
  async () => {
    const where = site()
    const producer = await clients.producer()
    await producer.latest.set('state', { key: [where, 'temp'], value: '18' })

    const reading = await clients.producer({
      clientId: 'state-' + where, durable: true,
    })
    const records = await reading.latest.consume('state', {
      key: [where], timeout: 10_000,
    })

    const first = await records.next()
    assert.equal(first.value.payload.toString(), '18')
    assert.equal(first.value.isCatchUp, true,
      'the value the consumer was catching up on did not say so')

    await producer.latest.set('state', { key: [where, 'temp'], value: '19' })
    const second = await records.next()
    assert.equal(second.value.payload.toString(), '19')
    assert.equal(second.value.isCatchUp, false, 'a change said it was catch-up')
    await records.close()
  })

test('two readers on one client take their own records', async () => {
  // With one queue per client, whichever reader asks first is handed
  // whatever arrived, whatever it subscribed to, and then acknowledges a
  // record belonging to the other subscription.
  const where = site()
  const producer = await clients.producer()
  const reading = await clients.producer({ clientId: 'two-' + where, durable: true })

  const events = await reading.append.consume('events', {
    key: [where], timeout: 10_000,
  })
  const readings = await reading.append.consume('readings', {
    key: [where], timeout: 10_000,
  })

  await producer.append.publish('readings', {
    key: [where, 'device', 'temp'], value: 'a reading',
  })
  await producer.append.publish('events', { key: [where, 'thing'], value: 'an event' })

  const [fromEvents] = await take(events, 1)
  const [fromReadings] = await take(readings, 1)
  assert.equal(fromEvents.payload.toString(), 'an event')
  assert.equal(fromReadings.payload.toString(), 'a reading')
})

test('what no reader is reading reaches the client itself', async () => {
  // Broadcast has no verb: a topic no channel claims is ordinary MQTT.
  // The client's own `message` event is what reached no reader, which is
  // what makes reading broadcast beside a channel work.
  const where = site()
  const producer = await clients.producer()
  const reading = await clients.producer({ clientId: 'mixed-' + where, durable: true })

  const unread = []
  reading.on('message', (record) => unread.push(record))
  await reading._subscribeAndCheck([`broadcast/${where}/#`], 1, 10_000)

  const records = await reading.append.consume('events', {
    key: [where], timeout: 8000,
  })
  await producer.append.publish('events', { key: [where, 'thing'], value: 'channel' })
  await producer.publish(`broadcast/${where}/shout`, 'broadcast')

  const [record] = await take(records, 1)
  assert.equal(record.payload.toString(), 'channel')

  await new Promise((resolve) => setTimeout(resolve, 500))
  assert.deepEqual(unread.map((one) => one.payload.toString()), ['broadcast'],
    'the reader\'s own record was also handed to the client')
})

test('a resumed session finds the records it was sent before it asked',
  async () => {
    // A durable client that reconnects is served records for
    // subscriptions made on its previous connection, before the
    // application has called consume for them. With nowhere to put those,
    // a durable consumer loses exactly what it reconnected for.
    const where = site()
    const name = 'resumed-' + where
    const producer = await clients.producer()

    const first = new Client(name, { durable: true })
    await first.start(running.broker.url)
    const records = await first.append.consume('events', {
      key: [where], timeout: 1500,
    })
    await producer.append.publish('events', { key: [where, 'thing'], value: 'before' })
    // **Drained rather than broken out of**, because leaving the loop
    // early leaves the record in hand unacknowledged: a loop that runs to
    // its timeout has asked past the last record, which is what
    // acknowledges it and moves the stored position.
    const seen = []
    for await (const record of records) seen.push(record.payload.toString())
    assert.deepEqual(seen, ['before'])
    await first.close()

    // Published while the client is away, and delivered the moment it
    // reconnects: before `consume` has been called on the new connection.
    await producer.append.publish('events', { key: [where, 'thing'], value: 'while away' })

    const second = new Client(name, { durable: true })
    await second.start(running.broker.url)
    await new Promise((resolve) => setTimeout(resolve, 500))
    const again = await second.append.consume('events', {
      key: [where], timeout: 8000,
    })
    const [swept] = await take(again, 1)
    assert.equal(swept.payload.toString(), 'while away',
      'the record delivered before consume was called went nowhere')
    await second.close()
  })

test('a subscription the broker refuses is raised rather than waited out',
  async () => {
    // MQTT answers each filter separately, and a client that does not
    // read the codes sits connected, subscribed to nothing, and receives
    // nothing for ever. An ordinary subscription may not cross a queue's
    // filter at all, which is a refusal this broker gives.
    const reading = await clients.producer({ clientId: 'refused-' + site() })
    const refused = await rejected(
      reading._subscribeAndCheck(['work/+/jobs/+'], 1, 10_000),
    )
    assert.ok(refused instanceof SubscriptionRefused, refused.message)
    assert.ok(refused.reasonCodes[0] >= 0x80, String(refused.reasonCodes))
    assert.ok(refused.reasonString, 'the broker explained itself and it was dropped')
  })

test('a durable reader survives the link dropping', async () => {
  // **The state transition nothing else drives.** Every other test
  // connects, works and disconnects cleanly, so what a reader does when
  // the socket dies under it, and what MQTT.js's automatic reconnect does
  // to the per-reader routing, would otherwise be unproven.
  //
  // The link is broken with saguin's own verb rather than by reaching
  // into MQTT.js: another client hangs this one up, which is the real
  // case an operator causes.
  const where = site()
  const name = 'survivor-' + where
  const producer = await clients.producer()
  const reading = await clients.producer({ clientId: name, durable: true })

  const records = await reading.append.consume('events', {
    key: [where], timeout: 20_000,
  })
  await producer.append.publish('events', { key: [where, 'before'], value: 'before' })
  const first = await records.next()
  assert.equal(first.value.payload.toString(), 'before')

  // Down it goes.
  assert.equal(await producer.admin.disconnect(name), 'hung-up')
  await waitUntil(() => !reading.connected, 'the client never noticed the link go')
  // MQTT.js brings it back; the session is durable, so the broker
  // restores the subscription.
  await waitUntil(() => reading.connected, 'MQTT.js did not reconnect within 10s')
  assert.equal(reading.sessionPresent, true, 'the session was not resumed')

  await producer.append.publish('events', { key: [where, 'after'], value: 'after' })

  // **`before` comes back first, and that is the promise rather than a
  // surprise.** A record is acknowledged when the loop asks for the next
  // one, so the one taken above was still unacknowledged when the link
  // went, and at-least-once means it is sent again. Expecting `after`
  // here would be expecting the guarantee to be broken.
  const got = []
  for await (const record of records) {
    got.push(record.payload.toString())
    if (got.includes('after')) break
  }
  assert.deepEqual(got, ['before', 'after'],
    'the reader went silent after the link came back, or lost the record it ' +
      'had not acknowledged')
})

test('a client that keeps no place loses its subscription with the link',
  async () => {
    // **And the case that does not survive, written down rather than
    // met.** A client with no durable session has nothing for the broker
    // to restore, and this library does not subscribe again by itself, so
    // a reader on one goes quiet when the link drops and nothing says so.
    //
    // That is MQTT's own behaviour rather than this library's, and the
    // remedy is `{ durable: true }`. It is a test so that it cannot
    // change without somebody noticing.
    const where = site()
    const name = 'fragile-' + where
    const producer = await clients.producer()
    const reading = await clients.producer({ clientId: name })

    const records = await reading.append.consume('events', {
      key: [where], timeout: 4000,
    })
    await producer.append.publish('events', { key: [where, 'before'], value: 'before' })
    const first = await records.next()
    assert.equal(first.value.payload.toString(), 'before')

    assert.equal(await producer.admin.disconnect(name), 'hung-up')
    await waitUntil(() => !reading.connected, 'the client never noticed the link go')
    await waitUntil(() => reading.connected, 'MQTT.js did not reconnect within 10s')
    assert.equal(reading.sessionPresent, false, 'a client with no place kept one')

    await producer.append.publish('events', { key: [where, 'after'], value: 'after' })
    const got = []
    for await (const record of records) got.push(record.payload.toString())
    assert.deepEqual(got, [],
      'the subscription came back on a client that keeps no place')
  })

test('a partly refused subscribe leaves nothing subscribed', async () => {
  // **The granted half of a partly refused SUBSCRIBE is undone.** MQTT
  // answers each filter separately, so a SUBSCRIBE naming two filters can
  // be half granted, and the caller gets an error rather than a reader.
  // Left alone, the granted filter goes on delivering to a client with
  // nobody reading it: the records pile into the unread buffer, and on a
  // queue they would be leases handed to a worker that does not exist.
  //
  // Driven at the guard the verbs use, because that is where a filter
  // list is sent. A queue's filter is refused to an ordinary subscriber,
  // which is the refusal this broker gives without an ACL file.
  const where = site()
  const producer = await clients.producer()
  const reading = await clients.producer({ clientId: 'partly-' + where })
  const seen = []
  reading.on('message', (record) => seen.push(record.payload.toString()))

  const events = `iot/${where}/events/+`
  const refused = await rejected(
    reading._subscribeAndCheck(['work/+/jobs/+', events], 1, 10_000),
  )
  assert.ok(refused instanceof SubscriptionRefused, refused.message)

  // Nothing arrives on the filter the broker granted.
  await producer.append.publish('events', { key: [where, 'one'], value: '1' })
  await new Promise((resolve) => setTimeout(resolve, 1500))
  assert.deepEqual(seen, [], 'the granted half of the subscribe was left in place')

  // And the record was there all along: this client had been unsubscribed
  // from it rather than the broker having gone quiet.
  await reading.subscribe(events, { qos: 1 })
  await producer.append.publish('events', { key: [where, 'two'], value: '2' })
  await waitUntil(() => seen.length > 0, 'nothing arrived once it subscribed again')
  assert.ok(['1', '2'].includes(seen[0]), seen.join())
})

test('reading a channel twice on one client takes it over', async () => {
  // Two readers of one filter cannot both hold it, and read, seek, read
  // again is an ordinary thing to do, so the second wins and the first is
  // closed. What must not happen is two readers interleaving on one
  // filter, since the one that asks first would acknowledge a record
  // belonging to the other.
  const where = site()
  const producer = await clients.producer()
  const reading = await clients.producer({ clientId: 'takeover-' + where, durable: true })

  const first = await reading.append.consume('events', { key: [where], timeout: 3000 })
  const second = await reading.append.consume('events', { key: [where], timeout: 3000 })
  assert.equal(first.closed, true, 'the first reader was left open')

  await producer.append.publish('events', { key: [where, 'one'], value: 'one' })
  const got = []
  for await (const record of second) got.push(record.payload.toString())
  assert.deepEqual(got, ['one'], 'the second reader did not get the records')

  const nothing = []
  for await (const record of first) nothing.push(record.payload.toString())
  assert.deepEqual(nothing, [], 'the closed reader kept reading')
})

test('start applies once and not on every restart', async () => {
  // The value is written once in the code, so a `start` that seeked every
  // time would replay the whole channel on every restart. It applies only
  // where the broker has no session for this client id, which is what
  // tells the library there is no position.
  //
  // Each read is drained to its timeout rather than broken out of,
  // because the loop acknowledges a record when it asks for the next one:
  // leaving early leaves the last record unacknowledged, and it rightly
  // comes back.
  const where = site()
  const name = 'once-' + where
  const producer = await clients.producer()
  for (const n of ['1', '2']) {
    await producer.append.publish('events', { key: [where, 'thing'], value: n })
  }

  const first = new Client(name, { durable: true })
  await first.start(running.broker.url)
  assert.equal(first.sessionPresent, false)
  const got = []
  for await (const record of await first.append.consume('events', {
    key: [where], start: 0, timeout: 4000,
  })) got.push(record.payload.toString())
  assert.deepEqual(got, ['1', '2'])
  await first.close()

  await producer.append.publish('events', { key: [where, 'thing'], value: '3' })

  const again = new Client(name, { durable: true })
  await again.start(running.broker.url)
  assert.equal(again.sessionPresent, true)
  const left = []
  for await (const record of await again.append.consume('events', {
    key: [where], start: 0, timeout: 4000,
  })) left.push(record.payload.toString())
  assert.deepEqual(left, ['3'], 'start fired a second time and replayed the channel')
  await again.close()
})
