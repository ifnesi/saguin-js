/**
 * What goes on the wire when the SDK publishes.
 *
 * What saguin promises, and what these measure against: a producer may
 * supply a UUIDv7 as the User Property `saguin-id`, and the broker stores
 * it as the record's Message ID, unchanged by redelivery, dead-lettering
 * and replay, so that a consumer can always deduplicate on it. A
 * publisher that supplies none is given one the broker generates.
 *
 * Every assertion below is made against a message read back by plain
 * MQTT.js.
 */

import assert from 'node:assert/strict'
import { test } from 'node:test'

import { RequestRefused, newMessageId } from '../src/index.js'
import {
  headersOf, rawProperties, rejected, sentPublishes, site, useBroker, useClients,
} from './fixtures.js'

const running = useBroker()
const clients = useClients(running)

/** The version and variant of a UUID, read off the text rather than
 * through anything this library wrote. */
function uuidShape(text) {
  assert.match(text, /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/)
  const hex = text.replace(/-/g, '')
  return {
    version: parseInt(hex[12], 16),
    variant: parseInt(hex[16], 16) >> 2, // 0b10 for RFC 4122
    minted: parseInt(hex.slice(0, 12), 16), // 48 bits of Unix milliseconds
  }
}

function idsOf(packet) {
  return rawProperties(packet).filter(([name]) => name === 'saguin-id').map(([, v]) => v)
}

test('a publish carries a generated UUIDv7 message id', async () => {
  const where = site()
  const topic = `iot/${where}/events/thing`
  const [producer, reader] = await Promise.all([clients.producer(), clients.reader()])
  assert.deepEqual(await reader.subscribe(`iot/${where}/events/+`), [1])

  const info = await producer.publish(topic, 'hello')
  const shape = uuidShape(info.saguinId)
  assert.equal(shape.version, 7)
  assert.equal(shape.variant, 0b10)

  const packet = await reader.next()
  assert.deepEqual(idsOf(packet), [info.saguinId])
  assert.equal(packet.payload.toString(), 'hello')
  assert.equal(packet.topic, topic)
})

test('a supplied message id is the one stored', async () => {
  const where = site()
  const mine = newMessageId()
  const [producer, reader] = await Promise.all([clients.producer(), clients.reader()])
  await reader.subscribe(`iot/${where}/events/+`)

  const info = await producer.publish(`iot/${where}/events/thing`, 'x', { saguinId: mine })
  assert.equal(info.saguinId, mine)
  assert.deepEqual(idsOf(await reader.next()), [mine])
})

test('a message id written into the properties is kept', async () => {
  const where = site()
  const mine = newMessageId()
  const [producer, reader] = await Promise.all([clients.producer(), clients.reader()])
  await reader.subscribe(`iot/${where}/events/+`)

  const info = await producer.publish(`iot/${where}/events/thing`, 'x', {
    properties: { userProperties: { 'saguin-id': mine, device: 'a' } },
  })
  assert.equal(info.saguinId, mine)
  const packet = await reader.next()
  assert.deepEqual(idsOf(packet), [mine])
  assert.deepEqual(headersOf(packet), [['device', 'a']])
})

test('the saguinId option wins over one in the properties', async () => {
  const where = site()
  const topic = `iot/${where}/events/thing`
  const written = newMessageId()
  const asked = newMessageId()
  const [producer, reader] = await Promise.all([clients.producer(), clients.reader()])
  await reader.subscribe(`iot/${where}/events/+`)
  // One id on the wire, not two: MQTT 5 permits a User Property more than
  // once, so a library that appended would send both and leave the broker
  // to choose. Asserted on what MQTT.js sent, because the broker strips a
  // publisher's `saguin-` properties and stamps its own, so the delivery
  // reads the same either way.
  const outgoing = sentPublishes(producer, topic)

  const info = await producer.publish(topic, 'x', {
    properties: { userProperties: { 'saguin-id': written } },
    saguinId: asked,
  })
  assert.equal(info.saguinId, asked)
  assert.equal(outgoing.length, 1, 'MQTT.js reported no publish to read')
  assert.deepEqual(idsOf(outgoing[0]), [asked])
  assert.deepEqual(idsOf(await reader.next()), [asked])
})

test('the caller\'s properties are neither changed nor reused', async () => {
  // A publisher that builds one properties object and reuses it would
  // otherwise send the first record's id on every record after it, and a
  // consumer deduplicating on the id would drop the lot.
  const where = site()
  const properties = { userProperties: { device: 'a' } }
  const [producer, reader] = await Promise.all([clients.producer(), clients.reader()])
  await reader.subscribe(`iot/${where}/events/+`)

  const first = await producer.publish(`iot/${where}/events/one`, '1', { properties })
  const second = await producer.publish(`iot/${where}/events/two`, '2', { properties })

  assert.notEqual(first.saguinId, second.saguinId)
  assert.deepEqual(properties, { userProperties: { device: 'a' } })

  const seen = new Map()
  for (let i = 0; i < 2; i += 1) {
    const packet = await reader.next()
    seen.set(packet.topic, packet)
  }
  assert.deepEqual(idsOf(seen.get(`iot/${where}/events/one`)), [first.saguinId])
  assert.deepEqual(idsOf(seen.get(`iot/${where}/events/two`)), [second.saguinId])
  for (const packet of seen.values()) {
    assert.deepEqual(headersOf(packet), [['device', 'a']])
  }
})

test('headers are written as User Properties, however they were given', async () => {
  // An object is the natural spelling, and pairs are still taken,
  // because a name may be given twice and an object cannot hold it
  // twice.
  const where = site()
  const [producer, reader] = await Promise.all([clients.producer(), clients.reader()])
  await reader.subscribe(`iot/${where}/events/+`)

  await producer.publish(`iot/${where}/events/one`, '1', { headers: { unit: 'C' } })
  assert.deepEqual(headersOf(await reader.next()), [['unit', 'C']])

  await producer.publish(`iot/${where}/events/two`, '2', {
    headers: [['tag', 'a'], ['tag', 'b']],
  })
  assert.deepEqual(headersOf(await reader.next()), [['tag', 'a'], ['tag', 'b']])
})

test('a publish is QoS 1 unless the caller says otherwise', async () => {
  // Driven over broadcast, where saguin is an ordinary MQTT broker and a
  // delivery carries the lower of the publish and the subscription, so
  // what arrives is proof of what the client sent.
  const topic = `broadcast/${site()}/reading`
  const [producer, reader] = await Promise.all([clients.producer(), clients.reader()])
  await reader.subscribe(topic, { qos: 1 })

  await producer.publish(topic, 'default')
  assert.equal((await reader.next()).qos, 1)

  await producer.publish(topic, 'asked for none', { qos: 0 })
  assert.equal((await reader.next()).qos, 0)
})

test('a refused publish carries the broker\'s own sentence', async () => {
  // The refusal that matters is not the reason code: 0x90 spelled out is
  // "Topic Name invalid", and saguin sends a sentence beside it naming
  // the rule that was broken. A library that dropped it would turn an
  // explanation into a number.
  const producer = await clients.producer()
  const refused = await rejected(producer.publish('$saguin/nonsense', 'x'))
  assert.ok(refused instanceof RequestRefused, refused.message)
  assert.equal(refused.reasonCode, 144)
  assert.equal(refused.reasonString, 'this is not a topic saguin defines')
  assert.match(refused.message, /this is not a topic saguin defines/)
})

test('newMessageId is a UUIDv7 that sorts by when it was minted', () => {
  const minted = Array.from({ length: 50 }, () => newMessageId())
  for (const one of minted) {
    const shape = uuidShape(one)
    assert.equal(shape.version, 7)
    assert.equal(shape.variant, 0b10)
  }
  assert.equal(new Set(minted).size, minted.length)
  // The first 48 bits are Unix milliseconds, so ids minted in order do
  // not sort out of it. Ties inside one millisecond are permitted.
  const stamps = minted.map((one) => uuidShape(one).minted)
  assert.deepEqual(stamps, [...stamps].sort((a, b) => a - b))
})
