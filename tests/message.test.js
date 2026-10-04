/**
 * What the broker stamps on a delivery, and what the SDK makes of it.
 *
 * What saguin promises, and what these measure against. Every record has
 * an id, an offset the broker assigns and a receipt time. User Properties
 * come back in the order the publisher wrote them, a name that appears
 * more than once included. The `saguin-` prefix is reserved: a client's
 * is stripped, so that a publisher cannot forge metadata a consumer would
 * read as the broker's. `saguin-channel` is on a delivery only where the
 * consumer's filter reaches more than one channel, and a queue offer
 * never carries it because a worker names one queue. A `latest`
 * subscriber is sent the current value of every topic its filter reaches
 * with the RETAIN flag set, and every change after that without it.
 */

import assert from 'node:assert/strict'
import { test } from 'node:test'

import { Headers, Message } from '../src/index.js'
import { site, useBroker, useClients } from './fixtures.js'

const running = useBroker()
const clients = useClients(running)

/** A packet as MQTT.js parses one, for the cases that need no broker. */
function aPacket(properties) {
  return {
    cmd: 'publish', topic: 'iot/x/events/y', payload: Buffer.from('x'), qos: 1,
    properties,
  }
}

test('a delivery carries the record\'s position and receipt time', async () => {
  const where = site()
  const [producer, reader] = await Promise.all([clients.producer(), clients.reader()])
  await reader.subscribe(`iot/${where}/events/+`)

  await producer.publish(`iot/${where}/events/one`, '1')
  await producer.publish(`iot/${where}/events/two`, '2')

  const first = producer.record(await reader.next())
  const second = producer.record(await reader.next())

  assert.equal(typeof first.offset, 'number')
  assert.ok(second.offset > first.offset)
  assert.ok(first.timestamp instanceof Date)
  assert.ok(Math.abs(Date.now() - first.timestamp.getTime()) < 120_000)
  assert.match(first.id, /^[0-9a-f-]{36}$/)
})

test('the reserved prefix is stripped from a publisher\'s headers', async () => {
  // A publisher cannot forge dead-letter metadata a consumer would read
  // as the broker's.
  const where = site()
  const [producer, reader] = await Promise.all([clients.producer(), clients.reader()])
  await reader.subscribe(`iot/${where}/events/+`)

  await producer.publish(`iot/${where}/events/thing`, 'x', {
    properties: { userProperties: { 'saguin-dlq-channel': 'forged', device: 'a' } },
  })
  const record = producer.record(await reader.next())

  assert.equal(record.dlq, null, 'a publisher forged a dead-letter account')
  assert.deepEqual(record.headers.pairs, [['device', 'a']])
})

test('headers keep their order and their duplicates', async () => {
  const where = site()
  const [producer, reader] = await Promise.all([clients.producer(), clients.reader()])
  await reader.subscribe(`iot/${where}/events/+`)

  await producer.publish(`iot/${where}/events/thing`, 'x', {
    headers: [['tag', 'a'], ['unit', 'C'], ['tag', 'b']],
  })
  const { headers } = producer.record(await reader.next())

  assert.deepEqual(headers.all('tag'), ['a', 'b'])
  assert.equal(headers.get('tag'), 'a')
  assert.equal(headers.get('missing'), undefined)
  assert.deepEqual(Object.fromEntries(headers), { tag: 'b', unit: 'C' })
  assert.equal(headers.size, 3)
})

test('the channel name is there only where a filter reaches two', async () => {
  const where = site()
  const topic = `iot/${where}/events/thing`
  const [producer, one, two] = await Promise.all([
    clients.producer(), clients.reader(), clients.reader(),
  ])
  await one.subscribe(`iot/${where}/events/+`)   // events, and nothing else
  await two.subscribe(`iot/${where}/#`)          // events and state

  await producer.publish(topic, 'x')

  assert.equal(producer.record(await one.next()).channel, null)
  assert.equal(producer.record(await two.next()).channel, 'events')
})

test('a latest value is catch-up on subscribe and a change is not', async () => {
  const where = site()
  const topic = `iot/${where}/state/temperature`
  const [producer, reader] = await Promise.all([clients.producer(), clients.reader()])
  await producer.publish(topic, '18')
  await reader.subscribe(`iot/${where}/state/+`)

  const state = producer.record(await reader.next())
  assert.equal(state.payload.toString(), '18')
  assert.equal(state.isCatchUp, true)

  await producer.publish(topic, '19')
  const change = producer.record(await reader.next())
  assert.equal(change.payload.toString(), '19')
  assert.equal(change.isCatchUp, false)
  assert.ok(change.offset > state.offset)
})

test('a queue offer carries its attempt and no channel name', async () => {
  const where = site()
  const [producer, worker] = await Promise.all([clients.producer(), clients.reader()])
  await worker.subscribe('$saguin/queue/tasks', { qos: 1 })
  await producer.queue.publish('tasks', { key: [where, '1'], value: 'do it' })

  const job = producer.record(await worker.next(15_000))
  assert.equal(job.attempt, 1)
  assert.equal(job.channel, null, 'a worker names one queue and was told anyway')
  assert.ok(job.properties.responseTopic, 'no Response Topic to answer a job on')
  assert.ok(job.properties.correlationData, 'no Correlation Data naming the delivery')
  await worker.answer(job.packet, 'ack')
})

// -- the record type, with no broker in it ----------------------------------

test('a delivery with no properties answers null rather than raising', () => {
  // What a 3.1.1 subscriber receives: the topic and the payload, and none
  // of the rest.
  const record = new Message(aPacket(undefined))
  assert.equal(record.id, null)
  assert.equal(record.offset, null)
  assert.equal(record.timestamp, null)
  assert.equal(record.channel, null)
  assert.equal(record.attempt, null)
  assert.equal(record.dlq, null)
  assert.equal(record.headers.size, 0)
})

test('one user property is read the same as several', () => {
  // MQTT.js carries one occurrence of a name as a string and several as
  // an array, so both spellings have to reach the decoder as one shape.
  // Asserted rather than assumed, because the decoder is written on the
  // strength of it.
  const one = new Message(aPacket({ userProperties: { device: 'a' } }))
  assert.deepEqual(one.headers.pairs, [['device', 'a']])

  const several = new Message(aPacket({ userProperties: { device: ['a', 'b'] } }))
  assert.deepEqual(several.headers.all('device'), ['a', 'b'])
})

test('headers is a sequence rather than a mapping', () => {
  const headers = new Headers([['tag', 'a'], ['tag', 'b']])
  assert.equal(headers.size, 2)
  assert.equal(headers.has('tag'), true)
  assert.equal(headers.has('other'), false)
  assert.deepEqual([...headers], [['tag', 'a'], ['tag', 'b']])
  assert.deepEqual(headers.names(), ['tag', 'tag'])
})

test('a timestamp is read as UTC milliseconds', () => {
  const record = new Message(aPacket({
    userProperties: { 'saguin-timestamp': '1756900000123' },
  }))
  assert.equal(record.timestamp.toISOString(), '2025-09-03T11:46:40.123Z')
})

test('a dead letter reads the broker\'s account', () => {
  const record = new Message(aPacket({
    userProperties: {
      'saguin-dlq-channel': 'tasks',
      'saguin-dlq-offset': '7',
      'saguin-dlq-attempts': '3',
      'saguin-dlq-reason': 'attempts_exhausted',
      'saguin-dlq-at': '2026-09-20T10:00:00Z',
      'saguin-dlq-first': '2026-09-20T09:59:00Z',
      'saguin-dlq-last': '2026-09-20T09:59:30Z',
    },
  }))
  assert.equal(record.dlq.channel, 'tasks')
  assert.equal(record.dlq.offset, 7)
  assert.equal(record.dlq.attempts, 3)
  assert.equal(record.dlq.reason, 'attempts_exhausted')
  assert.equal(record.dlq.at.toISOString(), '2026-09-20T10:00:00.000Z')
  assert.equal(record.dlq.first.toISOString(), '2026-09-20T09:59:00.000Z')
  assert.equal(record.dlq.last.toISOString(), '2026-09-20T09:59:30.000Z')
  // And none of it is among the publisher's own headers.
  assert.deepEqual(record.headers.pairs, [])
})

test('a time the broker never sent is null rather than an invalid Date', () => {
  // A delivery is not the place to discover that two libraries disagree
  // about a date format.
  const record = new Message(aPacket({
    userProperties: { 'saguin-dlq-channel': 'tasks', 'saguin-dlq-at': 'not a time' },
  }))
  assert.equal(record.dlq.at, null)
  assert.equal(record.dlq.reason, null)
})
