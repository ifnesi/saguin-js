/**
 * Who acknowledges a delivery, and when.
 *
 * What saguin promises, and what these measure against. A durable
 * consumer's stored position advances when the client acknowledges a
 * record, so an acknowledgement sent before the application has read it
 * is a position moved past data nobody saw. For a topic no channel
 * claims there is no stored position, and MQTT's own redelivery of an
 * unacknowledged QoS 1 message on a resumed session is the only replay
 * such a topic has - so acknowledging early costs that too.
 *
 * This library therefore holds every delivery:
 * `consume` acknowledges what it hands a reader when the loop asks
 * for the next record, and anything on the `message` event is answered by
 * the application with `record.ack()`. `{ manualAck: false }` hands the
 * job back to MQTT.js, which answers on arrival.
 *
 * Every assertion here reads the PUBACK off MQTT.js's own `packetsend`,
 * which is what went on the socket rather than what this library believes
 * it did.
 */

import assert from 'node:assert/strict'
import { test } from 'node:test'

import { Client } from '../src/index.js'
import { caught, site, useBroker, useClients } from './fixtures.js'

const running = useBroker()
const clients = useClients(running)

/** Every PUBACK this client puts on the wire. */
function pubacks(client) {
  const sent = []
  client.mqtt.on('packetsend', (packet) => {
    if (packet.cmd === 'puback') sent.push(packet.messageId)
  })
  return sent
}

const settle = (ms = 700) => new Promise((resolve) => setTimeout(resolve, ms))

test('a broadcast delivery is not acknowledged until the application says so',
  async () => {
    const where = site()
    const reader = await clients.producer({ clientId: 'hold-' + where, durable: true })
    const sent = pubacks(reader)
    const arrived = []
    reader.on('message', (record) => arrived.push(record))
    await reader.subscribe(`broadcast/${where}/#`, { qos: 1 })

    const producer = await clients.producer()
    await producer.publish(`broadcast/${where}/one`, 'hello')
    await settle()

    assert.equal(arrived.length, 1, 'the record did not reach the listener')
    assert.deepEqual(sent, [], 'the record was acknowledged before it was read')

    await arrived[0].ack()
    await settle(300)
    assert.equal(sent.length, 1, 'record.ack() sent no PUBACK')
  })

test('a delivery nothing read is not acknowledged at all', async () => {
  // The case with no listener: this library must not answer for a record
  // no application code has seen. On a durable session the broker sends
  // it again, which is the whole of what an unacknowledged QoS 1
  // delivery buys.
  const where = site()
  const name = 'unread-' + where
  const reader = new Client(name, { durable: true })
  await reader.start(running.broker.url)
  const sent = pubacks(reader)
  await reader.subscribe(`broadcast/${where}/#`, { qos: 1 })

  const producer = await clients.producer()
  for (const n of ['1', '2', '3']) await producer.publish(`broadcast/${where}/${n}`, n)
  await settle()
  assert.deepEqual(sent, [], 'records nobody read were acknowledged')
  await reader.mqtt.endAsync(true)

  const again = new Client(name, { durable: true })
  const back = []
  again.on('message', (record) => back.push(record.payload.toString()))
  await again.start(running.broker.url)
  await settle(1200)
  assert.deepEqual(back.sort(), ['1', '2', '3'],
    'the broker did not send back what this client never acknowledged')
  await again.close()
})

test('manualAck false hands the acknowledgement back to MQTT.js', async () => {
  // Every delivery answered on arrival, for a client that keeps no
  // position.
  const where = site()
  const reader = await clients.producer({ clientId: 'auto-' + where, manualAck: false })
  const sent = pubacks(reader)
  const arrived = []
  reader.on('message', (record) => arrived.push(record.payload.toString()))
  await reader.subscribe(`broadcast/${where}/#`, { qos: 1 })

  const producer = await clients.producer()
  await producer.publish(`broadcast/${where}/one`, 'hello')
  await settle()

  assert.deepEqual(arrived, ['hello'])
  assert.equal(sent.length, 1, 'nothing acknowledged the delivery')
})

test('an option this library does not know is refused', async () => {
  // A caller who writes `manual_ack`, or misspells an option, would
  // otherwise get a client that behaves differently from the one they
  // asked for, with nothing saying so.
  const refused = caught(() => new Client('typo-' + site(), { manual_ack: false }))
  assert.match(refused.message, /takes no option 'manual_ack'/)
  assert.match(refused.message, /manualAck/)

  const alsoRefused = caught(() => new Client('typo2-' + site(), { durrable: true }))
  assert.match(alsoRefused.message, /takes no option 'durrable'/)
})

test('a message listener that throws does not take the client down', async () => {
  // The listener runs inside MQTT.js's packet pump, so a throw escapes as
  // an uncaught exception and ends the process by default. The record is
  // unacknowledged either way, so it survives for the next connection.
  const where = site()
  const reader = await clients.producer({ clientId: 'throws-' + where, durable: true })
  const said = []
  reader.on('warning', (one) => said.push(one))
  const seen = []
  reader.on('message', (record) => {
    seen.push(record.payload.toString())
    throw new Error('the handler threw')
  })
  await reader.subscribe(`broadcast/${where}/#`, { qos: 1 })

  const producer = await clients.producer()
  await producer.publish(`broadcast/${where}/one`, 'one')
  await settle()
  await producer.publish(`broadcast/${where}/two`, 'two')
  await settle()

  assert.deepEqual(seen, ['one', 'two'],
    'the client stopped reading after a listener threw')
  assert.equal(reader.connected, true)
  assert.ok(said.some((one) => one.includes('threw')),
    'a listener that threw was swallowed in silence')
  assert.ok(said.some((one) => one.includes('unacknowledged')),
    'the warning does not say what happened to the record')
})

test('a record a reader handed out is acknowledged by the loop, not on arrival',
  async () => {
    // The rule the stored position rests on, read at the wire rather than
    // through its consequences: nothing is acknowledged while the
    // application is holding the record.
    const where = site()
    const producer = await clients.producer()
    const reading = await clients.producer({ clientId: 'loop-' + where, durable: true })
    const sent = pubacks(reading)

    const records = await reading.append.consume('events', {
      key: [where], timeout: 4000,
    })
    await producer.append.publish('events', { key: [where, 'one'], value: '1' })
    const first = await records.next()
    assert.equal(first.value.payload.toString(), '1')
    await settle(400)
    assert.deepEqual(sent, [], 'the record was acknowledged before the loop moved on')

    await producer.append.publish('events', { key: [where, 'two'], value: '2' })
    const second = await records.next()
    assert.equal(second.value.payload.toString(), '2')
    await settle(400)
    assert.equal(sent.length, 1, 'asking for the next record acknowledged nothing')
    await records.close()
  })
