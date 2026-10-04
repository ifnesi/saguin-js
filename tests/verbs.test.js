/**
 * Addressing a channel by name: the verbs that write, and the two that ask.
 *
 * What saguin promises, and what these measure against. A channel is a
 * name and a topic filter, and a client that works in channels asks the
 * broker what one is and builds the topic from the filter it answered. A
 * `latest` channel is a key-value store: a value set at a topic, read back
 * one at a time at `$saguin/kv/get` with a Response Topic, and deleted by
 * writing nothing. A key never set and a deleted one are the same empty
 * answer. `$saguin/sessions/disconnect` hangs up a client by id and says
 * whether there was one.
 *
 * The three writes are one publish underneath, so the only thing between
 * appending to a log and queueing work by mistake is that the library
 * asked what the channel was first.
 */

import assert from 'node:assert/strict'
import { test } from 'node:test'

import { KeyDoesNotFit, WrongChannelType } from '../src/index.js'
import {
  headersOf, notReserved, rejected, sentPublishes, site, useBroker, useClients,
} from './fixtures.js'

const running = useBroker()
const clients = useClients(running)

test('appending by channel name composes the topic', async () => {
  const where = site()
  const [producer, reader] = await Promise.all([clients.producer(), clients.reader()])
  await reader.subscribe(`iot/${where}/device/#`)

  const info = await producer.append.publish('readings', {
    key: [where, 'device', 'temp/1'],
    value: '21.5',
    headers: { unit: 'C' },
  })

  const packet = await reader.next()
  assert.equal(packet.topic, `iot/${where}/device/temp/1`)
  assert.equal(packet.payload.toString(), '21.5')
  assert.deepEqual(headersOf(packet), [['unit', 'C']])
  const stamped = Object.fromEntries(
    Object.entries(packet.properties.userProperties),
  )
  assert.equal(stamped['saguin-id'], info.saguinId)
  // It landed in the channel rather than in broadcast, which is what the
  // broker's own offset says.
  assert.match(String(stamped['saguin-offset']), /^\d+$/)
})

test('a write by name refuses a key before anything leaves', async () => {
  const producer = await clients.producer()
  const outgoing = sentPublishes(producer, notReserved)

  const refused = await rejected(
    producer.append.publish('readings', { key: [site(), 'gadget', 'b'], value: 'x' }),
  )
  assert.ok(refused instanceof KeyDoesNotFit, refused.message)
  assert.match(refused.message, /iot\/\+\/\{device,sensor\}\/#/)
  assert.equal(outgoing.length, 0, 'a record went out under a key that does not fit')
})

test('a verb for the wrong kind of channel says which to use', async () => {
  const where = site()
  const producer = await clients.producer()

  const wrong = await rejected(
    producer.append.publish('tasks', { key: [where, '1'], value: 'x' }),
  )
  assert.ok(wrong instanceof WrongChannelType, wrong.message)
  assert.match(wrong.message, /queue channel/)
  assert.match(wrong.message, /client\.queue\.publish\(\)/)

  assert.ok(await rejected(
    producer.latest.set('readings', { key: [where, 'device', 'x'], value: 'x' }),
  ) instanceof WrongChannelType)
  assert.ok(await rejected(
    producer.queue.publish('readings', { key: [where, 'device', 'x'], value: 'x' }),
  ) instanceof WrongChannelType)
  assert.ok(await rejected(
    producer.latest.get('events', { key: [where, 'thing'] }),
  ) instanceof WrongChannelType)
})

test('a latest channel is a key-value store', async () => {
  // set, get and delete, and an absent key is null: the same answer a
  // deleted one gives, as everywhere else on this channel type.
  const where = site()
  const producer = await clients.producer()
  const key = [where, 'temp']

  assert.equal(await producer.latest.get('state', { key }), null)

  await producer.latest.set('state', { key, value: '18' })
  assert.equal((await producer.latest.get('state', { key })).toString(), '18')

  await producer.latest.set('state', { key, value: '19' })
  assert.equal((await producer.latest.get('state', { key })).toString(), '19')

  await producer.latest.delete('state', { key })
  assert.equal(await producer.latest.get('state', { key }), null)
})

test('a point read does not make the reader a subscriber', async () => {
  // Reading a value once does not enrol you in every later change to it.
  // The answer is written to the asking connection, so nothing else can
  // arrive on it either.
  const where = site()
  const [reading, writing] = await Promise.all([clients.producer(), clients.producer()])
  const arrived = []
  reading.mqtt.on('message', (topic) => arrived.push(topic))

  await writing.latest.set('state', { key: [where, 'temp'], value: '18' })
  assert.equal((await reading.latest.get('state', { key: [where, 'temp'] })).toString(), '18')

  await writing.latest.set('state', { key: [where, 'temp'], value: '19' })
  // Long enough that a delivery would have arrived: the same connection
  // answered a point read in well under this.
  await new Promise((resolve) => setTimeout(resolve, 500))

  // What did arrive on this connection is the library's own answers,
  // which is the other half of the claim: the broker writes them to the
  // asking connection rather than publishing them, so no subscription is
  // involved and nobody else receives them.
  assert.ok(arrived.length >= 2, 'no answer arrived on the asking connection')
  assert.deepEqual(
    arrived.filter((topic) => !topic.startsWith('saguin/reply/')), [],
    'the point read subscribed the client to the topic',
  )
})

test('queueing work by name composes the topic', async () => {
  // A queue's records cannot be read back by an ordinary subscriber: an
  // ordinary subscription may not cross a queue's filter at all. So the
  // oracle here is MQTT.js's own report of what went on the socket, and
  // the broker's acceptance of it.
  const where = site()
  const producer = await clients.producer()
  const outgoing = sentPublishes(producer, `work/${where}/jobs/1`)

  const info = await producer.queue.publish('tasks', {
    key: [where, '1'], value: 'do it',
  })

  assert.equal(outgoing.length, 1, 'MQTT.js reported no publish to read')
  assert.equal(outgoing[0].payload.toString(), 'do it')
  assert.equal(outgoing[0].qos, 1)
  assert.ok(info.saguinId)
})

test('a client can ask the broker to hang up another', async () => {
  const name = 'hung-up-' + site()
  const [producer, reader] = await Promise.all([
    clients.producer(), clients.reader({ clientId: name }),
  ])
  const closed = new Promise((resolve) => reader.client.once('close', resolve))

  assert.equal(await producer.admin.disconnect(name), 'hung-up')
  await closed

  // A client that is not there is a different answer from one that was:
  // otherwise "that device went away an hour ago" and "you have misspelled
  // the id" read the same.
  assert.equal(await producer.admin.disconnect('nobody-' + site()), 'no-such-client')
})
