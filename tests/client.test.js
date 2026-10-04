/**
 * Connecting, and asking the broker what a channel is.
 *
 * What saguin promises, and what these measure against. A channel is a
 * name and a topic filter, and the filter lives in the operator's
 * configuration, so a client that wants to work in channels asks the
 * broker what one is, at `$saguin/catalogue/<channel>`, with a Response
 * Topic. The answer is written to the asking connection, so no
 * subscription is needed and no other client receives it. The filter
 * comes back **as written**. A channel this client may not use, and one
 * that does not exist, are the same empty answer.
 *
 * A session the broker keeps is a session expiry it granted, and the
 * broker's own limit wins over what a client asked for: the CONNACK says
 * what was granted, and a client that reported its own request would be
 * telling an operator a position is kept for a day when it is kept for
 * five minutes.
 */

import assert from 'node:assert/strict'
import { test } from 'node:test'

import { Client, ConnectRefused, UnknownChannel } from '../src/index.js'
import { caught, rejected, useBroker, useClients } from './fixtures.js'

const running = useBroker()
const clients = useClients(running)

test('the broker admits an SDK client', async () => {
  const producer = await clients.producer()
  assert.equal(producer.connectReasonCode, 0)
  assert.equal(producer.connected, true)
  running.broker.checkItStayedUp()
})

test('the broker says what a channel is', async () => {
  const producer = await clients.producer()

  const got = await producer.channel('readings')
  assert.equal(got.name, 'readings')
  assert.equal(got.type, 'append')
  assert.equal(got.filter, 'iot/+/{device,sensor}/#')
  assert.ok(got.verbs.includes('write') && got.verbs.includes('read'), got.verbs.join())
  assert.equal(got.pin, null)

  // A queue answers its pin, which is the one form it admits.
  const queue = await producer.channel('tasks')
  assert.equal(queue.type, 'queue')
  assert.equal(queue.pin, '$saguin/queue/tasks')
})

test('an answer is asked for once and remembered', async () => {
  const producer = await clients.producer()

  const first = await producer.channel('readings')
  assert.equal(await producer.channel('readings'), first, 'a second call asked again')

  // Two callers at once are one question rather than two.
  const [a, b] = await Promise.all([
    producer.channel('events'), producer.channel('events'),
  ])
  assert.equal(a, b, 'two callers at once asked twice')

  producer.forgetChannel('readings')
  assert.notEqual(await producer.channel('readings'), first)
})

test('a channel the broker knows nothing about says both things it could mean',
  async () => {
    const producer = await clients.producer()

    const unknown = await rejected(producer.channel('no-such-channel'))
    assert.ok(unknown instanceof UnknownChannel, unknown.message)
    assert.match(unknown.message, /no such channel/)
    assert.match(unknown.message, /roles grant nothing/)

    // And nothing is remembered about it, so a grant arriving later is
    // picked up by asking again rather than by any invalidation rule. A
    // remembered refusal would hand back the very same error object.
    const again = await rejected(producer.channel('no-such-channel'))
    assert.notEqual(again, unknown, 'the refusal was remembered')
  })

test('a durable client is told what the broker granted, not what it asked for',
  async () => {
    // The broker's cap here is 5m, and this asks for a day.
    const producer = await clients.producer({ durable: true })
    assert.equal(producer.sessionExpiry, 24 * 60 * 60)
    assert.equal(producer.grantedSessionExpiry, 300)
    assert.equal(producer.sessionPresent, false, 'a first connection has no session')
  })

test('a client with no id of its own is refused', () => {
  const refused = caught(() => new Client(''))
  assert.match(refused.message, /needs an id of its own/)
})

test('a durable client with no session expiry is refused', () => {
  const refused = caught(() => new Client('someone', { durable: true, sessionExpiry: 0 }))
  assert.match(refused.message, /cannot be durable/)
})

test('a protocol that is not MQTT 5 is refused', async () => {
  const client = new Client('someone')
  const refused = await rejected(
    client.start(running.broker.url, { protocolVersion: 4 }),
  )
  assert.match(refused.message, /MQTT 5/)
})

test('a durable client is refused a clean start', async () => {
  const client = new Client('someone', { durable: true })
  const refused = await rejected(client.start(running.broker.url, { clean: true }))
  assert.match(refused.message, /clean start off/)
})

test('a door that wants a password refuses a client with none', async () => {
  const client = new Client('stranger')
  const refused = await rejected(client.start(running.broker.guardedUrl))
  assert.ok(refused instanceof ConnectRefused, refused.message)
  // 0x86 is Bad User Name or Password, which is what MQTT 5 says for a
  // client that offered neither.
  assert.equal(refused.reasonCode, 134)
  // Nothing was granted, so nothing is kept: the client that threw is not
  // left holding a connection quietly retrying.
  assert.equal(client.connected, false)
  assert.equal(client.mqtt, null)
})

test('a question needs a connection for the answer to arrive on', async () => {
  const client = new Client('gone')
  const refused = await rejected(client.channel('readings'))
  assert.match(refused.message, /not connected/)
})
