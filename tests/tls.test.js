/**
 * Meeting a broker behind TLS, and being asked for a certificate.
 *
 * What saguin promises, and what these measure against. TLS is per
 * listener rather than per broker: a listener names a certificate and a
 * key, and one that also names a `client_ca_file` asks every client for a
 * certificate of its own and refuses a client with none. A client
 * certificate's Common Name becomes the client's user name, so mutual TLS
 * is authentication and not only encryption.
 *
 * This library adds nothing to any of it: the options are MQTT.js's own,
 * passed through `start`. What the tests hold down is that claim, which
 * is the one a reader of the README acts on.
 */

import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { test } from 'node:test'

import { Client, ConnectRefused } from '../src/index.js'
import { rejected, site, useTlsBroker } from './fixtures.js'

const running = useTlsBroker()

const read = (name) => readFileSync(join(running.broker.workdir, name))

test('a client that trusts the broker connects over TLS', async () => {
  // And publishes, because a handshake that completes and a connection
  // that works are two different claims.
  const where = site()
  const client = new Client('tls-' + where)
  await client.start(`mqtts://127.0.0.1:${running.broker.port}`, { ca: read('ca.pem') })

  const sent = await client.append.publish('events', {
    key: [where, 'thing'], value: 'over tls',
  })
  assert.ok(sent.saguinId)
  await client.close()
})

test('a client that does not trust the broker is refused', async () => {
  // The control, and the half that matters: without it the test above
  // passes against a client that verifies nothing.
  const client = new Client('untrusting-' + site())
  const refused = await rejected(
    client.start(`mqtts://127.0.0.1:${running.broker.port}`, {
      // No `ca`, so the broker's own authority is unknown to it.
      ca: undefined,
    }),
  )
  assert.match(refused.message, /unable to verify the first certificate/i)
  // **Not a ConnectRefused**, which says the broker answered the CONNECT
  // with a failure reason code. Nothing here reached a CONNACK, and
  // calling it a refusal would send the reader to the broker's ACL to
  // look for a certificate problem.
  assert.equal(refused instanceof ConnectRefused, false, refused.message)
})

test('a client presenting a certificate is admitted', async () => {
  // The WebSocket listener names a `client_ca_file`, so it asks every
  // client for a certificate.
  const client = new Client('device-7')
  await client.start(`wss://127.0.0.1:${running.broker.wsPort}`, {
    ca: read('ca.pem'),
    cert: read('device-7.pem'),
    key: read('device-7-key.pem'),
  })
  assert.equal(client.connected, true)
  assert.equal(client.connectReasonCode, 0)
  await client.close()
})

test('a client presenting no certificate is refused where one is required',
  async () => {
    // Trusting the broker is not enough at this door: the checking goes
    // both ways.
    const client = new Client('no-certificate-' + site())
    const refused = await rejected(
      client.start(`wss://127.0.0.1:${running.broker.wsPort}`, { ca: read('ca.pem') }),
    )
    assert.match(refused.message, /certificate required/i)
    assert.equal(refused instanceof ConnectRefused, false, refused.message)
  })
