/**
 * A real broker, started for the suite, and a reader that is not the SDK.
 *
 * Two rules shape this file:
 *
 * **The suite drives a running broker rather than a mock.** What is being
 * tested here is what saguin does with what a client sent, and a mock
 * would answer with what this SDK believes saguin does, which is the
 * thing under test.
 *
 * **The oracle is what saguin promises, and the reader is plain MQTT.js.**
 * The promise each test measures against is written out at the test
 * rather than cited: saguin's specification lives in the broker's own
 * repository, and a reference a reader of this one cannot open is worse
 * than no reference. Every assertion about what the broker stamps on a
 * delivery is checked against a message read by `mqtt.connect()`, not by
 * anything in this package.
 *
 * **One broker per test file**: `node --test` runs each file in its own
 * process, so a session-wide broker would be a process of its own to
 * manage and a port to hand around. A broker starts in well under a
 * second, and each file gets the hooks below.
 */

import { after, before } from 'node:test'
import { spawnSync } from 'node:child_process'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { randomUUID } from 'node:crypto'
import mqtt from 'mqtt'

import { Broker, brokerBinary } from './broker.js'
import { Client } from '../src/index.js'

export const CONFIG = `\
# Written by saguin-js's test suite. Memory storage throughout: what these
# tests ask about is what the broker puts on a delivery, and none of them
# restarts it.
broker:
  id: saguin-js-tests
  log_level: warn
  mqtt:
    listen:
      tcp:
        address: 127.0.0.1:{port}
      # A second door, and it wants a password. The tcp one above admits
      # everybody, which is what every other test needs, and it left
      # ConnectRefused unreachable, so the one error path on the way in
      # had never been driven.
      ws:
        address: 127.0.0.1:{ws_port}
        password_file: {passwd}
  storage:
    default: mem
    default_retention_period: none
    default_retention_bytes: none
    providers:
      - mem:
          type: memory
          snapshot_dir: none
  retained:
    storage: mem
    retention_period: none
  limits:
    # Deliberately far below the broker's own default, so that a client
    # asking for more than the cap can be driven rather than described.
    max_session_expiry: 5m
channels:
  # A schema registry is a \`latest\` channel and a convention: the schema
  # text is the value, the topic is the identity, and a producer names
  # that topic in a \`schema\` user property.
  - schemas:
      type: latest
      filter: schemas/#
  - events:
      type: append
      filter: iot/+/events/+
  # A braced level, which is a level with a fixed set of spellings rather
  # than a wildcard: the shape the channel-level API composes against.
  - readings:
      type: append
      filter: iot/+/{device,sensor}/#
  - state:
      type: latest
      filter: iot/+/state/+
  # Named apart from the first level of its own filter on purpose: the
  # subscription pin is \`$saguin/queue/<name>\`, which names the queue and
  # not its topics, so a pin built from the filter instead of the name
  # would be wrong here and would go unnoticed if the two agreed.
  - tasks:
      type: queue
      filter: work/+/jobs/+
      # One attempt and a short lease, so that a returned job is
      # dead-lettered while a test is still watching rather than a minute
      # later.
      visibility_timeout: 2s
      retry:
        max_attempts: 1
  # \`tasks\` is configured for one attempt so that a handed-back job is
  # dead-lettered at once, which is what the dead-letter tests need. This
  # one retries, which is what a test about handing work back needs: the
  # two cannot be the same queue.
  - retried:
      type: queue
      filter: again/+/jobs/+
      visibility_timeout: 2s
      retry:
        max_attempts: 5
  # The same, with a gap that grows between attempts. \`linear\` is
  # base x attempt, so 1s, 2s, 3s: the shortest the broker will hold,
  # which it keeps in whole seconds and refuses anything under. Well clear
  # of the queue's own 200ms tick, which is what a gap has to be told
  # apart from. Only a job a worker *handed back* waits like this; one
  # taken back by the visibility timeout has already waited longer.
  - backoff:
      type: queue
      filter: slow/+/jobs/+
      visibility_timeout: 30s
      retry:
        max_attempts: 4
        backoff: linear
        backoff_base: 1s
  # A queue whose filter ends in \`#\`, so its dead letters land at
  # \`bulk/__dlq/...\` rather than at the end, which is the case that makes
  # putting work back a matter of asking the filter rather than stripping
  # the last level.
  - bulk:
      type: queue
      filter: bulk/#
      visibility_timeout: 2s
      retry:
        max_attempts: 1
`

/** Start a broker for this file, and stop it when the file is done.
 *
 * Returns a handle whose `.broker` is live from the first test onwards.
 * The teardown checks the broker stayed up before stopping it, and prints
 * what it said, because a suite that went red for a broker that died
 * halfway reports on a broker it was not driving.
 */
export function useBroker(config = CONFIG) {
  const handle = { broker: null }
  let workdir

  before(async () => {
    workdir = mkdtempSync(join(tmpdir(), 'saguin-js-tests-'))
    handle.broker = await new Broker(brokerBinary(), workdir, config).start()
  })

  after(async () => {
    if (!handle.broker) return
    try {
      handle.broker.checkItStayedUp()
    } finally {
      await handle.broker.stop()
      const said = handle.broker.said()
      if (said.trim()) process.stderr.write(said)
      rmSync(workdir, { recursive: true, force: true })
    }
  })

  return handle
}

/** One topic level of this test's own, so that a file-long broker with
 * memory storage never serves one test another's records. */
export function site() {
  return randomUUID().replace(/-/g, '').slice(0, 12)
}

/** A plain MQTT 5 subscriber that collects what arrives.
 *
 * It reads and never publishes. A helper that does both swallows the
 * packets arriving behind the one it was showing you, and then reports an
 * empty channel about a broker that delivered.
 */
export class Reader {
  #arrived = []
  #waiting = []

  constructor(url, { clientId, clean = true, sessionExpiry } = {}) {
    this.url = url
    this.clientId = clientId ?? 'reader-' + randomUUID().slice(0, 8)
    this.clean = clean
    this.sessionExpiry = sessionExpiry
    this.client = null
  }

  async open() {
    const properties = {}
    if (this.sessionExpiry !== undefined) {
      properties.sessionExpiryInterval = this.sessionExpiry
    }
    this.client = mqtt.connect(this.url, {
      clientId: this.clientId,
      protocolVersion: 5,
      clean: this.clean,
      properties,
    })
    this.client.on('message', (topic, payload, packet) => {
      const next = this.#waiting.shift()
      if (next) next(packet)
      else this.#arrived.push(packet)
    })
    await new Promise((resolve, reject) => {
      this.client.once('connect', resolve)
      this.client.once('error', reject)
    })
    return this
  }

  /** Subscribe and wait for the SUBACK, answering its reason codes, so
   * that a test never publishes into a subscription the broker has not
   * yet made. */
  async subscribe(topic, { qos = 1, ...options } = {}) {
    return new Promise((resolve, reject) => {
      this.client.subscribe(topic, { qos, ...options }, (err, granted, packet) => {
        // A refused filter reaches the callback as an error, and the
        // codes are on the SUBACK packet either way: a test about a
        // refusal needs the code rather than a thrown message.
        if (err) {
          const codes = packet?.granted ?? err.packet?.granted ?? []
          resolve(codes.length ? codes : [err.code])
          return
        }
        if (!granted) return reject(new Error('the broker answered no grant'))
        resolve(granted.map((one) => one.qos))
      })
    })
  }

  /** The next message to arrive, or a failure saying nothing did. */
  next(timeout = 10_000) {
    const held = this.#arrived.shift()
    if (held) return Promise.resolve(held)
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        const at = this.#waiting.indexOf(take)
        if (at >= 0) this.#waiting.splice(at, 1)
        reject(new Error(`nothing arrived for ${this.clientId} within ${timeout}ms`))
      }, timeout)
      const take = (packet) => {
        clearTimeout(timer)
        resolve(packet)
      }
      this.#waiting.push(take)
    })
  }

  /** Answer a queue offer: `ack` or `return`.
   *
   * On this client rather than another, which is the protocol rather than
   * a convenience: the broker takes an answer only from the session
   * holding the job, so a second connection publishing the same words is
   * ignored and the job comes back on its lease.
   */
  answer(job, outcome) {
    return new Promise((resolve, reject) => {
      this.client.publish(
        String(job.properties.responseTopic), outcome,
        { qos: 1, properties: { correlationData: job.properties.correlationData } },
        (err, packet) => (err ? reject(err) : resolve(packet)),
      )
    })
  }

  async close() {
    if (this.client) await this.client.endAsync(false)
  }
}

/** Readers and SDK clients that this file closes when it is done.
 *
 * A client left open holds a session the broker keeps, and on a durable
 * one that is a position: a suite that leaks them tests a broker whose
 * state it no longer knows.
 */
export function useClients(handle) {
  const open = []

  after(async () => {
    await Promise.all(open.splice(0).map((one) => one.close().catch(() => {})))
  })

  return {
    /** A plain MQTT.js reader, connected. */
    async reader(options = {}) {
      const one = new Reader(handle.broker.url, options)
      open.push(one)
      await one.open()
      return one
    },
    /** An SDK client, connected, with the CONNACK already answered.
     *
     * Waiting for it matters: a test that publishes into a connection the
     * broker has not yet accepted is a test whose timing decides whether
     * it passed. */
    async producer(options = {}, startOptions = {}) {
      const { clientId, ...rest } = options
      // Every client in the suite knows where schemas live, because a
      // client that does not is a different test.
      const one = new Client(clientId ?? 'producer-' + randomUUID().slice(0, 8),
        { schemaRegistry: 'schemas', ...rest })
      open.push(one)
      await one.start(handle.broker.url, startOptions)
      return one
    },
  }
}

/** The User Properties as MQTT.js handed them over: the oracle side, so
 * that a decoding defect in the SDK cannot hide one on the wire.
 *
 * MQTT.js carries one occurrence of a name as a string and several as an
 * array, so this answers pairs and a test can count them.
 */
export function rawProperties(packet) {
  const found = []
  for (const [name, value] of Object.entries(packet.properties?.userProperties ?? {})) {
    for (const one of Array.isArray(value) ? value : [value]) found.push([name, one])
  }
  return found
}

// -- a broker behind TLS ----------------------------------------------------

// **A second broker rather than a second listener on the first.** A broker
// takes one listener of each kind, and the session broker's two are already
// spoken for: the plain one every other test connects to, and the WebSocket
// one that wants a password. Putting TLS on either would change what those
// tests are about.
//
// Both listeners here carry the same certificate and differ in one thing: the
// WebSocket one names a `client_ca_file`, so it asks every client for a
// certificate and the plain-TLS one does not.
export const TLS_CONFIG = `\
broker:
  id: saguin-js-tls-tests
  log_level: warn
  mqtt:
    listen:
      tcp:
        address: 127.0.0.1:{port}
        tls:
          cert_file: CERTS/cert.pem
          key_file: CERTS/key.pem
      ws:
        address: 127.0.0.1:{ws_port}
        tls:
          cert_file: CERTS/cert.pem
          key_file: CERTS/key.pem
          client_ca_file: CERTS/ca.pem
  storage:
    default: mem
    default_retention_period: none
    default_retention_bytes: none
    providers:
      - mem:
          type: memory
          snapshot_dir: none
  retained:
    storage: mem
    retention_period: none
channels:
  - events:
      type: append
      filter: iot/+/events/+
`

/** One authority, a certificate for the broker and one for a device.
 *
 * **The README's own recipe, run rather than quoted.** It is the three
 * openssl commands per certificate that a reader of the broker's
 * documentation is told to type, so running them here is what says they
 * still work.
 *
 * The broker's subjectAltName is the address the tests dial. A Common Name
 * alone fails modern verification, which is the mistake this is most likely
 * to be copied into.
 */
export function makeCertificates(into) {
  const run = (...args) => {
    const done = spawnSync(args[0], args.slice(1), { encoding: 'utf8' })
    if (done.error || done.status !== 0) {
      throw new Error(
        `${args.join(' ')}\n${done.stdout ?? ''}${done.stderr ?? done.error?.message}`,
      )
    }
  }
  const at = (name) => join(into, name)
  run('openssl', 'req', '-x509', '-newkey', 'ec',
    '-pkeyopt', 'ec_paramgen_curve:P-256', '-days', '2', '-nodes',
    '-subj', '/CN=saguin-js-tests-ca',
    '-keyout', at('ca-key.pem'), '-out', at('ca.pem'))
  run('openssl', 'req', '-newkey', 'ec',
    '-pkeyopt', 'ec_paramgen_curve:P-256', '-nodes', '-subj', '/CN=broker',
    '-addext', 'subjectAltName=IP:127.0.0.1',
    '-keyout', at('key.pem'), '-out', at('broker.csr'))
  run('openssl', 'x509', '-req', '-in', at('broker.csr'),
    '-CA', at('ca.pem'), '-CAkey', at('ca-key.pem'),
    '-CAcreateserial', '-days', '2', '-copy_extensions', 'copy',
    '-out', at('cert.pem'))
  // The Common Name is the client's name: it becomes the user name and
  // matches ACL patterns, and the password file is not consulted for it.
  run('openssl', 'req', '-newkey', 'ec',
    '-pkeyopt', 'ec_paramgen_curve:P-256', '-nodes', '-subj', '/CN=device-7',
    '-keyout', at('device-7-key.pem'), '-out', at('device-7.csr'))
  run('openssl', 'x509', '-req', '-in', at('device-7.csr'),
    '-CA', at('ca.pem'), '-CAkey', at('ca-key.pem'),
    '-CAcreateserial', '-days', '2', '-out', at('device-7.pem'))
}

/** A broker behind TLS, and the certificates to meet it with.
 *
 * The files are in `handle.broker.workdir`: `ca.pem` is the authority that
 * signed both sides, `device-7.pem` and `device-7-key.pem` are a client's.
 */
export function useTlsBroker() {
  const handle = { broker: null }
  let workdir

  before(async () => {
    workdir = mkdtempSync(join(tmpdir(), 'saguin-js-tls-'))
    makeCertificates(workdir)
    handle.broker = await new Broker(
      brokerBinary(), workdir, TLS_CONFIG.replaceAll('CERTS', workdir),
    ).start()
  })

  after(async () => {
    if (!handle.broker) return
    try {
      handle.broker.checkItStayedUp()
    } finally {
      await handle.broker.stop()
      const said = handle.broker.said()
      if (said.trim()) process.stderr.write(said)
      rmSync(workdir, { recursive: true, force: true })
    }
  })

  return handle
}

/** Every PUBLISH this client hands MQTT.js, as MQTT.js reports it.
 *
 * **For the claims about what went out rather than what came back.** The
 * broker strips a `saguin-` User Property a publisher wrote and stamps
 * its own, so a delivery cannot show that this library sent one id rather
 * than two: what arrives is normalized either way. `packetsend` is
 * MQTT.js saying what it put on the socket, which is outside this
 * library and is the level the claim lives at.
 */
export function sentPublishes(client, topic) {
  const wanted = typeof topic === 'function'
    ? topic
    : (one) => topic === undefined || one === topic
  const out = []
  client.mqtt.on('packetsend', (packet) => {
    if (packet.cmd === 'publish' && wanted(packet.topic)) out.push(packet)
  })
  return out
}

/** A topic the library sends on its own account: a question to the
 * broker rather than a record. A count of what a verb put on the wire
 * leaves these out, or it counts the catalogue lookup the verb had to
 * make first. */
export const notReserved = (topic) => !topic.startsWith('$saguin/')

/** What the publisher sent, with the broker's own stamps left out.
 *
 * A record delivered out of a channel carries `saguin-offset`,
 * `saguin-timestamp` and the rest beside whatever the publisher wrote,
 * and the `saguin-` prefix is reserved for exactly that. A test about
 * headers that did not drop them would be asserting on the broker's work
 * as though the client had sent it.
 */
export function headersOf(packet) {
  return rawProperties(packet).filter(([name]) => !name.startsWith('saguin-'))
}

/** The error a call threw, for a test that goes on to read it.
 *
 * `assert.throws` answers nothing, and a test that wants the refusal's
 * own words would otherwise assert them inside a callback, where a
 * mistyped property name passes by never running.
 */
export function caught(fn) {
  try {
    fn()
  } catch (err) {
    return err
  }
  throw new Error('nothing was refused')
}

/** The same for a promise. */
export async function rejected(promise) {
  try {
    await promise
  } catch (err) {
    return err
  }
  throw new Error('nothing was refused')
}
