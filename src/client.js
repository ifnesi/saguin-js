/**
 * The client: an MQTT.js client with saguin's verbs beside MQTT.js's own.
 *
 * `saguin.Client` **holds** an MQTT.js client rather than extending one,
 * and hands it to you as `client.mqtt`. Holding is MQTT.js's shape
 * rather than a preference: a client is built by
 * `mqtt.connect(url, options)`, which returns an `MqttClient` it
 * constructed itself with a stream builder the package does not export.
 * Subclassing would mean copying that function's protocol handling into
 * this library, which is the one thing a thin layer must not do.
 *
 * What this adds sits beside MQTT.js, grouped by the kind of channel it
 * works on. Those name a *channel*, and the library works out the MQTT:
 * it asks the broker what the channel is, builds the topic from the
 * channel's filter and the key you gave, and puts a record id on every
 * write.
 */

import { EventEmitter } from 'node:events'
import { randomBytes, randomUUID } from 'node:crypto'
import mqtt from 'mqtt'

import {
  ChannelInfo, compose, declarations, inside, subscriptions,
} from './channels.js'
import * as schemas from './schemas.js'
import { Message } from './message.js'
import { Reader } from './reader.js'
import { Admin, Append, Latest, Queue } from './verbs.js'

/** saguin is MQTT 5 only through this library. 3.1.1 has no User
 * Properties, no Response Topic, no shared subscriptions and no session
 * expiry, which is every saguin feature: a 3.1.1 client could only
 * publish, and the broker goes on admitting 3.1.1 publishers without a
 * library to do it. */
export const PROTOCOL_VERSION = 5

/** Where a client asks what a channel is; the name goes on the end. */
const CATALOGUE_TOPIC = '$saguin/catalogue/'

/** Where it reads one value from a `latest` channel without subscribing. */
const KV_GET_TOPIC = '$saguin/kv/get'

/** Where it asks the broker to hang another client up. */
const DISCONNECT_TOPIC = '$saguin/sessions/disconnect'

/** Where a consumer moves its own position; the channel goes in the middle. */
const seekTopic = (channel) => `$saguin/consumer/${channel}/seek`

/** How many records that reached no reader are held before the oldest is
 * dropped. Bounded because a client that neither consumes nor listens for
 * `message` would otherwise grow a queue nobody empties; the drop is said
 * out loud, because a record lost in silence is the failure this library
 * is written against. */
const UNREAD_BOUND = 10000

/** **What is answered to MQTT.js for a record this library is holding.**
 *
 * MQTT.js sends the PUBACK from the same callback that drives its packet
 * pump, so holding that callback until the application has read the
 * record would stop the client reading anything at all, including the
 * answers to its own questions. Answering it with an error skips its
 * PUBACK and lets the pump go on, and the acknowledgement is then this
 * library's to send, which it does when the reading loop asks for the
 * next record.
 *
 * The alternative is acknowledging on arrival, and that is not an
 * alternative: the broker advances a durable consumer's stored position
 * on the acknowledgement, so a client that died half way through a record
 * would resume after it. */
const HELD = new Error('saguin: this record is acknowledged when it is read')

/** A day: long enough that an overnight outage, a reboot or a redeploy
 * resumes where it stopped, short enough that a client which is never
 * coming back stops costing the broker a stored position within a day.
 * The broker's own ceiling is the operator's and is longer. */
export const DEFAULT_SESSION_EXPIRY = 24 * 60 * 60

/** How long a question to the broker waits for its answer. */
const ASK_TIMEOUT = 10_000

/** How long closing a reader waits for the broker to confirm the
 * unsubscribe. Long enough for any answer that is coming, and short
 * enough that closing a reader on a link that has silently gone is not a
 * hang. */
const UNSUBSCRIBE_TIMEOUT = 5_000

/** Which verbs belong to which kind of channel, so that a refusal can say
 * what to use instead rather than only what was wrong. */
export const VERBS_OF = {
  append: 'client.append.publish(), .consume() or .seek()',
  latest: 'client.latest.set(), .get(), .delete() or .consume()',
  queue: 'client.queue.publish(), .fetch(), .work(), .ack(), .nack() or .redrive()',
}

function a(kind) {
  return 'aeiou'.includes(kind[0]) ? `an ${kind}` : `a ${kind}`
}

/** The broker answered nothing about a channel.
 *
 * **It says both things it could mean, because the broker does not say
 * which**: a channel that does not exist and one this client holds no
 * verb on are answered identically, so that asking cannot be used to
 * discover what a broker has.
 */
export class UnknownChannel extends Error {
  constructor(name) {
    super(
      `the broker knows no channel '${name}' that this client may use: either ` +
        'there is no such channel, or this client\'s roles grant nothing on it',
    )
    this.name = 'UnknownChannel'
    this.channel = name
  }
}

/** This verb is for a different kind of channel.
 *
 * It names what the channel actually is and which verbs it takes, because
 * the three writes are one publish underneath and this is the only thing
 * that tells them apart: the alternative is work quietly appended to a
 * log by somebody who meant to queue it.
 */
export class WrongChannelType extends TypeError {
  constructor(info, want) {
    super(
      `'${info.name}' is ${a(info.type)} channel, and this verb is for ` +
        `${a(want)} one - use ${VERBS_OF[info.type]}`,
    )
    this.name = 'WrongChannelType'
    this.channel = info.name
    this.type = info.type
  }
}

/** The broker refused a request in its PUBACK.
 *
 * Every question this library asks the broker rides a QoS 1 publish, and
 * what is wrong with one comes back as a reason code and the broker's own
 * sentence, never on the reply topic, which carries an answer and nothing
 * else so that an empty answer can mean one thing.
 */
export class RequestRefused extends Error {
  constructor(about, reasonCode, reasonString) {
    super(
      `the broker refused ${about}: ${reasonCode}` +
        (reasonString ? ` - ${reasonString}` : ''),
    )
    this.name = 'RequestRefused'
    this.reasonCode = reasonCode
    /** What the broker said, or undefined where it said nothing.
     *
     * **The sentence, not the code.** `0x83` spelled out is
     * "Implementation specific error", which tells the person reading it
     * nothing at all: saguin sends a sentence beside it naming the rule
     * that was broken, and this is where it arrives. */
    this.reasonString = reasonString
  }
}

/** At least one filter in a SUBSCRIBE came back refused.
 *
 * Raised rather than returned, because this is the failure that looks
 * exactly like success: MQTT answers each filter separately and a client
 * that does not read the codes sits connected, subscribed to nothing, and
 * receives nothing for ever.
 */
export class SubscriptionRefused extends Error {
  constructor(refused, reasonCodes, reasonString) {
    super(
      'the broker refused ' +
        refused.map(([filter, code]) => `'${filter}': ${code}`).join(', ') +
        (reasonString ? ` - ${reasonString}` : ''),
    )
    this.name = 'SubscriptionRefused'
    this.refused = refused
    this.reasonCodes = reasonCodes
    /** What the broker said, or undefined where it said nothing.
     *
     * **The sentence, not the code.** `0x83` spelled out is
     * "Implementation specific error", which tells the person reading it
     * nothing at all: saguin sends a sentence beside it naming the rule
     * that was broken, and this is where it arrives. */
    this.reasonString = reasonString
  }
}

/** The broker answered the CONNECT with a failure reason code. */
export class ConnectRefused extends Error {
  constructor(reasonCode, said) {
    super(`the broker refused the connection: ${reasonCode}${said ? ` - ${said}` : ''}`)
    this.name = 'ConnectRefused'
    this.reasonCode = reasonCode
  }
}

/** A UUIDv7: 48 bits of Unix milliseconds, then random, so that ids sort
 * by the order they were minted in.
 *
 * The broker accepts any string here and generates one itself when a
 * publisher sends none. What makes a client-supplied id worth having is
 * that it is the *same* id on a retry: the record identity a consumer
 * deduplicates on outlives the packet.
 */
export function newMessageId() {
  const raw = Buffer.alloc(16)
  raw.writeUIntBE(Date.now(), 0, 6)
  randomBytes(10).copy(raw, 6)
  raw[6] = (raw[6] & 0x0f) | 0x70 // version 7
  raw[8] = (raw[8] & 0x3f) | 0x80 // variant 10
  const hex = raw.toString('hex')
  return [
    hex.slice(0, 8),
    hex.slice(8, 12),
    hex.slice(12, 16),
    hex.slice(16, 20),
    hex.slice(20),
  ].join('-')
}

/** What a publish answers: the id that went on the wire, and the packet
 * the broker acknowledged it with.
 *
 * **Awaited rather than handed back to be waited on**: `publish` is a
 * promise that settles on
 * the PUBACK, because awaiting is how JavaScript says "and then", and
 * because a refusal arrives in that PUBACK. A publisher that wants the
 * id before the broker has answered supplies it: `{ saguinId }` is used
 * unchanged, which is also what makes a retry the same record.
 */
export class PublishResult {
  constructor(saguinId, packet) {
    this.saguinId = saguinId
    this.packet = packet
    Object.freeze(this)
  }
}

/** An MQTT 5 client that also speaks in saguin channels.
 *
 *     const client = new saguin.Client('gateway-1')
 *     await client.start('mqtt://broker.local:1883')
 *     await client.publish('iot/site42/device/temp/1', '21.5')
 *
 * **Reading needs a durable client**, which is one that keeps its place:
 * clean start off, a session expiry that is not zero, and the same client
 * id next time. The three together are what the broker stores a position
 * against, and `{ durable: true }` is what sends them.
 *
 * Two defaults differ from MQTT.js's, and both are saguin's answer rather
 * than a preference:
 *
 * * **The protocol is MQTT 5** and nothing else is accepted.
 * * **A publish is QoS 1** unless the caller asks for another, because
 *   every refusal saguin gives a publisher comes back as a PUBACK reason
 *   code, and at QoS 0 there is no PUBACK, so all of them arrive as
 *   silence.
 */
export class Client extends EventEmitter {
  #known = new Map()
  #schemas = new Map()
  #answers = new Map()
  #asked = 0
  #replyTopic
  /** The readers a `consume` or a `fetch` handed out, in the order they
   * were made. */
  #readers = new Set()
  /** **Everything that reaches no reader.** A resumed session is served
   * records for subscriptions made on a previous connection, before the
   * application has called `consume`, so before any route for them
   * exists. With nowhere to put those, a durable consumer loses exactly
   * what it reconnected for. They wait here, and `consume` sweeps out the
   * ones that are its own. */
  #unread = []
  /** **Which connection a record arrived on.** A packet identifier is the
   * broker's, per connection, and it starts again at 1 on the next one,
   * so an identifier from a dead connection acknowledges whatever the
   * live connection happens to be holding under that number. Counted
   * here, carried beside every buffered record, and checked before
   * anything is acknowledged. */
  #generation = 0

  /**
   * @param {string} clientId the id the broker stores this client's
   *   session, and any position in it, against.
   * @param {object} [options]
   * @param {boolean} [options.durable] keep a session, and a position in
   *   it, between connections.
   * @param {number} [options.sessionExpiry] seconds the broker keeps a
   *   durable client's session after it goes.
   * @param {string} [options.schemaRegistry] the channel schemas are
   *   registered in.
   * @param {boolean} [options.manualAck] acknowledge a delivery when
   *   whoever reads it says so, which is the default. False acknowledges
   *   every delivery on arrival.
   */
  constructor(clientId, options = {}) {
    super()
    // **An option this library does not know is refused rather than
    // ignored.** A caller who writes
    // `manual_ack` here, or misspells one of these, otherwise gets a
    // client that quietly behaves differently from the one they asked
    // for. MQTT.js's own options go to `start`, which is where they are
    // read.
    const known = ['durable', 'sessionExpiry', 'schemaRegistry', 'manualAck']
    for (const name of Object.keys(options)) {
      if (!known.includes(name)) {
        throw new Error(
          `saguin.Client takes no option '${name}' - it takes ` +
            `${known.join(', ')}, and MQTT.js's own options go to start()`,
        )
      }
    }
    const {
      durable = false, sessionExpiry = DEFAULT_SESSION_EXPIRY,
      schemaRegistry = null, manualAck = true,
    } = options
    if (!clientId) {
      throw new Error(
        'a saguin client needs an id of its own: the broker stores a durable ' +
          "client's position against it, so a generated one is a new client " +
          'every time and the position is never found again',
      )
    }
    if (durable && !sessionExpiry) {
      throw new Error(
        'a session expiry of zero ends the session with the connection and ' +
          'stores no position, so it cannot be durable',
      )
    }

    this.name = clientId
    this.durable = Boolean(durable)
    /** Whether a delivery waits for whoever reads it to acknowledge it.
     *
     * **True by default, for a reason:** a
     * channel record's acknowledgement is what moves the
     * stored position, so it must not go out before the application has
     * read the record. `consume` acknowledges what it hands out; what
     * arrives on the `message` event is acknowledged with
     * `record.ack()`.
     *
     * False hands acknowledgement back to MQTT.js, which sends it as
     * each delivery arrives - the right answer only where nothing this
     * client reads keeps a position. */
    this.manualAck = Boolean(manualAck)
    /** What this client asks for. What it was granted is
     * `grantedSessionExpiry`, and they differ when the broker's cap is
     * lower. */
    this.sessionExpiry = durable ? Number(sessionExpiry) : 0
    /** The channel schemas are registered in, named once here.
     *
     * Deserializing needs it, and not as a convenience: a consumer must
     * not follow a schema pointer out of its registry, since an ACL
     * governs who may *write* a topic and never who may *name* one. */
    this.schemaRegistry = schemaRegistry

    this.append = new Append(this)
    this.latest = new Latest(this)
    this.queue = new Queue(this)
    this.admin = new Admin(this)

    /** The MQTT.js client, once `start` has built it. Everything MQTT.js
     * does is here: a broadcast topic, one no channel claims, is ordinary
     * MQTT and is published to and subscribed to the ordinary way. */
    this.mqtt = null
    this.grantedSessionExpiry = null
    /** Whether the broker found this client's session. False on a first
     * connection, and also when a stored position has fallen below a
     * channel's retention floor, which is the broker saying it cannot
     * resume you without a silent gap. */
    this.sessionPresent = null
    this.connectReasonCode = null

    // A topic of this client's own, never published to and never
    // subscribed: the broker writes an answer to the asking connection
    // rather than publishing it, so this needs no subscription and nobody
    // else can receive it.
    this.#replyTopic = 'saguin/reply/' + randomUUID().replace(/-/g, '')
  }

  /** Connect, and wait for the broker's answer.
   *
   * One call rather than three, because everything this client needs to
   * know is in the CONNACK: whether its session was found, and how long
   * its position will be kept. MQTT.js's own options are passed through,
   * so TLS, credentials, WebSockets and reconnect behaviour are set the
   * way they always are.
   *
   * @param {string} url `mqtt://host:port`, or any URL MQTT.js takes.
   * @param {object} [options] MQTT.js client options.
   */
  async start(url, options = {}) {
    if (this.mqtt) {
      throw new Error(`client '${this.name}' is already started`)
    }
    if (options.protocolVersion !== undefined &&
        options.protocolVersion !== PROTOCOL_VERSION) {
      throw new Error(
        'saguin.Client speaks MQTT 5 and nothing else; for a 3.1.1 publisher ' +
          'use mqtt.connect() directly',
      )
    }
    if (this.durable && options.clean) {
      throw new Error(
        'a durable client connects with clean start off, or the broker ' +
          'discards the position it connected to resume from',
      )
    }

    const properties = { ...options.properties }
    if (this.durable) properties.sessionExpiryInterval = this.sessionExpiry
    const settings = {
      ...options,
      clientId: this.name,
      protocolVersion: PROTOCOL_VERSION,
      clean: this.durable ? false : options.clean ?? true,
      // **MQTT.js resubscribes after a reconnection on its own, so this
      // is off unless the caller asks for it.** A
      // durable client needs nothing: the broker restores its session's
      // subscriptions, and a second SUBSCRIBE on a `latest` channel would
      // be answered with the whole of current state again, as catch-up
      // the application has already seen. A client that keeps no place
      // goes quiet when the link drops, which is MQTT's own behaviour and
      // what `durable: true` is the answer to.
      resubscribe: options.resubscribe ?? false,
      properties,
    }

    const client = mqtt.connect(url, settings)
    this.mqtt = client
    // **Routing happens here rather than on the `message` event**, because
    // this is the only place that can decide when the record is
    // acknowledged. MQTT.js emits `message` for every delivery either
    // way, so `client.mqtt.on('message')` still sees everything, and this
    // client's own `message` event is what reached no reader.
    client.handleMessage = (packet, done) => this.#took(packet, done)
    // **Every connection, not only the first.** MQTT.js reconnects on its
    // own, and each reconnection numbers its packet identifiers from 1
    // again, so what may be acknowledged is counted here rather than in
    // `start`. What the broker said a channel is goes with it: a broker
    // whose channels changed dropped this connection on the way.
    client.on('connect', (connack) => {
      this.#generation += 1
      this.#known.clear()
      this.sessionPresent = Boolean(connack.sessionPresent)
    })

    try {
      const connack = await new Promise((resolve, reject) => {
        const connected = (packet) => {
          client.removeListener('error', failed)
          resolve(packet)
        }
        const failed = (err) => {
          client.removeListener('connect', connected)
          reject(err)
        }
        client.once('connect', connected)
        client.once('error', failed)
      })
      this.connectReasonCode = connack.reasonCode ?? 0
      this.sessionPresent = Boolean(connack.sessionPresent)
      const granted = connack.properties?.sessionExpiryInterval
      this.grantedSessionExpiry = granted === undefined ? this.sessionExpiry : granted
      return connack
    } catch (err) {
      // **Ended here rather than left reconnecting.** MQTT.js suppresses
      // its own reconnect after a refused CONNACK but keeps the client,
      // and a caller whose `start` threw holds an object that looks
      // usable. Nothing was granted, so there is nothing to keep.
      this.mqtt = null
      await client.endAsync(true).catch(() => {})
      // **A number is a CONNACK reason code; anything else never got that
      // far.** A TLS handshake that failed, a refused socket and a name
      // that does not resolve all arrive here with a string code, and
      // calling one of those "the broker refused the connection" sends
      // the reader to the broker's ACL to look for a certificate problem.
      if (typeof err?.code === 'number') throw new ConnectRefused(err.code, err.message)
      throw err
    }
  }

  /** Stop the client and disconnect. A durable client's session, and the
   * position in it, stay with the broker for the granted expiry. */
  async close() {
    if (!this.mqtt) return
    // **The readers first**, so that a queue job left in hand is said out
    // loud while there is still a connection to say it about.
    for (const reader of [...this.#readers]) await reader.close().catch(() => {})
    const client = this.mqtt
    this.mqtt = null
    await client.endAsync(false)
  }

  /** So that `await using client = ...` closes it, where the runtime has
   * explicit resource management. */
  async [Symbol.asyncDispose]() {
    await this.close()
  }

  get connected() {
    return Boolean(this.mqtt?.connected)
  }

  // -- what a channel is ----------------------------------------------

  /** What the broker says this channel is, asked once and remembered.
   *
   * The answer holds the channel's type and the topic filter it claims,
   * which is the one thing about a channel a client cannot work out for
   * itself. It cannot go stale while this connection lasts: channels are
   * fixed when the broker starts, so a broker whose channels changed
   * dropped this connection on the way.
   *
   * Throws `UnknownChannel` where the broker answers nothing, and
   * remembers no such answer, so a grant that arrives later is picked up
   * by simply asking again.
   */
  async channel(name, { timeout = ASK_TIMEOUT } = {}) {
    const known = this.#known.get(name)
    if (known) return known
    // **The promise is what is remembered, not the answer.** Two verbs
    // naming one channel at the same moment is ordinary in JavaScript,
    // and remembering only the settled answer would ask the broker twice
    // and hand back two ChannelInfo objects for one channel.
    const asking = this._ask(
      CATALOGUE_TOPIC + name, '', timeout, `what channel '${name}' is`,
    ).then((reply) => {
      if (!reply.payload || reply.payload.length === 0) throw new UnknownChannel(name)
      return ChannelInfo.fromJSON(JSON.parse(reply.payload.toString()))
    })
    this.#known.set(name, asking)
    try {
      const found = await asking
      this.#known.set(name, found)
      return found
    } catch (err) {
      this.#known.delete(name)
      throw err
    }
  }

  /** Drop what was remembered about one channel, so the next call asks
   * again.
   *
   * What a channel *is* cannot change under a live connection, but what
   * this client may do there can: an `acl_file` is re-read on SIGUSR1. A
   * refused publish or subscribe drops the entry through this.
   */
  forgetChannel(name) {
    this.#known.delete(name)
  }

  /** The schema registered at a topic, read once and remembered.
   *
   * **Refused where the topic falls outside the registry**, which is the
   * rule that makes it safe to follow a pointer somebody else put in a
   * message: a publisher may name any topic, including a `latest` channel
   * holding device state, and a consumer that followed one would read it
   * out.
   */
  async schemaText(topic, { timeout = ASK_TIMEOUT } = {}) {
    if (!this.schemaRegistry) {
      throw new schemas.SchemaError(
        'this client was not told which channel schemas live in, so it cannot ' +
          'follow a schema pointer; build it with { schemaRegistry: <channel> }',
      )
    }
    const registry = await this.channel(this.schemaRegistry, { timeout })
    // **Checked, because pointing this at the wrong kind of channel fails
    // later and confusingly.** A registry is a key-value store: a schema
    // is set at a topic, read back one at a time, and retired by deleting
    // it. An append channel would take the writes and answer no read.
    if (registry.type !== 'latest') {
      throw new schemas.SchemaError(
        `'${this.schemaRegistry}' is ${a(registry.type)} channel, and a schema ` +
          'registry is a latest one - it is a key-value store of schema texts',
      )
    }
    if (!inside(registry.filter, topic)) {
      throw new schemas.SchemaError(
        `'${topic}' is outside '${this.schemaRegistry}', whose filter is ` +
          `'${registry.filter}' - a schema pointer is followed only inside its ` +
          'own registry',
      )
    }
    // **Kept for the life of the connection, and a republished schema is
    // not noticed.** That is a limit rather than an oversight, and the
    // reason is Avro: schemaless deserializing cannot reliably tell that
    // it has the wrong schema, so there is no clever invalidation here
    // that would only work for protobuf.
    //
    // It costs nothing where the convention is followed, because the
    // topic is the identity: **a new version is a new topic**, and a new
    // topic is a cache miss. `forgetSchema` is for anybody who
    // republishes at the same one anyway.
    const known = this.#schemas.get(topic)
    if (known !== undefined) return known
    const raw = await this._readTopic(topic, timeout)
    if (!raw) throw new schemas.SchemaError(`no schema is registered at '${topic}'`)
    const text = raw.toString()
    if (this.#schemas.size > 200) this.#schemas.clear()
    this.#schemas.set(topic, text)
    return text
  }

  /** Drop a remembered schema, or all of them, so the next read asks the
   * broker again.
   *
   * Needed only where a schema is republished at a topic this client has
   * already read, which the convention says to avoid, since the topic is
   * the identity and a new version is a new topic.
   */
  forgetSchema(topic = undefined) {
    if (topic === undefined) this.#schemas.clear()
    else this.#schemas.delete(topic)
  }

  // -- MQTT's own publish, with a record id on it ----------------------

  /** Publish to a topic, exactly as MQTT.js does, plus a record id.
   *
   * This is MQTT's own verb and it takes a **topic**: a broadcast topic
   * is published to the ordinary way. To write to a saguin channel by
   * name, use the channel verbs, which build the topic from the channel's
   * filter.
   *
   * **The record id is the library's to handle.** Every publish carries a
   * `saguin-id`, a UUIDv7 the broker stores as the record's Message ID,
   * stable across redelivery, dead-lettering and replay, and what a
   * consumer deduplicates on. Supply one with `{ saguinId }` and it is
   * used unchanged, which is what makes a retry the same record rather
   * than a second one; supply none and one is minted. Either way the id
   * that went out is on the object this resolves to.
   *
   * The caller's `properties` is never modified: a publisher that builds
   * one and reuses it would otherwise send the first record's id on every
   * record after it.
   */
  async publish(topic, payload = '', options = {}) {
    const {
      qos = 1, retain = false, properties, saguinId, headers, schema, ...rest
    } = options
    let sending = payload
    let sent = properties
    let named = headers
    if (schema !== undefined && schema !== null) {
      // **The schema is fetched before anything is sent**, which is the
      // whole reason a schema must be registered first: a record written
      // against a schema nobody can fetch is a record nobody can read.
      // **The caller's Content Type is passed in rather than replaced.**
      // A publisher saying what it sent is better evidence than anything
      // inferred about it, and avro has four spellings in the wild: one
      // that named `avro/binary` and was sent `application/avro` would
      // have its own header rewritten under it.
      const [encoded, contentType] = await schemas.serialize(
        payload, schema, await this.schemaText(schema), properties?.contentType,
      )
      sending = encoded
      named = [...pairsOf(headers ?? []), [schemas.SCHEMA_PROPERTY, schema]]
      sent = { ...properties, contentType }
    }
    const [props, wentOut] = withMessageId(withHeaders(sent, named), saguinId)
    const packet = await this._send(topic, sending, {
      ...rest, qos, retain, properties: props,
    }, `a publish to '${topic}'`)
    return new PublishResult(wentOut, packet)
  }

  // -- reading -----------------------------------------------------------

  /** A raw MQTT.js packet, as a saguin record.
   *
   * **For broadcast**, which is the one path this library does not hand
   * you a record on: a topic no channel claims is ordinary MQTT, read
   * through `client.on('message')` or MQTT.js's own, and what arrives
   * there is a packet rather than one of these.
   */
  record(packet) {
    return new Message(packet, this)
  }

  /** Where a delivery goes, and when it is acknowledged.
   *
   * MQTT.js hands every incoming PUBLISH here with the callback that
   * sends its PUBACK. A record a reader is waiting for is held: the
   * callback is answered with `HELD`, which sends no PUBACK, and this
   * library sends one when the reading loop moves on.
   */
  #took(packet, done) {
    // The packet's topic is a Buffer here: MQTT.js decodes one for its own
    // `message` event and leaves the packet as it parsed it.
    const topic = String(packet.topic)
    // **An answer to a question this library asked is not a record.** The
    // broker writes it to the asking connection, so it arrives here like
    // everything else and would otherwise be handed to the application as
    // a delivery it never subscribed to.
    if (topic === this.#replyTopic) {
      const waiting = this.#answers.get(correlationOf(packet))
      if (waiting) waiting(packet)
      done()
      return
    }
    const held = { generation: this.#generation, packet }
    // **The same rule for every delivery**, which is the reference
    // library's: nothing is acknowledged except by whoever reads it.
    // `consume` acknowledges what it hands a reader when the loop asks
    // for the next record; anything else is acknowledged by the
    // application with `record.ack()`. Turning `manualAck` off hands the
    // job back to MQTT.js, which answers on arrival.
    const answer = () => (this.manualAck ? done(HELD) : done())

    for (const reader of this.#readers) {
      if (reader.wants(topic) && reader.push(held)) {
        answer()
        return
      }
    }

    // Nobody is reading it: broadcast, and a resumed session's records
    // before `consume` has been called for them. They are kept rather
    // than dropped, and the `consume` that arrives sweeps out its own.
    answer()
    if (this.#unread.length >= UNREAD_BOUND) {
      const dropped = this.#unread.shift()
      this.warn(
        `dropping '${String(dropped.packet.topic)}': ${UNREAD_BOUND} records ` +
          `reached no reader on '${this.name}' and nothing is reading them - ` +
          'read them with consume(), or acknowledge them with record.ack()',
      )
    }
    this.#unread.push(held)

    const record = new Message(packet, this, this.#generation)
    // **An application's handler must not take the client down with it.**
    // This runs inside MQTT.js's packet pump, so a listener that throws
    // escapes as an uncaught exception and ends the process by default.
    // The record is unacknowledged either way, so it survives for the
    // next connection.
    try {
      this.emit('message', record)
    } catch (failed) {
      this.warn(
        `a message listener on '${this.name}' threw, and the record it was ` +
          `given stays unacknowledged: ${failed?.stack ?? failed}`,
      )
    }
  }

  /** Acknowledge a delivery this client handed you.
   *
   * For what arrives on the `message` event: broadcast, and a resumed
   * session's records before `consume` was called for them. A record a
   * `consume` loop handed you is acknowledged by that loop when it asks
   * for the next one, and acknowledging it here as well is harmless but
   * says nothing new.
   *
   * The record carries what is needed - the packet and the connection
   * it arrived on - so the record is the argument.
   */
  ack(record) {
    if (!record?.packet) {
      throw new Error('saguin.Client.ack takes a record, as the message event ' +
        'hands you one')
    }
    this._acknowledge(record.packet, record.generation)
  }

  /** Acknowledge a record, unless the connection it arrived on has gone.
   *
   * `_sendPacket` is MQTT.js's own, and this is the one thing this
   * library reaches past the public surface for: there is no other way to
   * answer a PUBLISH later than the callback MQTT.js offers. The test
   * that a record left unread comes back is what holds it down.
   */
  _acknowledge(packet, generation) {
    if (packet.qos === 0 || generation !== this.#generation) return
    if (!this.mqtt?.connected) return
    this.mqtt._sendPacket({ cmd: 'puback', messageId: packet.messageId, reasonCode: 0 })
  }

  /** Subscribe, and answer a reader that yields what arrives on these
   * filters.
   *
   * **Each reader gets its own arrivals.** With one queue per client, two
   * readers on the same client take each other's records: whichever asks
   * for the next one first is handed whatever arrived, whatever it
   * subscribed to. That is not a tidiness problem; the reader then
   * acknowledges a record belonging to the other subscription, which is a
   * record acknowledged away from a consumer that never saw it.
   */
  async _consume(want, channel, { key = [], qos = 1, timeout, askTimeout = ASK_TIMEOUT,
    start = null, topicHash, saguinFilter } = {}) {
    const declared = declarations({ topicHash, saguinFilter })
    if (declared.length && want === 'queue') {
      // **Refused before anything is asked of the broker**, which refuses
      // it too: a queue already divides its work between its workers, so
      // a slice of one is a second answer to the question the channel
      // type exists to answer. Saying so here names the call that did it
      // rather than handing back a reason code from a SUBACK.
      throw new Error(
        `a queue cannot be sliced: it already hands each job to one worker, so ` +
          `declaring a slice of '${channel}' would be two mechanisms dividing ` +
          'one stream',
      )
    }
    const info = await this.channel(channel, { timeout: askTimeout })
    this._mustBe(info, want)

    // **Before the subscribe, and only where there is no position.**
    // Before, or records from the old position are already in flight and
    // race the seek. Only where there is none, because a value written
    // once in the code would otherwise fire on every restart: a client
    // asking for the floor would replay the whole channel every Monday.
    // `sessionPresent` is how the broker says it has no session for this
    // id, and so no position.
    //
    // It cannot see one case: a session that exists but has never read
    // *this* channel. There is no verb for "where am I here", so the
    // channel's own `start:` setting decides that one.
    if (start !== null && want === 'append' && !this.sessionPresent) {
      await this._seek(channel, start, { timeout: askTimeout })
    }

    // **What is subscribed to and what arrives are not the same on a
    // queue.** A queue is joined through its pin, and its records arrive
    // on the queue's own topics, so routing on the pin would match
    // nothing that is ever delivered.
    const filters = info.pin ? [info.pin] : subscriptions(info, key)
    const arriving = info.pin ? [info.filter] : filters

    // **A second reader of a filter takes it over, and the first is
    // closed rather than left quietly empty.** Reading a channel,
    // seeking, and reading it again is an ordinary thing to do, so
    // refusing the second was wrong. What must not happen is two readers
    // interleaving on one filter.
    for (const held of [...this.#readers]) {
      if (arriving.some((one) => held.arriving.includes(one))) await held.close()
    }

    const reader = new Reader(this, {
      arriving, filters, timeout, queueName: info.pin ? channel : null,
    })
    // **Registered before the SUBSCRIBE, and before the caller asks for a
    // record.** The broker answers a SUBSCRIBE on a `latest` channel with
    // the current state at once, so a reader added after it misses that.
    this.#readers.add(reader)
    this.#sweep(reader)

    try {
      await this._subscribeAndCheck(filters, qos, askTimeout, declared)
    } catch (refused) {
      this.#readers.delete(reader)
      // What a channel *is* cannot change under a live connection, but
      // what this client may do there can. So the answer is dropped and
      // the next call asks again, while the broker's own refusal is
      // raised rather than retried against a broker that is saying no on
      // purpose.
      this.forgetChannel(channel)
      throw refused
    }
    return reader
  }

  /** Move this reader's records out of the unread buffer.
   *
   * Everything not this reader's keeps its place in the buffer, in the
   * order it arrived.
   */
  #sweep(reader) {
    const left = []
    for (const held of this.#unread.splice(0)) {
      if (held.generation !== this.#generation) {
        // **Dropped rather than read.** This record reached nobody on a
        // connection that has since gone, so it was never acknowledged,
        // and a session that survives the drop is sent it again, which is
        // what at-least-once means. Keeping it would hand the caller the
        // record twice and, worse, acknowledge it with the old
        // connection's packet identifier.
        continue
      }
      if (reader.wants(String(held.packet.topic))) reader.push(held)
      else left.push(held)
    }
    this.#unread.push(...left)
  }

  /** Stop routing to a reader, stop being sent its filters, and keep
   * whatever it was still holding.
   *
   * **Unsubscribing is the half that is easy to leave out, and on a queue
   * it takes work nobody will do.** A reader that has finished but stays
   * subscribed goes on being handed records, and a queue hands out
   * *leases*: jobs given to a client that will never answer, which then
   * time out, are retried, and are dead-lettered for no reason anybody
   * could see.
   */
  async _closeReader(reader) {
    this.#readers.delete(reader)
    // **In front of the buffer, not behind it.** Everything this reader
    // is handing back is older than anything the buffer holds for its
    // filters: the route was live until the line above.
    const left = reader.takeBack()
    if (left.length) this.#unread.unshift(...left)
    if (!this.mqtt?.connected) return
    try {
      await this._stopBeingSent(reader.filters)
    } catch {
      // **Never raises, and is always bounded.** Closing a reader has to
      // work on a connection that has already gone, where no answer is
      // coming. What is lost by giving up is that a record may still
      // arrive, and that record is kept rather than dropped.
    }
  }

  /** Unsubscribe, and wait for the broker to say it took effect.
   *
   * **Sending the UNSUBSCRIBE is not the same as being unsubscribed.**
   * Until the broker has processed it, a record published in the gap is
   * still delivered under the old subscription, so a reader that has
   * finished goes on receiving, which on a queue is a job leased to a
   * worker that will never answer it.
   */
  _stopBeingSent(filters) {
    return deadline(
      this.mqtt.unsubscribeAsync(filters),
      UNSUBSCRIBE_TIMEOUT,
      `the broker did not answer an unsubscribe of ${filters.join(', ')}`,
    )
  }

  /** Subscribe and read the SUBACK, raising on a refused filter.
   *
   * MQTT answers each filter separately, and a client that does not read
   * the codes sits connected, subscribed to nothing, and receives nothing
   * for ever.
   */
  _subscribeAndCheck(filters, qos, timeout, declared = []) {
    const properties = {}
    if (declared.length) {
      // **One set of properties for the whole packet.** The filters here
      // are one channel's, expanded from one written filter, so they take
      // one slice between them, which is what the broker means by the
      // declaration being on the packet rather than on a filter.
      properties.userProperties = { [declared[0][0]]: declared.map(([, one]) => one) }
    }
    return deadline(new Promise((resolve, reject) => {
      // The map form, because MQTT.js takes a string, an array of them,
      // or `{ filter: { qos } }`, and answers the granted codes in the
      // order it read them.
      this.mqtt.subscribe(
        Object.fromEntries(filters.map((one) => [one, { qos }])), { properties },
        (err, granted, packet) => {
          const codes = packet?.granted ?? err?.packet?.granted ?? []
          const refused = filters
            .map((filter, at) => [filter, codes[at]])
            .filter(([, code]) => code === undefined || code >= 0x80)
          if (!refused.length && !err) return resolve(codes)
          if (!codes.length) return reject(err)
          // **The granted half is undone before the refusal is raised.**
          // MQTT answers each filter separately, so a SUBSCRIBE that is
          // partly refused leaves the client subscribed to the rest, and
          // to no reader, because the caller is about to be handed an
          // error instead of one. Those records then arrive for ever and
          // pile into the unread buffer; on a queue they are leases
          // handed to a worker that does not exist.
          const kept = filters.filter((_, at) => codes[at] < 0x80)
          if (kept.length) this.mqtt.unsubscribe(kept, () => {})
          reject(new SubscriptionRefused(
            refused, codes, packet?.properties?.reasonString ??
              err?.packet?.properties?.reasonString,
          ))
        },
      )
    }), timeout, `the broker did not answer a subscribe to ${filters.join(', ')}`)
  }

  /** MQTT's own subscribe, and the words a subscriber takes a slice with.
   *
   *     await client.subscribe(`iot/site42/#`, { topicHash: [8, 1] })
   *
   * `topicHash` is a `[partitions, index]` pair, or several of them for a
   * member holding more than one slice. The broker delivers a message
   * only where `topicHash(topic) % partitions` is one of the indices
   * declared, and a client declaring nothing gets everything, which is
   * every client that has never heard of this.
   *
   * **Here as well as on the channel verbs, because broadcast has no
   * verb.** A topic no channel claims is ordinary MQTT and is subscribed
   * to the ordinary way, so the one place a broadcast subscriber could
   * say this is the call it already makes.
   *
   * `saguinFilter` puts a call this library has not heard of on the
   * packet verbatim, so a broker that has grown one does not wait for a
   * release here.
   *
   * Unlike MQTT.js's own, this reads the SUBACK and raises on a refused
   * filter: a client that does not read the codes sits connected,
   * subscribed to nothing, and receives nothing for ever.
   */
  subscribe(filter, { qos = 1, topicHash, saguinFilter, timeout = ASK_TIMEOUT } = {}) {
    return this._subscribeAndCheck(
      Array.isArray(filter) ? filter : [filter], qos, timeout,
      declarations({ topicHash, saguinFilter }),
    )
  }

  /** Move this client's position in one channel, and answer where it
   * landed.
   *
   * The value is the broker's own: an integer offset (`0` the retention
   * floor, `-1` the next offset, or a position), or a string holding a
   * duration such as `12h` or `7d`, or an RFC 3339 moment. It is passed
   * through unchanged and never guessed at: **a bare integer always means
   * an offset**, because `1763000000` is a plausible offset and a
   * plausible Unix time, and seeking to the wrong one reads on in order
   * and reports success.
   */
  async _seek(channel, to, { timeout = ASK_TIMEOUT } = {}) {
    const info = await this.channel(channel, { timeout })
    this._mustBe(info, 'append')
    if (!this.durable) {
      throw new Error(
        'seeking moves a stored position, and a client that keeps no place ' +
          'has none; build it with { durable: true }',
      )
    }
    const reply = await this._ask(
      seekTopic(channel), String(to), timeout,
      `a seek of '${channel}' to '${to}'`,
    )
    return Number(reply.payload.toString())
  }

  /** Where this library says something nobody asked it to say.
   *
   * One place, so that an application can replace it: a library that
   * writes to the console and cannot be told not to is a library that
   * writes to somebody's log.
   */
  warn(said) {
    if (this.emit('warning', said)) return
    console.warn('saguin: ' + said)
  }

  // -- what the verbs are made of --------------------------------------

  /** This channel is that kind, or a refusal naming the verbs it takes. */
  _mustBe(info, want) {
    if (info.type !== want) throw new WrongChannelType(info, want)
  }

  /** A write by channel name: ask what the channel is, check it is the
   * kind this verb belongs to, build the topic from its filter and the
   * key, and publish. */
  async _write(want, channel, { key = [], value, headers, timeout = ASK_TIMEOUT,
    ...rest } = {}) {
    const info = await this.channel(channel, { timeout })
    this._mustBe(info, want)
    return this.publish(compose(info, key), value, { headers, ...rest })
  }

  /** One key's current value, by channel and key. */
  _pointRead(info, key, timeout) {
    return this._readTopic(compose(info, key), timeout)
  }

  /** One topic's current value, by topic rather than by channel and key,
   * which is what a schema pointer is.
   *
   * An empty answer is `null`: a key never set and one that was deleted
   * are the same answer, as they are everywhere else on this channel
   * type. */
  async _readTopic(topic, timeout = ASK_TIMEOUT) {
    const reply = await this._ask(
      KV_GET_TOPIC, topic, timeout, `a read of '${topic}'`,
    )
    return reply.payload && reply.payload.length ? reply.payload : null
  }

  /** Answer a queue offer: `ack` or `return`.
   *
   * On this connection rather than another, which is the protocol rather
   * than a convenience: the broker takes an answer only from the session
   * holding the job, so a second connection publishing the same words is
   * ignored and the job comes back on its lease.
   */
  _answerJob(job, outcome) {
    const replyTo = job.properties?.responseTopic
    const correlation = job.properties?.correlationData
    if (!replyTo || !correlation) {
      throw new Error(
        'this is not a queue delivery: it carries no Response Topic and ' +
          'Correlation Data to answer with',
      )
    }
    // **Marked answered here**, before the publish is awaited, so that
    // leaving the reading loop after acknowledging a job, which is an
    // ordinary thing to do, is not reported as abandoning it.
    job.answered = true
    return this._send(
      String(replyTo), outcome,
      { qos: 1, properties: { correlationData: correlation } },
      `an answer to a job on '${String(replyTo)}'`,
    )
  }

  /** Ask the broker to hang up another client, and answer what it said. */
  async _disconnectClient(clientId, timeout = ASK_TIMEOUT) {
    const reply = await this._ask(
      DISCONNECT_TOPIC, String(clientId), timeout,
      `a request to hang up '${clientId}'`,
    )
    return reply.payload.toString()
  }

  /** Publish, and answer what the broker acknowledged it with.
   *
   * **The PUBACK is where a refusal arrives**, so it is read rather than
   * dropped: MQTT.js hands the failure to the callback as an error and
   * the packet beside it, and saguin's own sentence is in that packet's
   * Reason String. `publishAsync` rejects with the error alone, which
   * would turn a broker that explained itself into "Implementation
   * specific error".
   */
  _send(topic, payload, options, about) {
    const client = this.mqtt
    if (!client) {
      throw new Error(
        `this client is not connected, so it cannot send ${about} - call ` +
          'start() first',
      )
    }
    return new Promise((resolve, reject) => {
      client.publish(topic, payload ?? '', options, (err, packet) => {
        if (!err) return resolve(packet)
        if (err.code === undefined) return reject(err)
        reject(new RequestRefused(about, err.code, packet?.properties?.reasonString))
      })
    })
  }

  /** Ask the broker a question and wait for the answer it writes back to
   * this connection.
   *
   * No subscription is involved: the reply is written to the asking
   * socket, so nobody else can receive it. **The PUBACK is read first**,
   * because that is where a refused request is answered: a request the
   * broker refused never produces a reply, and waiting for one would turn
   * the broker's own sentence into a timeout saying nothing.
   */
  async _ask(topic, payload, timeout, about) {
    if (!this.connected) {
      throw new Error(
        `this client is not connected, so it cannot ask the broker ${about} - ` +
          'a question needs a connection for the answer to arrive on',
      )
    }
    this.#asked += 1
    const correlation = `saguin-${this.#asked}`
    // **Registered before the publish**, because the answer can arrive on
    // the socket before the line after `publish` runs, and an answer
    // nobody is waiting for is dropped and waited out as a timeout.
    let arrived
    const answer = new Promise((resolve) => {
      arrived = resolve
      this.#answers.set(correlation, resolve)
    })
    try {
      await deadline(
        this._send(topic, payload, {
          qos: 1,
          properties: {
            responseTopic: this.#replyTopic,
            correlationData: Buffer.from(correlation),
          },
        }, about),
        timeout,
        `the broker did not acknowledge ${about} within ${timeout}ms`,
      )
      return await deadline(
        answer,
        timeout,
        `the broker did not answer ${about} within ${timeout}ms`,
      )
    } finally {
      this.#answers.delete(correlation)
      void arrived
    }
  }
}

function correlationOf(packet) {
  const raw = packet?.properties?.correlationData
  return raw === undefined ? undefined : Buffer.from(raw).toString()
}

/** The caller's properties with these headers added as User Properties.
 *
 * A copy, never the caller's own object: a publisher that builds one set
 * of properties and reuses it would otherwise accumulate every message's
 * headers on every message after it.
 */
function withHeaders(properties, headers) {
  if (!headers) return properties
  const props = copyProperties(properties)
  for (const [name, value] of pairsOf(headers)) {
    append(props.userProperties, String(name), String(value))
  }
  return props
}

/** The properties to send, and the id they carry.
 *
 * Both together, rather than reading the id back off the object
 * afterwards: MQTT.js carries one User Property as a bare string and
 * several as an array of them, so a reader has to normalize a shape this
 * function already knows.
 */
function withMessageId(properties, saguinId) {
  const props = copyProperties(properties)
  const existing = props.userProperties['saguin-id']
  let went = saguinId
  if (went === undefined || went === null) {
    went = Array.isArray(existing) ? existing[0] : existing
  }
  if (went === undefined || went === null) went = newMessageId()
  // **Replaced rather than appended**, because a record may carry one id
  // and MQTT 5 permits a User Property more than once: a publisher who
  // wrote one into the properties and then asked for another would
  // otherwise send both.
  props.userProperties['saguin-id'] = String(went)
  return [props, String(went)]
}

function copyProperties(properties) {
  const props = { ...properties }
  const copied = {}
  for (const [name, value] of Object.entries(properties?.userProperties ?? {})) {
    copied[name] = Array.isArray(value) ? [...value] : value
  }
  props.userProperties = copied
  return props
}

function append(userProperties, name, value) {
  const held = userProperties[name]
  if (held === undefined) userProperties[name] = value
  else if (Array.isArray(held)) held.push(value)
  else userProperties[name] = [held, value]
}

function pairsOf(headers) {
  if (Array.isArray(headers)) return headers
  if (headers instanceof Map) return [...headers]
  return Object.entries(headers)
}

/** A promise, or a failure saying what was waited for.
 *
 * The timer is cleared either way: a pending one keeps Node alive, and a
 * suite whose broker has answered everything would sit there until the
 * longest timeout it ever set had run out.
 */
function deadline(promise, ms, said) {
  let timer
  const bell = new Promise((resolve, reject) => {
    timer = setTimeout(() => reject(new Error(said)), ms)
  })
  return Promise.race([promise, bell]).finally(() => clearTimeout(timer))
}
