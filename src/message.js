/**
 * What the broker says about a record, read off the delivery.
 *
 * Every field here arrives as an MQTT 5 User Property carrying a decimal
 * string, because that is what crosses a wire that has no types. Turning
 * them into a number, a Date and a list of the publisher's own headers is
 * the whole of this module, and it is this library's largest daily win:
 * without it every consumer writes the same four lookups, and each one of
 * them decides on its own what an absent property means.
 */

/** The broker stamps every delivery with the record's identity, position
 * and receipt time, adds the channel where a consumer's filter reaches
 * more than one, and adds the attempt on a queue offer. It strips
 * anything else a client sent under this prefix, so what arrives under it
 * is the broker's own word rather than a publisher's claim about it. */
export const RESERVED_PREFIX = 'saguin-'

/** One of the broker's RFC 3339 times, as a Date.
 *
 * A value this cannot read is answered as null rather than thrown over: a
 * delivery is not the place to discover that two libraries disagree about
 * a date format.
 */
function aMoment(raw) {
  if (!raw) return null
  const at = new Date(raw)
  return Number.isNaN(at.getTime()) ? null : at
}

function aNumber(raw) {
  if (raw === undefined || raw === null) return null
  const value = Number(raw)
  return Number.isNaN(value) ? null : value
}

/** Why a record is in a dead-letter channel rather than its queue.
 *
 * The broker writes these when it moves a record, and a client cannot
 * forge them: everything a publisher sends under the reserved prefix is
 * stripped, so a consumer reading them is reading the broker's own word
 * about work that failed.
 */
export class DeadLetter {
  constructor({ channel, offset, attempts, reason, at, first, last }) {
    /** The queue it came from, not the channel it is in now. */
    this.channel = channel
    /** Its offset in that queue. */
    this.offset = offset
    /** How many attempts were made. */
    this.attempts = attempts
    /** The broker's own word: `attempts_exhausted` or `expired`. */
    this.reason = reason
    /** When it was dead-lettered. */
    this.at = at
    /** When it was first handed to a worker, or null if it never was. */
    this.first = first
    /** When it was last handed to a worker, or null. */
    this.last = last
    Object.freeze(this)
  }
}

/** Every User Property on a delivery, in order, duplicates kept.
 *
 * MQTT.js carries one occurrence of a name as a string and several as an
 * array, so this is where that shape stops mattering. The order between
 * different names is the order they were parsed in; the order within one
 * name is the order it was sent in, which is what a repeated header is
 * for.
 */
export function userProperties(properties) {
  const found = []
  for (const [name, value] of Object.entries(properties?.userProperties ?? {})) {
    for (const one of Array.isArray(value) ? value : [value]) found.push([name, one])
  }
  return found
}

/** The publisher's own User Properties, in the order they were sent.
 *
 * Not a plain object, and that is the point. MQTT 5 permits a repeated
 * name and it is the standard way to carry a list; the broker forwards
 * the properties unaltered and in order, so a record keeps them as a
 * sequence. Flattening them into a map here would throw away what the
 * publisher sent and nothing would ever say so.
 *
 * `h.get('name')` answers the first value, `h.all('name')` answers every
 * one of them, and `Object.fromEntries(h)` is there for whoever knows
 * their own names are unique.
 */
export class Headers {
  #pairs

  constructor(pairs = []) {
    this.#pairs = [...pairs].map(([name, value]) => [String(name), value])
    Object.freeze(this)
  }

  /** Every [name, value] pair, in order, duplicates kept. */
  get pairs() {
    return [...this.#pairs]
  }

  /** Every name, in order, a repeated one appearing each time. */
  names() {
    return this.#pairs.map(([name]) => name)
  }

  /** Every value sent under this name, in order. Empty if none. */
  all(name) {
    return this.#pairs.filter(([one]) => one === name).map(([, value]) => value)
  }

  get(name, fallback = undefined) {
    const found = this.#pairs.find(([one]) => one === name)
    return found === undefined ? fallback : found[1]
  }

  has(name) {
    return this.#pairs.some(([one]) => one === name)
  }

  get size() {
    return this.#pairs.length
  }

  /** Iterates pairs rather than names, so `Object.fromEntries(headers)`
   * and `new Map(headers)` both work. */
  [Symbol.iterator]() {
    return this.#pairs[Symbol.iterator]()
  }
}

/** A delivery, with what saguin stamped on it already read.
 *
 * It wraps the MQTT.js packet rather than replacing it: `.packet` is the
 * object MQTT.js handed over, unchanged, and topic, payload, qos, retain,
 * dup and properties are all here under their MQTT.js names.
 *
 * One name means something different from MQTT's, and it is deliberate.
 * `timestamp` here is **the broker's receipt time**, which is what a
 * consumer asking how old a reading is wants.
 */
export class Message {
  #props
  #client

  constructor(packet, client = null, generation = null) {
    /** The MQTT.js packet, unchanged. */
    this.packet = packet
    this.#client = client
    /** Which connection this arrived on.
     *
     * A packet identifier is the broker's, per connection, and the next
     * connection numbers its own from 1, so an acknowledgement carrying
     * one from a dead connection would answer for whatever the live one
     * holds under that number. `ack` checks this. */
    this.generation = generation
    this.#props = userProperties(packet?.properties)
    /** The publisher's own User Properties, the reserved ones left out. */
    this.headers = new Headers(
      this.#props.filter(([name]) => !name.startsWith(RESERVED_PREFIX)),
    )
    /** A DeadLetter where this record failed out of a queue, else null. */
    this.dlq = this.#deadLetter()
    /** True once `queue.ack` or `queue.nack` has answered this job.
     *
     * A queue job is answered by a verb of its own rather than by the
     * reading loop moving on, so this is what tells a job that was
     * handled from one that was abandoned, and only the second is worth
     * warning about. */
    this.answered = false
  }

  #deadLetter() {
    const found = new Map(this.#props.filter(([name]) => name.startsWith('saguin-dlq-')))
    if (!found.has('saguin-dlq-channel')) return null
    return new DeadLetter({
      channel: found.get('saguin-dlq-channel'),
      offset: aNumber(found.get('saguin-dlq-offset')),
      attempts: aNumber(found.get('saguin-dlq-attempts')),
      reason: found.get('saguin-dlq-reason') ?? null,
      at: aMoment(found.get('saguin-dlq-at')),
      first: aMoment(found.get('saguin-dlq-first')),
      last: aMoment(found.get('saguin-dlq-last')),
    })
  }

  // -- what the packet already has ------------------------------------

  get topic() {
    // A string, whatever the packet holds: MQTT.js parses the topic as a
    // Buffer and decodes one only for its own `message` event.
    return String(this.packet.topic)
  }

  get payload() {
    return this.packet.payload
  }

  get qos() {
    return this.packet.qos
  }

  get retain() {
    return Boolean(this.packet.retain)
  }

  get dup() {
    return Boolean(this.packet.dup)
  }

  get properties() {
    return this.packet.properties
  }

  // -- what saguin stamped --------------------------------------------

  #reserved(name) {
    const found = this.#props.find(([one]) => one === name)
    return found === undefined ? null : found[1]
  }

  /** The record's Message ID: stable across redelivery, dead-lettering
   * and replay, which is what makes deduplicating on it work. Not the
   * MQTT packet identifier, which is per-hop and reused constantly. */
  get id() {
    return this.#reserved('saguin-id')
  }

  /** The record's position in its channel, or null off a channel.
   *
   * Deduplicating on this, by ignoring anything at or below the highest
   * you have processed on that channel, is what makes at-least-once exact
   * for a consumer that reconnects.
   */
  get offset() {
    return aNumber(this.#reserved('saguin-offset'))
  }

  /** When the broker received the record, as a Date.
   *
   * Null where the broker sent none: a record restored from a file
   * written before saguin stamped these carries no claim about its age
   * rather than a wrong one.
   */
  get timestamp() {
    const raw = this.#reserved('saguin-timestamp')
    return raw === null ? null : new Date(Number(raw))
  }

  /** Which channel the record is in, present only where the consumer's
   * filter reaches more than one, since a consumer whose filter reaches
   * exactly one already knows and would pay 26 bytes a message to be
   * told. Null is not "no channel"; it is "you did not need telling". */
  get channel() {
    return this.#reserved('saguin-channel')
  }

  /** Which delivery attempt of a queue job this is, starting at 1. Null
   * off a queue. */
  get attempt() {
    return aNumber(this.#reserved('saguin-attempt'))
  }

  /** True where this is state the consumer is catching up on rather than
   * a change that has just happened.
   *
   * It is the RETAIN flag, named for what it means here: a `latest`
   * channel sends the current value of every topic a filter reaches at
   * subscribe with the flag set, and every change after that without it.
   */
  get isCatchUp() {
    return Boolean(this.packet.retain)
  }

  /** The topic of the schema this payload was written against, or
   * undefined. An ordinary User Property, so it is among the publisher's
   * own headers too. */
  get schema() {
    return this.headers.get('schema')
  }

  /** The client this record was read through, or null where it was built
   * from a packet on its own. */
  get client() {
    return this.#client
  }

  /** Acknowledge this record, when it came from the `message` event.
   *
   * A record a `consume` loop handed you needs nothing: that loop
   * acknowledges it when it asks for the next one, which is what moves
   * the stored position. What arrives on the `message` event - broadcast,
   * and a resumed session's records before `consume` was called for
   * them - is the application's to answer for.
   */
  ack() {
    if (this.#client === null) {
      throw new Error(
        'this record was not read through a saguin client, so there is ' +
          'nothing here that can acknowledge it',
      )
    }
    this.#client.ack(this)
  }

  /** The payload read through the schema it names.
   *
   * **A method rather than a property**: the
   * schema is fetched from the broker rather than
   * carried on the record, so reading one is a question and a question is
   * awaited.
   *
   * Refused where the record names no schema, where this client was not
   * told which channel schemas live in, or where the pointer falls
   * outside that registry: a consumer must not follow a pointer out of
   * its own registry, since a publisher may name any topic.
   */
  async deserialized() {
    const { SchemaError, deserialize } = await import('./schemas.js')
    const pointer = this.schema
    if (!pointer) {
      throw new SchemaError(
        'this record names no schema, so there is nothing to read it through',
      )
    }
    if (this.#client === null) {
      throw new SchemaError(
        'this record was not read through a saguin client, so there is nothing ' +
          'here that can fetch its schema',
      )
    }
    const text = await this.#client.schemaText(pointer)
    return deserialize(this.payload, pointer, text, this.properties?.contentType)
  }
}
