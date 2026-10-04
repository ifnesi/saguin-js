/**
 * Serializing a payload against a schema, and reading one back.
 *
 * **A schema registry needs nothing from the broker.** A `latest` channel
 * is already a key-value store with delete, readable one key at a time,
 * so a registry is a channel and a convention: register by publishing the
 * schema text to a topic in it, retire with a zero-length payload,
 * produce with a User Property named `schema` carrying **the schema's
 * topic**, and consume by reading that property and point-reading the
 * topic it names.
 *
 * **The pointer is a whole topic rather than a bare id**, which is what
 * makes the convention work without an allocator: a bare name lets two
 * publishers in different domains pick the same one and lets the second
 * silently replace the first, and says nothing about where to look it up.
 *
 * The two readers are optional peers rather than dependencies: an edge
 * box that publishes bytes should not
 * install a protobuf compiler to do it. `avsc` reads avro and
 * `protobufjs` reads proto3, and each is loaded the first time it is
 * needed.
 */

import { createHash } from 'node:crypto'

export const PROTOBUF_TYPES = [
  'application/x-protobuf', 'application/protobuf',
  'application/vnd.google.protobuf',
]
export const AVRO_TYPES = [
  'application/avro', 'application/x-avro', 'avro/binary',
  'application/vnd.apache.avro+binary',
]
export const SCHEMA_TYPES = [...PROTOBUF_TYPES, ...AVRO_TYPES]

/** The User Property that points at a schema. Not `saguin-schema`, which
 * is the name that looks most official and is the one that would not
 * survive: the reserved prefix is stripped from anything a client
 * sends. */
export const SCHEMA_PROPERTY = 'schema'

/** A schema could not be found, read, or used on these bytes. */
export class SchemaError extends Error {
  constructor(said) {
    super(said)
    this.name = 'SchemaError'
  }
}

const compiled = new Map()

/** Which reader a schema needs, read off the schema itself.
 *
 * A fallback for a publisher that names a schema and no Content Type,
 * which is the common shape in the wild. It is not a guess about the
 * payload: a `.proto` can only be read by protobuf and an Avro schema is
 * JSON, so the schema settles the question the missing header would have
 * answered. Where a header is present it wins: a publisher saying what it
 * sent is better evidence than anything inferred about it.
 */
export function formatOf(text) {
  const head = text.trimStart().slice(0, 400)
  if (head.startsWith('syntax') || head.includes('message ') || head.includes('package ')) {
    return PROTOBUF_TYPES[0]
  }
  let doc
  try {
    doc = JSON.parse(text)
  } catch {
    return null
  }
  return doc !== null && typeof doc === 'object' ? AVRO_TYPES[0] : null
}

/** The Content Type to use, and whether it was inferred rather than sent.
 *
 * The publisher's wins where there is one; otherwise the schema itself
 * says which reader it needs. Those are different claims and the caller
 * is told which it got.
 */
export function readerFor(contentType, topic, text) {
  const said = contentType?.toLowerCase().split(';')[0]
  if (said && SCHEMA_TYPES.includes(said)) return [said, false]
  const inferred = formatOf(text)
  if (inferred === null) {
    throw new SchemaError(
      `no Content Type on the message, and the schema at '${topic}' is neither ` +
        'proto3 nor an avro schema - so nothing says how to read these bytes',
    )
  }
  return [inferred, true]
}

async function load(name, what, extra) {
  try {
    return await import(name)
  } catch {
    throw new SchemaError(
      `reading ${what} needs \`${name}\`, which is not installed - ` +
        `npm install ${name}${extra ?? ''}`,
    )
  }
}

/** The message type a proto3 schema describes, compiled and cached.
 *
 * **Keyed by the schema's topic and a digest of its text**, so a schema
 * republished at the same topic is compiled again rather than served from
 * the last one. Each schema is parsed into a root of its own, so two
 * schemas that name the same message cannot collide.
 */
export async function compiledMessage(topic, text) {
  const digest = createHash('sha256').update(text).digest('hex')
  const hit = compiled.get(topic)
  if (hit && hit.digest === digest) return hit.type

  const protobuf = await load('protobufjs', 'a protobuf payload')
  let root
  try {
    // `keepCase` keeps the field names the schema wrote, which is what a
    // consumer reading the `.proto` expects to see on the decoded object.
    root = (protobuf.default ?? protobuf).parse(text, { keepCase: true }).root
  } catch (failed) {
    throw new SchemaError(`the schema at '${topic}' is not valid proto3: ${failed.message}`)
  }

  const names = []
  const walk = (node) => {
    for (const child of Object.values(node.nested ?? {})) {
      if (child.fields) names.push(child.fullName.replace(/^\./, ''))
      if (child.nested) walk(child)
    }
  }
  walk(root)
  if (!names.length) throw new SchemaError(`the schema at '${topic}' defines no message`)

  // **One message per schema topic is the convention.** The pointer is a
  // whole topic, so a schema holds the one thing that topic names. Where
  // a file holds several, the one whose name matches the topic wins and
  // the rest are listed rather than guessed between.
  let chosen = names.length === 1 ? names[0] : null
  if (chosen === null) {
    const want = topic.replace(/\/+$/, '').split('/').slice(-2)
      .map((one) => one.toLowerCase().replace(/_/g, ''))
    chosen = names.find(
      (one) => want.includes(one.split('.').pop().toLowerCase().replace(/_/g, '')),
    ) ?? null
  }
  if (chosen === null) {
    throw new SchemaError(
      `the schema at '${topic}' defines ${[...names].sort().join(', ')} and ` +
        'nothing says which describes this message - the convention is one ' +
        'message per schema topic',
    )
  }

  const type = root.lookupType(chosen)
  if (compiled.size > 200) compiled.clear()
  compiled.set(topic, { digest, type })
  return type
}

async function avroType(topic, text) {
  const avsc = await load('avsc', 'an avro payload')
  try {
    return (avsc.default ?? avsc).Type.forSchema(JSON.parse(text))
  } catch (failed) {
    throw new SchemaError(`the schema at '${topic}' is not valid avro: ${failed.message}`)
  }
}

/** Serialize a value against the schema at `topic`.
 *
 * Answers the bytes and the Content Type they were written with, so a
 * publisher can say what it sent rather than leaving a consumer to work
 * it out.
 */
export async function serialize(value, topic, text, contentType = undefined) {
  const [ctype] = readerFor(contentType, topic, text)
  if (PROTOBUF_TYPES.includes(ctype)) {
    const type = await compiledMessage(topic, text)
    const wrong = type.verify(value)
    if (wrong) throw new SchemaError(`this does not fit ${type.name}: ${wrong}`)
    return [Buffer.from(type.encode(type.create(value)).finish()), ctype]
  }
  const type = await avroType(topic, text)
  try {
    return [type.toBuffer(value), ctype]
  } catch (failed) {
    throw new SchemaError(
      `this does not fit the avro schema at '${topic}': ${failed.message}`,
    )
  }
}

/** One payload, read through the schema its headers name. */
export async function deserialize(raw, topic, text, contentType = undefined) {
  const [ctype] = readerFor(contentType, topic, text)
  if (PROTOBUF_TYPES.includes(ctype)) {
    const type = await compiledMessage(topic, text)
    try {
      return type.toObject(type.decode(raw), { defaults: true, longs: String })
    } catch (failed) {
      throw new SchemaError(`these bytes are not ${type.name}: ${failed.message}`)
    }
  }
  const type = await avroType(topic, text)
  try {
    // **A plain object, as the protobuf half answers.**
    // avsc hands back an instance of a class
    // it generated for the schema, which reads the same but is not equal
    // to the object a consumer writes in a test. The fields themselves
    // are left exactly as avsc decoded them.
    const decoded = type.fromBuffer(Buffer.from(raw))
    const plain = decoded !== null && typeof decoded === 'object' &&
      !Array.isArray(decoded) && !Buffer.isBuffer(decoded)
    return plain ? { ...decoded } : decoded
  } catch (failed) {
    throw new SchemaError(
      `these bytes do not fit the avro schema at '${topic}': ${failed.message}`,
    )
  }
}
