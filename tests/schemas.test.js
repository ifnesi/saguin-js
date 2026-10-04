/**
 * Serializing a payload against a schema, and reading one back.
 *
 * What saguin promises, and what these measure against. A schema registry
 * is a `latest` channel and a convention: the schema text is published to
 * a topic in it, retired with a zero-length payload, and a producer names
 * **that topic**, not an id, in a User Property called `schema`. A
 * consumer reads the property, point-reads the topic, and caches the
 * answer. The pointer is a whole topic because a bare name lets two
 * publishers in different domains choose the same one and lets the second
 * silently replace the first. And a consumer follows a pointer **only
 * where it lands inside the registry's own filter**, since an ACL governs
 * who may write a topic and never who may name one.
 */

import assert from 'node:assert/strict'
import { test } from 'node:test'

import { Client, SchemaError, inside } from '../src/index.js'
import { AVRO_TYPES, PROTOBUF_TYPES, SCHEMA_TYPES, formatOf } from '../src/schemas.js'
import { rejected, site, useBroker, useClients } from './fixtures.js'

const running = useBroker()
const clients = useClients(running)

const AVRO = JSON.stringify({
  type: 'record',
  name: 'Reading',
  fields: [{ name: 'site', type: 'string' }, { name: 'temp', type: 'double' }],
})

const PROTO = `syntax = "proto3";
message Reading {
  string site = 1;
  double temp = 2;
}
`

/** Register a schema: it is an ordinary write to the registry channel. */
function register(client, topic, text) {
  return client.latest.set('schemas', { key: [topic.split('/').slice(1).join('/')], value: text })
}

async function take(reader, count) {
  const got = []
  for await (const record of reader) {
    got.push(record)
    if (got.length >= count) break
  }
  return got
}

// -- the matcher and the format, with no broker in them ---------------------

test('a pointer is followed only inside the registry', () => {
  // The one place this library matches rather than composes on a
  // publisher's say-so, and the reason is a leak: a publisher may name
  // any topic, including a `latest` channel holding device state.
  assert.equal(inside('schemas/#', 'schemas/acme/weather/v1'), true)
  assert.equal(inside('schemas/#', 'schemas'), true)
  assert.equal(inside('schemas/#', 'state/site42/temp'), false)
  assert.equal(inside('iot/+/schemas/+', 'iot/acme/schemas/v1'), true)
  assert.equal(inside('iot/+/schemas/+', 'iot/acme/schemas/v1/deeper'), false)
  // Braces are expanded first, since that is what the broker matches on.
  assert.equal(inside('schemas/{acme,other}/#', 'schemas/acme/v1'), true)
  assert.equal(inside('schemas/{acme,other}/#', 'schemas/third/v1'), false)
})

test('the format is read off the schema when nobody said', () => {
  // A `.proto` can only be read by protobuf and an avro schema is JSON,
  // so the schema settles the question a missing Content Type would have
  // answered. It is not a guess about the payload.
  assert.ok(PROTOBUF_TYPES.includes(formatOf(PROTO)))
  assert.ok(AVRO_TYPES.includes(formatOf(AVRO)))
  assert.equal(formatOf('neither one nor the other'), null)
})

// -- against a running broker -----------------------------------------------

for (const [kind, text] of [['avro', AVRO], ['protobuf', PROTO]]) {
  test(`${kind === 'avro' ? 'an' : 'a'} ${kind} payload goes out encoded and ` +
    'comes back decoded', async () => {
    const where = site()
    const topic = `schemas/${where}/${kind}`
    const producer = await clients.producer()
    await register(producer, topic, text)

    const reading = await clients.producer({
      clientId: `schema-reader-${kind}-` + where, durable: true,
    })
    const records = await reading.append.consume('events', {
      key: [where], timeout: 10_000,
    })

    await producer.append.publish('events', {
      key: [where, kind], value: { site: where, temp: 21.5 }, schema: topic,
    })
    const [record] = await take(records, 1)

    // **Encoded, and not merely bytes.** A payload the library never
    // encoded at all is bytes too, so what proves the writing half is
    // that the bytes are neither the JSON of the value nor as long as it.
    const asJson = Buffer.from(JSON.stringify({ site: where, temp: 21.5 }))
    assert.ok(Buffer.isBuffer(record.payload))
    assert.notDeepEqual(record.payload, asJson)
    assert.ok(record.payload.length < asJson.length,
      `the payload is no smaller than its JSON: ${record.payload.toString('hex')}`)
    assert.ok(record.payload.includes(where), 'the string field should be in there')

    // The pointer travels as an ordinary header, so a consumer with no
    // schema support still sees where to look.
    assert.equal(record.schema, topic)
    assert.equal(record.headers.get('schema'), topic)
    // And the publisher said how it wrote them, rather than leaving it to
    // be worked out.
    assert.ok(SCHEMA_TYPES.includes(record.properties.contentType),
      String(record.properties.contentType))

    const read = await record.deserialized()
    assert.equal(read.site, where)
    assert.equal(Number(read.temp), 21.5)
  })
}

test('decoding after the client closed says what is wrong', async () => {
  // Reading a schema reaches for the broker, so it needs the connection
  // the record was read on, and saying "the broker did not acknowledge
  // within 10s" about a closed client sends the reader hunting the wrong
  // thing.
  const where = site()
  const topic = `schemas/${where}/closed`
  const producer = await clients.producer()
  await register(producer, topic, AVRO)

  const reading = new Client('closer-' + where, {
    durable: true, schemaRegistry: 'schemas',
  })
  await reading.start(running.broker.url)
  const records = await reading.append.consume('events', {
    key: [where], timeout: 10_000,
  })
  await producer.append.publish('events', {
    key: [where, 'x'], value: { site: where, temp: 1.0 }, schema: topic,
  })
  const [record] = await take(records, 1)
  await reading.close()

  const refused = await rejected(record.deserialized())
  assert.match(refused.message, /not connected/)
})

test('a pointer outside the registry is refused', async () => {
  const where = site()
  const reading = await clients.producer({ clientId: 'nosy-' + where })
  const refused = await rejected(reading.schemaText(`iot/${where}/state/temp`))
  assert.ok(refused instanceof SchemaError, refused.message)
  assert.match(refused.message, /outside/)
  assert.match(refused.message, /schemas\/#/)
})

test('a client that was not told where schemas live says so', async () => {
  const untold = new Client('untold-' + site())
  await untold.start(running.broker.url)
  const refused = await rejected(untold.schemaText('schemas/anything'))
  assert.match(refused.message, /schemaRegistry/)
  await untold.close()
})

test('an unregistered schema is refused before anything is sent', async () => {
  // Which is the whole reason a schema must be registered first: a record
  // written against a schema nobody can fetch is a record nobody can
  // read.
  const where = site()
  const producer = await clients.producer()
  const refused = await rejected(producer.append.publish('events', {
    key: [where, 'x'], value: { site: where, temp: 1.0 },
    schema: `schemas/${where}/never-registered`,
  }))
  assert.match(refused.message, /no schema is registered/)
})

test('a payload that does not fit the schema is refused', async () => {
  const where = site()
  const topic = `schemas/${where}/fit`
  const producer = await clients.producer()
  await register(producer, topic, AVRO)

  const refused = await rejected(producer.append.publish('events', {
    key: [where, 'x'], value: { site: where }, schema: topic, // temp missing
  }))
  assert.ok(refused instanceof SchemaError, refused.message)
  assert.match(refused.message, /does not fit/)
})

test('a record naming no schema says so', async () => {
  const where = site()
  const [producer, reader] = await Promise.all([clients.producer(), clients.reader()])
  await reader.subscribe(`iot/${where}/events/+`)
  await producer.append.publish('events', { key: [where, 'plain'], value: 'bytes' })
  const record = producer.record(await reader.next())

  assert.equal(record.schema, undefined)
  const refused = await rejected(record.deserialized())
  assert.match(refused.message, /names no schema/)
})

test('a schema is remembered until it is forgotten', async () => {
  // The topic is the identity and a new version is a new topic, so a
  // republished schema is not noticed: what makes that safe is that
  // forgetting is a verb.
  const where = site()
  const topic = `schemas/${where}/remembered`
  const producer = await clients.producer()
  await register(producer, topic, AVRO)

  const reading = await clients.producer({ clientId: 'remember-' + where })
  assert.equal(await reading.schemaText(topic), AVRO)

  const changed = JSON.stringify({
    type: 'record',
    name: 'Reading',
    fields: [{ name: 'site', type: 'string' }, { name: 'temp', type: 'double' },
      { name: 'unit', type: 'string', default: 'C' }],
  })
  await register(producer, topic, changed)

  assert.equal(await reading.schemaText(topic), AVRO,
    'the same client re-read a schema it had already been given')
  reading.forgetSchema(topic)
  assert.equal(await reading.schemaText(topic), changed,
    'forgetting a schema did not make the next read ask again')
})

test('a registry that is not a latest channel says so', async () => {
  const wrong = new Client('wrongreg-' + site(), { schemaRegistry: 'events' })
  await wrong.start(running.broker.url)
  const refused = await rejected(wrong.schemaText('iot/x/events/y'))
  assert.match(refused.message, /key-value store of schema texts/)
  await wrong.close()
})

for (const [name, write] of [
  ['append.publish', (c, topic, where) => c.append.publish('events', {
    key: [where, 'x'], value: { site: where, temp: 1.0 }, schema: topic,
  })],
  ['latest.set', (c, topic, where) => c.latest.set('state', {
    key: [where, 'temp'], value: { site: where, temp: 1.0 }, schema: topic,
  })],
  ['queue.publish', (c, topic, where) => c.queue.publish('tasks', {
    key: [where, '1'], value: { site: where, temp: 1.0 }, schema: topic,
  })],
]) {
  test(`${name} takes a schema`, async () => {
    // Every write path is one publish underneath, so a schema works on
    // all three or the one it does not work on is a surprise.
    const where = site()
    const topic = `schemas/${where}/${name.replace('.', '-')}`
    const producer = await clients.producer()
    await register(producer, topic, AVRO)

    const sent = await write(producer, topic, where)
    assert.ok(sent.saguinId)
  })
}

test('the Content Type a publisher names is the one that goes out', async () => {
  // The publisher's wins where there is one, and the schema settles it
  // only where nobody said: a publisher saying what it sent is better
  // evidence than anything inferred about it. Avro has four spellings in
  // the wild, and a library that inferred over the top of one would
  // rewrite a publisher's own header under it.
  const where = site()
  const topic = `schemas/${where}/named`
  const [producer, reader] = await Promise.all([clients.producer(), clients.reader()])
  await register(producer, topic, AVRO)
  await reader.subscribe(`iot/${where}/events/+`)

  await producer.append.publish('events', {
    key: [where, 'x'], value: { site: where, temp: 1.5 }, schema: topic,
    properties: { contentType: 'avro/binary' },
  })
  const packet = await reader.next()
  assert.equal(packet.properties.contentType, 'avro/binary',
    'the publisher\'s Content Type was replaced by the inferred one')

  // And it is still readable, because the spelling names the same reader.
  const record = producer.record(packet)
  assert.deepEqual(await record.deserialized(), { site: where, temp: 1.5 })
})
