#!/usr/bin/env node
/**
 * A guided tour of saguin-js, against a real broker.
 *
 *     SAGUIN_BROKER=/path/to/saguin node examples/demo.js
 *
 * It starts a broker of its own, walks through every verb the library
 * has, and then goes out of its way to **break things**, because what a
 * developer needs from a tour is not only the shape of the working call:
 * it is what comes back when the call is wrong, and whether that tells
 * them enough to fix it.
 *
 * Nothing here is set up behind your back. The broker's configuration is
 * `examples/saguin.yaml`, the channels are the ones in it, and every call
 * in the tour is one you could type.
 */

import { readFileSync } from 'node:fs'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

import { Client, partition } from '../src/index.js'
// **The tour borrows the suite's broker rather than starting one its own
// way.** Both would otherwise hold a copy of "where is the binary, and
// how do I wait for it to listen", with two different refusals when it is
// missing: one rule in two places is how two copies drift apart.
import { Broker, brokerBinary } from '../tests/broker.js'

const HERE = dirname(fileURLToPath(import.meta.url))
const BOLD = '\u001b[1m'
const DIM = '\u001b[2m'
const GREEN = '\u001b[32m'
const YELLOW = '\u001b[33m'
const OFF = '\u001b[0m'

const say = (text) => console.log(`\n${BOLD}== ${text}${OFF}`)
const note = (text = '') => console.log(`   ${DIM}${text}${OFF}`)
const code = (text) => {
  for (const line of text.trim().split('\n')) console.log(`   ${GREEN}${line}${OFF}`)
}
/** Something the tour just did, said out loud.
 *
 * **Every action is announced**, because a tour that publishes quietly
 * and then shows three records where you counted one is teaching you
 * nothing: it is asking you to trust it. */
const did = (text) => console.log(`   ${YELLOW}-> ${text}${OFF}`)
const shown = (label, value) => console.log(`   ${label.padEnd(24)} ${value}`)

/** What came back when the call was wrong. The whole point of the last
 * section: an error that does not say what to do next is a defect in this
 * library. */
const broke = (err) => console.log(`   ${DIM}${err.name}:${OFF} ${err.message}`)

const AVRO = JSON.stringify({
  type: 'record',
  name: 'Reading',
  fields: [{ name: 'site', type: 'string' }, { name: 'temp', type: 'double' }],
})
const PROTO = `syntax = "proto3";
message Job {
  string order = 1;
  int32 items = 2;
}
`

async function drain(reader) {
  const got = []
  for await (const record of reader) got.push(record)
  return got
}

async function main() {
  const workdir = mkdtempSync(join(tmpdir(), 'saguin-js-demo-'))
  const config = readFileSync(join(HERE, 'saguin.yaml'), 'utf8')
  const broker = await new Broker(brokerBinary(), workdir, config).start()
  const opened = []
  const open = async (name, options = {}) => {
    const client = new Client(name, { schemaRegistry: 'schemas', ...options })
    await client.start(broker.url)
    opened.push(client)
    return client
  }

  try {
    say('0. a broker, and the configuration it is running')
    note('Started from examples/saguin.yaml, on a free port. Every channel the')
    note('tour uses is in that file, and nothing is created at runtime.')
    shown('broker', broker.url)
    for (const line of config.split('\n')) {
      if (/^\s{2}- \w+:/.test(line)) note('channel ' + line.trim().slice(2, -1))
    }

    const client = await open('gateway-1')

    say('1. what a channel is, asked once and remembered')
    code(`const readings = await client.channel('readings')`)
    const readings = await client.channel('readings')
    shown('type', readings.type)
    shown('filter', readings.filter)
    shown('verbs granted', readings.verbs.join(', '))
    note('The filter lives in the operator\'s configuration and is the one thing')
    note('a client cannot work out for itself, so it asks.')

    say('2. appending, with the key filling in the filter')
    code(`await client.append.publish('readings', {
  key: ['site42', 'device', 'temp/1'],
  value: '21.5',
  headers: { unit: 'C' },
})`)
    const sent = await client.append.publish('readings', {
      key: ['site42', 'device', 'temp/1'], value: '21.5', headers: { unit: 'C' },
    })
    did(`published, and the broker stored it under ${sent.saguinId}`)
    shown('composed topic', 'iot/site42/device/temp/1')

    say('3. reading it back, and keeping a place')
    code(`const reader = new Client('tour-reader', { durable: true })
for await (const record of await reader.append.consume('readings', {
  key: ['site42'],
})) { ... }`)
    const reader = await open('tour-reader', { durable: true })
    const records = await reader.append.consume('readings', {
      key: ['site42'], start: 0, timeout: 2000,
    })
    const read = await drain(records)
    did(`read ${read.length} record(s), and the loop acknowledged each one`)
    for (const record of read) {
      shown(record.topic, `${record.payload} (offset ${record.offset}, unit ` +
        `${record.headers.get('unit')})`)
    }
    note('A record is acknowledged when the loop asks for the next one, so a')
    note('reader that stops half way is served the same record again.')

    say('4. where to begin, and where to jump')
    code(`await reader.append.seek('readings', 0)   // the retention floor`)
    const landed = await reader.append.seek('readings', 0)
    did(`the stored position is now offset ${landed}`)
    const again = await drain(await reader.append.consume('readings', {
      key: ['site42'], timeout: 2000,
    }))
    did(`reading again from there gave ${again.length} record(s)`)

    say('5. state: a key-value store that survives a restart')
    code(`await client.latest.set('state', { key: ['site42', 'temp'], value: '18' })
await client.latest.get('state', { key: ['site42', 'temp'] })`)
    await client.latest.set('state', { key: ['site42', 'temp'], value: '18' })
    did('set site42/temp to 18')
    shown('get', await client.latest.get('state', { key: ['site42', 'temp'] }))
    await client.latest.delete('state', { key: ['site42', 'temp'] })
    did('deleted it')
    shown('get after delete', String(await client.latest.get('state', {
      key: ['site42', 'temp'],
    })))
    note('A key never set and a deleted one are the same answer, as they have')
    note('always been on this channel type.')

    say('6. work: one job, one worker, one answer')
    code(`for await (const job of await worker.queue.fetch('tasks')) {
  await worker.queue.ack(job)      // or .nack(job) to hand it back
}`)
    const worker = await open('tour-worker', { durable: true })
    const jobs = await worker.queue.fetch('tasks', { timeout: 3000 })
    await client.queue.publish('tasks', { key: ['site42', '1'], value: 'pack it' })
    did('queued one job')
    const { value: job } = await jobs.next()
    shown('taken', `${job.payload} (attempt ${job.attempt})`)
    await worker.queue.nack(job)
    did('handed it back, which spends the attempt')
    await jobs.close()

    say('7. dead letters, and putting the work back')
    note('The queue is configured for one attempt, so the job handed back above')
    note('is already in tasks__dlq, with the broker\'s own account of why.')
    const operator = await open('tour-operator', { durable: true })
    const dead = await drain(await operator.append.consume('tasks__dlq', {
      start: 0, timeout: 5000,
    }))
    for (const record of dead) {
      shown('dead letter', `${record.payload} from '${record.dlq.channel}', ` +
        `${record.dlq.reason} after ${record.dlq.attempts} attempt(s)`)
    }
    if (dead.length) {
      await operator.queue.redrive('tasks', dead[0])
      did(`put it back on the queue, keeping its own id ${dead[0].id}`)
      const back = await drain(await operator.queue.fetch('tasks', { timeout: 3000 }))
      for (const one of back) {
        shown('back on the queue', `${one.payload} at ${one.topic}, attempt ${one.attempt}`)
        await operator.queue.ack(one)
      }
    }

    say('8. slices: two members split a channel, with no coordinator')
    code(`await member.append.consume('readings', { topicHash: [2, 0] })`)
    const members = []
    for (const index of [0, 1]) {
      const member = await open(`tour-member-${index}`, { durable: true })
      members.push({
        index,
        records: await member.append.consume('readings', {
          key: ['split'], topicHash: [2, index], timeout: 2500,
        }),
      })
    }
    const devices = ['a', 'b', 'c', 'd', 'e', 'f']
    for (const device of devices) {
      await client.append.publish('readings', {
        key: ['split', 'device', device], value: device,
      })
    }
    did(`published ${devices.length} records across ${devices.length} topics`)
    let total = 0
    for (const member of members) {
      const mine = await drain(member.records)
      total += mine.length
      shown(`member ${member.index} of 2`, mine.map((one) => one.payload).join(' ') || '(none)')
      for (const one of mine) {
        if (partition(one.topic, 2) !== member.index) {
          throw new Error(`${one.topic} went to the wrong member`)
        }
      }
    }
    shown('records in all', `${total} of ${devices.length}, each to one member`)

    say('9. schemas: a channel and a convention')
    code(`await client.latest.set('schemas', { key: ['acme/weather/v1'], value: AVRO })
await client.append.publish('readings', {
  key: ['written', 'sensor', 'w'], value: { site: 'site42', temp: 21.5 },
  schema: 'schemas/acme/weather/v1',
})`)
    await client.latest.set('schemas', { key: ['acme/weather/v1'], value: AVRO })
    await client.latest.set('schemas', { key: ['acme/job/v1'], value: PROTO })
    did('registered an avro schema and a protobuf one, as ordinary values')

    const schemaReader = await open('tour-schemas', { durable: true })
    const reading = await schemaReader.append.consume('readings', {
      key: ['written'], timeout: 2500,
    })
    await client.append.publish('readings', {
      key: ['written', 'sensor', 'w'], value: { site: 'site42', temp: 21.5 },
      schema: 'schemas/acme/weather/v1',
    })
    await client.latest.set('state', {
      key: ['written', 'summary'], value: { site: 'site42', temp: 19.5 },
      schema: 'schemas/acme/weather/v1',
    })
    await client.queue.publish('tasks', {
      key: ['site42', '2'], value: { order: 'A-1', items: 3 },
      schema: 'schemas/acme/job/v1',
    })
    did('wrote one record on each channel type, each against a schema')

    for (const record of await drain(reading)) {
      shown('append, deserialized', JSON.stringify(await record.deserialized()))
      shown('content type', record.properties.contentType)
    }
    const summary = await open('tour-state', { durable: true })
    for (const record of await drain(await summary.latest.consume('state', {
      key: ['written'], timeout: 2500,
    }))) {
      shown('latest, deserialized', JSON.stringify(await record.deserialized()))
      shown('content type', record.properties.contentType)
    }
    const jobReader = await open('tour-jobs', { durable: true })
    for (const record of await drain(await jobReader.queue.fetch('tasks', {
      timeout: 2500,
    }))) {
      shown('queue, deserialized', JSON.stringify(await record.deserialized()))
      shown('content type', record.properties.contentType)
      await jobReader.queue.ack(record)
    }

    say('10. an operator\'s verb: hanging up a client')
    code(`await client.admin.disconnect('tour-reader')`)
    shown('disconnect tour-reader', await client.admin.disconnect('tour-reader'))
    shown('disconnect nobody', await client.admin.disconnect('no-such-device'))
    note('Two answers rather than one, because "that device went away an hour')
    note('ago" and "you have misspelled the id" are different problems.')

    say('11. and now the things that go wrong')
    note('Every refusal below names what was wrong and what to do instead. An')
    note('error that does not is a defect in this library.')

    try {
      await client.append.publish('readings', { key: ['site42', 'gadget'], value: 'x' })
    } catch (err) {
      broke(err)
    }
    try {
      await client.append.publish('tasks', { key: ['site42', '1'], value: 'x' })
    } catch (err) {
      broke(err)
    }
    try {
      await client.channel('no-such-channel')
    } catch (err) {
      broke(err)
    }
    try {
      await client.append.seek('readings', 0)
    } catch (err) {
      broke(err)
    }
    try {
      await client.append.publish('readings', {
        key: ['site42', 'sensor', 'x'], value: { site: 'site42' },
        schema: 'schemas/acme/weather/v1',
      })
    } catch (err) {
      broke(err)
    }
    try {
      await client.schemaText('iot/site42/state/temp')
    } catch (err) {
      broke(err)
    }
    try {
      await client.publish('$saguin/nonsense', 'x')
    } catch (err) {
      broke(err)
    }

    say('Done')
    note('Everything above ran against a real broker started from')
    note('examples/saguin.yaml, and every record on the screen was announced')
    note('before it was shown.')
  } finally {
    for (const one of opened) await one.close().catch(() => {})
    await broker.stop().catch(() => {})
    rmSync(workdir, { recursive: true, force: true })
  }
}

await main()
