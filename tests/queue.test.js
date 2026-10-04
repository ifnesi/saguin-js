/**
 * Work: taking it, answering for it, and what happens when it fails.
 *
 * What saguin promises, and what these measure against. A queue hands a
 * job to **one** worker at a time, through the queue's own subscription
 * form `$saguin/queue/<channel>` and no other. Every offer carries a
 * Response Topic and Correlation Data naming that delivery, and the
 * broker takes the answer only from the session holding the job: `ack`
 * resolves it, `return` hands it back and spends the attempt. A job whose
 * attempts run out is moved to the queue's dead-letter channel, which is
 * the queue's name with `__dlq` on the end, carrying the broker's own
 * account of why it failed under the reserved prefix.
 *
 * Putting work back is not a broker verb: it is a read and a republish,
 * because deciding that failed work should be tried again is a judgement
 * nobody but the operator can make.
 */

import assert from 'node:assert/strict'
import { test } from 'node:test'

import { Client, WrongChannelType } from '../src/index.js'
import { caught, rejected, site, useBroker, useClients } from './fixtures.js'

const running = useBroker()
const clients = useClients(running)

async function take(reader, count) {
  const got = []
  for await (const record of reader) {
    got.push(record)
    if (got.length >= count) break
  }
  return got
}

/** Fail a job once as a plain MQTT.js worker, which is what puts it in
 * the dead-letter channel on a queue configured for one attempt. */
async function failItOnce(queue) {
  const worker = await clients.reader()
  await worker.subscribe(`$saguin/queue/${queue}`, { qos: 1 })
  const job = await worker.next(15_000)
  await worker.answer(job, 'return')
  await worker.close()
  return job
}

test('a worker answers for its own job', async () => {
  const where = site()
  const producer = await clients.producer()
  const worker = await clients.producer({ clientId: 'acker-' + where, durable: true })

  const jobs = await worker.queue.fetch('tasks', { timeout: 15_000 })
  await producer.queue.publish('tasks', { key: [where, '1'], value: 'do it' })
  const [job] = await take(jobs, 1)

  assert.equal(job.payload.toString(), 'do it')
  assert.equal(job.attempt, 1)
  assert.equal(job.topic, `work/${where}/jobs/1`)
  await worker.queue.ack(job)
  assert.equal(job.answered, true)
  await jobs.close()

  // Acknowledged work is gone: a second worker is offered nothing.
  const later = await clients.producer({ clientId: 'second-' + where, durable: true })
  const nothing = await take(await later.queue.fetch('tasks', { timeout: 5000 }), 1)
  assert.deepEqual(nothing, [])
})

test('a queue is fetched through its pin and not narrowed', async () => {
  // A queue admits one subscription form and no other: two spellings
  // would be two consumer groups, each taking a copy of every job. So
  // fetch takes no key at all, and asking for one is refused by the
  // method rather than becoming a silent second group.
  const where = site()
  const worker = await clients.producer({ clientId: 'pinned-' + where, durable: true })

  const refused = caught(() => worker.queue.fetch('tasks', { key: [where] }))
  assert.ok(refused instanceof TypeError, refused.message)
  assert.match(refused.message, /one subscription form/)

  const producer = await clients.producer()
  const jobs = await worker.queue.fetch('tasks', { timeout: 15_000 })
  await producer.publish(`work/${where}/jobs/1`, 'do it')
  const [job] = await take(jobs, 1)
  assert.equal(job.payload.toString(), 'do it')
  assert.equal(job.attempt, 1)
  await worker.queue.ack(job)
  await jobs.close()
})

test('a worker acks what succeeded and hands back what threw', async () => {
  // The shape RabbitMQ's clients and the frameworks over them have: a job
  // that throws is handed back and **the worker carries on**. One that
  // stopped on a bad job would stop everything behind it.
  //
  // Seen through the attempt count, which is where it shows: a handed
  // back job comes straight back, to this same worker, since it is the
  // one asking, so what proves the nack is the second offer arriving as
  // attempt 2.
  const where = site()
  const producer = await clients.producer()
  await producer.queue.publish('retried', { key: [where, 'flaky'], value: 'flaky' })
  await producer.queue.publish('retried', { key: [where, 'fine'], value: 'fine' })

  const attempts = []
  const failures = []
  const worker = await clients.producer({ clientId: 'worker-' + where, durable: true })
  const handled = await worker.queue.work('retried', (job) => {
    attempts.push([job.payload.toString(), job.attempt])
    if (job.payload.toString() === 'flaky' && job.attempt === 1) {
      throw new Error('cannot pack it')
    }
  }, {
    onError: (job, failed) => failures.push([job.payload.toString(), failed.message]),
    timeout: 8000,
  })

  assert.ok(attempts.some(([p, a]) => p === 'flaky' && a === 1), JSON.stringify(attempts))
  assert.ok(attempts.some(([p, a]) => p === 'flaky' && a === 2), JSON.stringify(attempts))
  assert.deepEqual(attempts.filter(([p]) => p === 'fine'), [['fine', 1]])
  assert.equal(handled, 3, 'two jobs, one of them twice')
  assert.deepEqual(failures, [['flaky', 'cannot pack it']])

  // Both are resolved now, so nothing is left for anybody else.
  const later = await clients.producer({ clientId: 'after-' + where, durable: true })
  assert.deepEqual(
    await take(await later.queue.fetch('retried', { timeout: 5000 }), 1), [],
  )
})

test('a failing job is said out loud when nobody is watching', async () => {
  // With no onError, the failure goes somewhere rather than nowhere: a
  // queue that fails every job with nothing anywhere saying so is what
  // this is written to avoid.
  const where = site()
  const producer = await clients.producer()
  await producer.queue.publish('retried', { key: [where, '1'], value: 'boom' })

  const worker = await clients.producer({ clientId: 'quiet-' + where, durable: true })
  const said = []
  worker.on('warning', (one) => said.push(one))

  await worker.queue.work('retried', () => {
    throw new Error('it went wrong')
  }, { timeout: 6000 })

  assert.ok(said.some((one) => one.includes('it went wrong')),
    'a job that threw was handed back with nothing written anywhere')
})

test('a dead letter says which queue it came from and why', async () => {
  // The queue is configured for one attempt, so a worker that hands the
  // job back has exhausted it.
  const where = site()
  const producer = await clients.producer()
  const sent = await producer.queue.publish('tasks', {
    key: [where, '1'], value: 'do it',
  })
  await failItOnce('tasks')

  const reading = await clients.producer({ clientId: 'dlq-' + where, durable: true })
  const [dead] = await take(
    await reading.append.consume('tasks__dlq', { key: [where], timeout: 30_000 }), 1,
  )

  assert.equal(dead.payload.toString(), 'do it')
  assert.equal(dead.id, sent.saguinId, "a dead letter keeps the record's own id")
  assert.ok(dead.dlq, 'no dead-letter account on a record in a dead-letter channel')
  assert.equal(dead.dlq.channel, 'tasks')
  assert.equal(dead.dlq.reason, 'attempts_exhausted')
  assert.equal(dead.dlq.attempts, 1)
  assert.equal(typeof dead.dlq.offset, 'number')
  assert.ok(dead.dlq.at instanceof Date)
  assert.ok(Math.abs(Date.now() - dead.dlq.at.getTime()) < 300_000)
  assert.ok(dead.dlq.first instanceof Date && dead.dlq.last instanceof Date)

  // The broker's account of the failure is not left among the publisher's
  // own headers, where a consumer would read it as one.
  assert.equal(dead.headers.has('saguin-dlq-reason'), false)
})

test('an ordinary record has no dead-letter account', async () => {
  const where = site()
  const producer = await clients.producer()
  const reading = await clients.producer({ clientId: 'plain-' + where, durable: true })
  const records = await reading.append.consume('events', {
    key: [where], timeout: 10_000,
  })
  await producer.append.publish('events', { key: [where, 'thing'], value: 'x' })
  const [record] = await take(records, 1)
  assert.equal(record.dlq, null)
})

test('a dead letter can be put back on its queue', async () => {
  const where = site()
  const producer = await clients.producer()
  const sent = await producer.queue.publish('tasks', {
    key: [where, '1'], value: 'do it',
  })
  await failItOnce('tasks')

  const operator = await clients.producer({
    clientId: 'redriver-' + where, durable: true,
  })
  const [dead] = await take(
    await operator.append.consume('tasks__dlq', { key: [where], timeout: 30_000 }), 1,
  )
  assert.equal(dead.dlq.reason, 'attempts_exhausted')
  await operator.queue.redrive('tasks', dead)

  // **It is on the queue again, as attempt 1 and with its own id.** The
  // id is what makes redriving twice something a consumer can notice
  // rather than a second piece of work.
  const worker = await clients.producer({ clientId: 'again-' + where, durable: true })
  const [back] = await take(await worker.queue.fetch('tasks', { timeout: 15_000 }), 1)
  assert.equal(back.payload.toString(), 'do it')
  assert.equal(back.topic, `work/${where}/jobs/1`)
  assert.equal(back.id, sent.saguinId)
  assert.equal(back.attempt, 1)
  assert.equal(back.dlq, null, "the broker's dead-letter account travelled with it")
})

test('the __dlq level comes off where the filter puts it', async () => {
  // **The case the whole design is for.** A queue filtered `bulk/#` has
  // its dead letters at `bulk/__dlq/...`, so `__dlq` is the *second*
  // level and not the last. Stripping the last level, the obvious
  // implementation, would put the work back on the wrong topic, or on no
  // channel at all.
  const where = site()
  const producer = await clients.producer()
  assert.equal((await producer.channel('bulk__dlq')).filter, 'bulk/__dlq/#',
    'the broker puts __dlq somewhere else than this test assumes')

  await producer.queue.publish('bulk', { key: [`${where}/deep/1`], value: 'heavy' })
  await failItOnce('bulk')

  const operator = await clients.producer({
    clientId: 'bulk-redriver-' + where, durable: true,
  })
  const [dead] = await take(
    await operator.append.consume('bulk__dlq', {
      key: [`${where}/deep/1`], timeout: 30_000,
    }), 1,
  )
  assert.equal(dead.topic, `bulk/__dlq/${where}/deep/1`)
  await operator.queue.redrive('bulk', dead)

  const worker = await clients.producer({ clientId: 'bulk-after-' + where, durable: true })
  const [back] = await take(await worker.queue.fetch('bulk', { timeout: 15_000 }), 1)
  assert.equal(back.topic, `bulk/${where}/deep/1`, 'the level came off in the wrong place')
  assert.equal(back.payload.toString(), 'heavy')
})

test('redrive refuses a record that is not a dead letter', async () => {
  // A topic without `__dlq` where the filter puts it is not a record this
  // rule describes, and republishing it unchanged would put the work
  // straight back into the dead-letter channel it came from.
  const where = site()
  const producer = await clients.producer()
  const reading = await clients.producer({ clientId: 'nodrive-' + where, durable: true })
  const records = await reading.append.consume('events', {
    key: [where], timeout: 10_000,
  })
  await producer.append.publish('events', { key: [where, 'thing'], value: 'ordinary' })
  const [ordinary] = await take(records, 1)

  const refused = await rejected(reading.queue.redrive('tasks', ordinary))
  assert.match(refused.message, /__dlq/)
  assert.match(refused.message, /work\/\+\/jobs\/\+\/__dlq/)
})

test('redrive refuses a channel that is not a queue', async () => {
  // The queue is named, not the dead-letter channel, so naming anything
  // else is caught by this namespace's own check before the record is
  // even looked at.
  const where = site()
  const producer = await clients.producer()
  const reading = await clients.producer({ clientId: 'wrongchan-' + where, durable: true })
  const records = await reading.append.consume('events', {
    key: [where], timeout: 10_000,
  })
  await producer.append.publish('events', { key: [where, 'thing'], value: 'x' })
  const [record] = await take(records, 1)

  const refused = await rejected(reading.queue.redrive('events', record))
  assert.ok(refused instanceof WrongChannelType, refused.message)
  assert.match(refused.message, /queue/)
})

test('a job left in hand is said out loud', async () => {
  // It is not lost and it is not duplicated: it is held for this
  // connection, and the queue goes on looking healthy while it is one job
  // short. Silence here is somebody reading a queue's depth and finding
  // no reason for it.
  const where = site()
  const producer = await clients.producer()
  const worker = new Client('abandoner-' + where, { durable: true })
  await worker.start(running.broker.url)
  const said = []
  worker.on('warning', (one) => said.push(one))

  const jobs = await worker.queue.fetch('tasks', { timeout: 15_000 })
  await producer.queue.publish('tasks', { key: [where, '1'], value: 'do it' })
  await take(jobs, 1)

  assert.ok(said.some((one) => one.includes('neither acknowledged nor handed back')),
    'a queue job was abandoned in silence')
  for (const want of ['tasks', 'queue.nack']) {
    assert.ok(said.some((one) => one.includes(want)),
      `the warning does not say '${want}': ${said.join(' ')}`)
  }

  // **And the job does not come back while this client holds it.** Its
  // lease starts at the acknowledgement, so one that was never
  // acknowledged has no lease to expire: the broker holds it for a worker
  // that has stopped asking, and the queue's depth shows a job nothing is
  // working with no reason on the face of it. Not lost and not
  // duplicated, which is exactly why it needs saying.
  const other = await clients.producer({ clientId: 'other-' + where, durable: true })
  const offered = await take(await other.queue.fetch('tasks', { timeout: 4000 }), 1)
  assert.deepEqual(offered, [], 'an abandoned job was offered to another worker')

  // It comes back when this client disconnects, and this test clears up
  // after itself: a queue is not narrowed by a key the way a channel is,
  // so a stray job is handed to whichever test reads this queue next.
  await worker.close()
  for (const one of await take(await other.queue.fetch('tasks', { timeout: 8000 }), 1)) {
    await other.queue.ack(one)
  }
})

test('answering something that is not a job says so', async () => {
  const where = site()
  const producer = await clients.producer()
  const reading = await clients.producer({ clientId: 'notajob-' + where, durable: true })
  const records = await reading.append.consume('events', {
    key: [where], timeout: 10_000,
  })
  await producer.append.publish('events', { key: [where, 'thing'], value: 'x' })
  const [record] = await take(records, 1)

  const refused = caught(() => reading.queue.ack(record))
  assert.match(refused.message, /not a queue delivery/)
})

test('two workers share a queue and neither sees the other\'s work', async () => {
  // A queue hands each job to one worker. Both workers are proved to have
  // done some of it, because a test where one did everything would pass
  // while measuring nothing about sharing.
  const where = site()
  const producer = await clients.producer()
  const jobs = Array.from({ length: 8 }, (_, n) => String(n))
  for (const one of jobs) {
    await producer.queue.publish('retried', { key: [where, one], value: one })
  }

  const taken = new Map()
  const run = async (name) => {
    const worker = await clients.producer({ clientId: name, durable: true })
    await worker.queue.work('retried', (job) => {
      const payload = job.payload.toString()
      taken.set(payload, [...(taken.get(payload) ?? []), name])
    }, { timeout: 6000 })
  }

  const names = [`worker-a-${where}`, `worker-b-${where}`]
  await Promise.all(names.map(run))

  assert.deepEqual([...taken.keys()].sort(), [...jobs].sort(),
    `not every job was handed out: ${[...taken.keys()].sort()}`)
  const doubled = [...taken].filter(([, who]) => who.length > 1)
  assert.deepEqual(doubled, [], `a job went to more than one worker: ${doubled}`)
  const did = new Set([...taken.values()].flat())
  assert.deepEqual([...did].sort(), [...names].sort(),
    `only ${[...did]} did any work, so this measured nothing about sharing`)
})

test('a handed-back job waits longer each time', async () => {
  // `linear` backoff is base x attempt, so the gaps grow: 1s, 2s, 3s.
  //
  // Asserted as *growing* rather than as exact numbers, because a gap is
  // the broker's floor and not its promise: the queue offers on a 200ms
  // tick, so every gap is that much or more. What would be wrong is a gap
  // that did not grow, or none at all.
  const where = site()
  const producer = await clients.producer()
  await producer.queue.publish('backoff', { key: [where, '1'], value: 'never works' })

  const seen = []
  const worker = await clients.producer({ clientId: 'backoff-' + where, durable: true })
  await worker.queue.work('backoff', (job) => {
    seen.push([job.attempt, Date.now()])
    throw new Error('no')
  }, { onError: () => {}, timeout: 12_000 })

  assert.deepEqual(seen.map(([attempt]) => attempt), [1, 2, 3, 4],
    `want four attempts and no more, since max_attempts is 4; got ${JSON.stringify(seen)}`)
  const gaps = seen.slice(1).map(([, at], n) => (at - seen[n][1]) / 1000)
  assert.ok(gaps[0] >= 0.9 && gaps[1] >= 1.9 && gaps[2] >= 2.9,
    `gaps were ${gaps}, want about 1s, 2s and 3s`)
  assert.ok(gaps[0] < gaps[1] && gaps[1] < gaps[2], `the gap did not grow: ${gaps}`)

  // And with its attempts spent, the job is dead-lettered rather than
  // offered a fifth time.
  const reading = await clients.producer({
    clientId: 'backoff-dlq-' + where, durable: true,
  })
  const [dead] = await take(
    await reading.append.consume('backoff__dlq', { key: [where], timeout: 15_000 }), 1,
  )
  assert.equal(dead.payload.toString(), 'never works')
  assert.equal(dead.dlq.reason, 'attempts_exhausted')
  assert.equal(dead.dlq.attempts, 4)
})
