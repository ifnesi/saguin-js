/**
 * The verbs, grouped by the kind of channel they belong to.
 *
 * `client.append.publish(...)`, `client.latest.get(...)`,
 * `client.queue.publish(...)`, `client.admin.disconnect(...)`.
 *
 * **Grouped rather than flat, because the flat version was three names
 * for one act.** Writing to an append channel, setting a key and queueing
 * work are one publish underneath, told apart only by which channel the
 * topic lands in, so a flat surface had to invent a different verb for
 * each and a reader had to know which invented word went with which
 * channel type. Here the channel type is the namespace and the verb is
 * the same word.
 *
 * It also keeps `client.admin.disconnect`, which hangs up somebody else,
 * away from MQTT.js's own `end`, which hangs up you.
 *
 * Each verb takes the channel by name and then an options object:
 * `key`, `value` and `headers` are read here, and anything else is
 * passed to the publish underneath.
 */

/** saguin's own, and not configurable: a queue's dead letters are the
 * channel of the same name with this on the end, and the level appears in
 * that channel's filter where the queue's filter had its `#`, or last. */
const DLQ_LEVEL = '__dlq'
const DLQ_SUFFIX = '__dlq'

/** A group of verbs: the client they run on, and the channel type they
 * work on.
 *
 * `_client` rather than a private field, because JavaScript has no
 * protected and the groups below are subclasses.
 */
class Verbs {
  constructor(client) {
    this._client = client
  }

  async _channel(name, timeout) {
    const info = await this._client.channel(name, { timeout })
    this._client._mustBe(info, this.constructor.kind)
    return info
  }
}

/** A durable, replayable stream of records. */
export class Append extends Verbs {
  static kind = 'append'

  /** Add a record. Consumers keep their own positions, so nothing another
   * reader has seen is consumed away from anybody else. */
  publish(channel, options = {}) {
    return this._client._write(Append.kind, channel, options)
  }

  /** Read the channel from where this client left off, answering a reader
   * to iterate.
   *
   *     for await (const record of await client.append.consume('events')) {
   *       handle(record)
   *     }
   *
   * `start` says where to begin **when this client has no position yet**:
   * it has never read here, or its session expired. It is the broker's
   * own vocabulary: an integer offset (`0` the retention floor, `-1` the
   * next offset, or a position), or a string holding a duration such as
   * `'12h'` or `'7d'`, or an RFC 3339 moment.
   *
   * It applies once rather than on every call, which is the whole reason
   * it is not a seek: a value written once in the code would otherwise
   * replay the channel on every restart. To move a position deliberately,
   * `seek`.
   *
   * `topicHash: [partitions, index]` takes a slice of what this reaches,
   * so that several members can split a channel between them with no
   * coordination and per-topic order kept inside each. It applies to the
   * replay from a stored position as well as to live records, and a
   * member's position moves past the records outside its slice, so
   * widening the slice later recovers none of them.
   */
  consume(channel, options = {}) {
    return this._client._consume(Append.kind, channel, options)
  }

  /** Move this client's position, and answer the offset it landed on.
   *
   * Takes the same values as `consume`'s `start` and applies them
   * **always**: replaying a day after a bug, or skipping a backlog, is a
   * deliberate act rather than a default.
   */
  seek(channel, to, options = {}) {
    return this._client._seek(channel, to, options)
  }
}

/** Current value per topic: a key-value store that survives restart.
 *
 * saguin calls this a `latest` channel, which is the word in the
 * configuration file and in the documents, so it is the word here.
 */
export class Latest extends Verbs {
  static kind = 'latest'

  /** Set the value of one key. Whoever subscribes next is sent it, so a
   * device that was away learns the state. */
  set(channel, options = {}) {
    return this._client._write(Latest.kind, channel, options)
  }

  /** Read one key's current value, or **null where there is none**.
   *
   * A key never set and one that was deleted are the same answer, as they
   * are everywhere else on this channel type. It does not subscribe:
   * reading a value once does not enrol you in every later change to it.
   */
  async get(channel, { key = [], timeout } = {}) {
    const info = await this._channel(channel, timeout)
    return this._client._pointRead(info, key, timeout)
  }

  /** Remove one key.
   *
   * It is a write with nothing in it, which is how MQTT already says
   * "gone", and it has a name here because "publish an empty payload to
   * delete it" is exactly the lore this library exists to remove.
   */
  delete(channel, options = {}) {
    return this._client._write(Latest.kind, channel, { ...options, value: '' })
  }

  /** Yield the current value of everything this reaches, then every
   * change. `record.isCatchUp` tells the two apart.
   *
   * `topicHash: [partitions, index]` takes a slice, and takes it on
   * **both** halves: a member sent the whole of current state and then
   * only its share of the changes would hold a copy that starts complete
   * and drifts. */
  consume(channel, options = {}) {
    return this._client._consume(Latest.kind, channel, options)
  }
}

/** Work handed to one worker at a time. */
export class Queue extends Verbs {
  static kind = 'queue'

  /** Put work on the queue, for one worker to take. */
  publish(channel, options = {}) {
    return this._client._write(Queue.kind, channel, options)
  }

  /** Take work, answering a reader that yields one job at a time.
   *
   * Not narrowed by a key: a queue admits one subscription form and no
   * other, because two spellings would be two consumer groups each taking
   * its own copy of every job.
   */
  fetch(channel, options = {}) {
    if (options.key !== undefined) {
      throw new TypeError(
        `a queue cannot be narrowed: '${channel}' admits one subscription ` +
          'form and no other, because two spellings would be two consumer ' +
          'groups each taking its own copy of every job',
      )
    }
    return this._client._consume(Queue.kind, channel, options)
  }

  /** Take work and hand each job to `handler`, until nothing comes.
   *
   *     await worker.queue.work('tasks', packTheOrder)
   *
   * A handler that returns normally has its job **acked**: resolved, and
   * nobody else will see it. A handler that throws has its job **nacked**,
   * handed back to be offered again now, and the worker **carries on to
   * the next job**.
   *
   * That is the shape RabbitMQ's clients and the frameworks over them
   * have: log the failure and keep going. A worker that stopped on one
   * bad job would stop processing everything behind it, which is a worse
   * outcome than the job that failed.
   *
   * **The failure is said out loud rather than swallowed.** A queue that
   * fails every job with nothing anywhere saying so is the thing this is
   * written to avoid. Pass `onError(job, error)` to do something else
   * with it instead.
   *
   * **A handed-back job comes straight back**, to this worker, since it
   * is the one asking, so a handler that always throws for the same job
   * will spend its attempts in quick succession. That is not a runaway to
   * guard against here: the queue's `max_attempts` bounds it and the job
   * is dead-lettered when they run out.
   *
   * Answers the number of jobs handled, offers rather than distinct jobs,
   * since one job handed back and taken again is two.
   */
  async work(channel, handler, { onError, ...options } = {}) {
    let done = 0
    for await (const job of await this.fetch(channel, options)) {
      try {
        await handler(job)
        await this.ack(job)
      } catch (failed) {
        await this.nack(job)
        if (onError) onError(job, failed)
        else {
          this._client.warn(
            `handing back a job from '${channel}' that threw: ${failed?.message ?? failed}`,
          )
        }
      }
      done += 1
    }
    return done
  }

  /** Put one dead-lettered record back on its queue.
   *
   *     for await (const record of await reader.append.consume('tasks__dlq')) {
   *       if (worthRetrying(record)) await worker.queue.redrive('tasks', record)
   *     }
   *
   * `channel` is **the queue**, which is what you name everywhere else: a
   * dead-letter channel is the queue's own, with `__dlq` on the end of
   * its name, so there is nothing to name twice.
   *
   * **Not a broker verb**: it is a read and a republish done here, because
   * deciding that failed work should be tried again is a judgement nobody
   * but the operator can make.
   *
   * The `__dlq` level comes off **where the dead-letter channel's own
   * filter puts it**, which is not always the end: a queue filtered
   * `bulk/#` has its dead letters at `bulk/__dlq/...`. So the filter is
   * asked for rather than assumed, and a record whose topic does not
   * carry `__dlq` there is refused, since republishing its topic
   * unchanged would put the work straight back into the dead-letter
   * channel it came from.
   *
   * **The record's own id travels with it**, so the work keeps the
   * identity a consumer deduplicates on rather than becoming a second
   * piece of work. Everything else the publisher sent goes unchanged, and
   * the broker's own dead-letter account does not: a client may not write
   * under the reserved prefix.
   *
   * **The dead letter stays where it is.** A dead-letter channel is an
   * `append` channel and reading one removes nothing, so redriving twice
   * queues the work twice, and the id is what makes that something a
   * consumer can notice.
   */
  async redrive(channel, record, { timeout } = {}) {
    // The queue first, through this namespace's own check: naming
    // anything else here is the mistake worth catching, and it is caught
    // before the record is looked at.
    await this._channel(channel, timeout)

    const deadLetters = await this._client.channel(channel + DLQ_SUFFIX, { timeout })
    const levels = deadLetters.filter.split('/')
    const at = levels.indexOf(DLQ_LEVEL)
    if (at < 0) {
      throw new Error(
        `'${deadLetters.filter}' holds no '${DLQ_LEVEL}' level, so this broker ` +
          `does not put '${channel}''s dead letters where this expects`,
      )
    }

    const topic = record.topic.split('/')
    if (topic.length <= at || topic[at] !== DLQ_LEVEL) {
      throw new Error(
        `'${record.topic}' does not carry '${DLQ_LEVEL}' at level ${at + 1} ` +
          `where '${deadLetters.filter}' puts it, so it is not one of ` +
          `'${channel}''s dead letters`,
      )
    }

    return this._client.publish(
      [...topic.slice(0, at), ...topic.slice(at + 1)].join('/'),
      record.payload,
      { headers: record.headers.pairs, saguinId: record.id },
    )
  }

  /** The work succeeded: resolve the record.
   *
   * Answered on the connection the job arrived on, because the broker
   * takes a job's answer only from the session holding it.
   */
  ack(job) {
    return this._client._answerJob(job, 'ack')
  }

  /** The work failed: hand it back to be offered again now.
   *
   * Sends the broker's own word, `return`, spelled `nack` here because
   * ack/nack is the pair everybody already knows. The attempt is spent,
   * and a job whose attempts run out is dead-lettered with `record.dlq`
   * saying why.
   */
  nack(job) {
    return this._client._answerJob(job, 'return')
  }
}

/** What an ordinary MQTT connection may ask the broker to do.
 *
 * Not the operations listener: sessions, config, the ACL and the metrics
 * are a different door with the **operator's** credential, and that stays
 * a separate object, because an operator's credential must not be a
 * device's.
 */
export class Admin extends Verbs {
  /** Hang up a connected client, by id.
   *
   * It ends the connection and leaves the session alone, so the device
   * reconnects and resumes at its stored position: the whole cost is one
   * reconnection. On its own it withdraws nothing, and what the device
   * may do when it returns is whatever the broker's files say then.
   *
   * Answers `"hung-up"` or `"no-such-client"`, and the two are worth
   * keeping apart: otherwise "that device went away an hour ago" and "you
   * have misspelled the id" read the same.
   */
  disconnect(clientId, { timeout } = {}) {
    return this._client._disconnectClient(clientId, timeout)
  }
}
