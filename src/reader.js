/**
 * A reader: what a `consume` or a `fetch` hands back.
 *
 * It is an async iterable, and it holds one rule: **a
 * record is acknowledged when the next one is asked for, never when it
 * arrives.** The stored position advances on the acknowledgement, so
 * acknowledging on arrival would let a client that died half way through
 * a record resume *after* it, which is the one thing a position must
 * never do.
 *
 *     for await (const record of await client.append.consume('events')) {
 *       handle(record)
 *     }
 *
 * **On an append or `latest` channel, leaving the loop early leaves the
 * record in hand unacknowledged, and it comes back**: the position has
 * not advanced, so the next read is served it again.
 *
 * **On a queue it does not come back, not until this client
 * disconnects.** A job's lease starts at the acknowledgement, so a job
 * that was never acknowledged has no lease to expire and nothing brings
 * it round again; it is held for a worker that has stopped asking, and
 * the queue quietly stops draining by one job. Acknowledge it or hand it
 * back before leaving the loop, which is what `queue.work` does on every
 * path.
 */

import { inside } from './channels.js'
import { Message } from './message.js'

export class Reader {
  /** Arrivals waiting to be asked for: `{ generation, packet }`. */
  #arrived = []
  /** Whoever is waiting for the next one. */
  #waiting = []
  #closed = false
  #holding = null
  #arrivedOn = null

  constructor(client, { arriving, filters, timeout, queueName = null }) {
    this.client = client
    /** The filters records arrive on. On a queue this is the channel's
     * own filter, which is not what was subscribed to. */
    this.arriving = arriving
    /** What was subscribed to: a queue's pin, or the channel's filters. */
    this.filters = filters
    /** How long to wait for a record before the loop ends. */
    this.timeout = timeout
    /** The queue this is reading, or null. Only a queue has a job that
     * can be left unanswered. */
    this.queueName = queueName
    this.closed = false
  }

  /** Whether a delivery on this topic belongs here. */
  wants(topic) {
    return !this.#closed && this.arriving.some((filter) => inside(filter, topic))
  }

  /** Called by the client when a delivery is routed here. */
  push(held) {
    if (this.#closed) return false
    const waiting = this.#waiting.shift()
    if (waiting) waiting(held)
    else this.#arrived.push(held)
    return true
  }

  /** Everything this reader is still holding, for the client to put back
   * where it came from. */
  takeBack() {
    return this.#arrived.splice(0)
  }

  [Symbol.asyncIterator]() {
    return this
  }

  async next() {
    // **The record in hand is acknowledged here, not when it arrived.**
    this.#acknowledgeHeld()
    if (this.#closed) return { value: undefined, done: true }

    const held = await this.#nextArrival()
    if (held === undefined) {
      // **Nothing came within the timeout, and the reader closes itself.**
      // A loop that runs out is finished with its subscription, and
      // `for await` does not call `return()` when an iterator says it is
      // done, so nothing else would. On a queue that matters more than it
      // looks: a reader left subscribed stays in the consumer group and
      // goes on being handed work nobody is reading.
      await this.close()
      return { value: undefined, done: true }
    }
    this.#holding = new Message(held.packet, this.client, held.generation)
    this.#arrivedOn = held.generation
    return { value: this.#holding, done: false }
  }

  /** What `for await` calls when the loop is left early, and what
   * `close()` is. */
  async return() {
    await this.close()
    return { value: undefined, done: true }
  }

  async throw(err) {
    await this.close()
    throw err
  }

  /** Stop reading: stop being routed these filters, stop being sent them,
   * and hand back whatever had arrived and was never read. */
  async close() {
    if (this.#closed) return
    this.#closed = true
    this.closed = true
    // **A queue job in hand is said out loud**, because nothing else
    // would ever say it. It is not lost and it is not duplicated: it is
    // held for this connection, and the queue goes on looking healthy
    // while it is one job short. Silence here is somebody reading a
    // queue's depth and finding no reason for it.
    if (this.#holding && this.queueName && !this.#holding.answered) {
      this.client.warn(
        `client '${this.client.name}' left the reading loop for queue ` +
          `'${this.queueName}' holding a job that was neither acknowledged nor ` +
          'handed back. It stays with this client until it disconnects and no ' +
          'other worker is offered it, so the queue is one job short with ' +
          'nothing to show for it. Call queue.ack(record) or queue.nack(record) ' +
          'before leaving the loop.',
      )
    }
    for (const waiting of this.#waiting.splice(0)) waiting(undefined)
    await this.client._closeReader(this)
  }

  #acknowledgeHeld() {
    if (this.#holding === null) return
    // **Never acknowledged across a reconnection.** The packet identifier
    // belongs to the connection that delivered the record, and the next
    // connection numbers its own from 1, so this acknowledgement would
    // land on whatever that one is holding under the same number, which
    // is a record acknowledged away from a consumer that never saw it.
    // The record itself is not lost: unacknowledged on a session that
    // survived, the broker sends it again.
    this.client._acknowledge(this.#holding.packet, this.#arrivedOn)
    this.#holding = null
    this.#arrivedOn = null
  }

  #nextArrival() {
    const held = this.#arrived.shift()
    if (held !== undefined) return Promise.resolve(held)
    return new Promise((resolve) => {
      const timer = this.timeout === undefined || this.timeout === null
        ? null
        : setTimeout(() => {
          const at = this.#waiting.indexOf(take)
          if (at >= 0) this.#waiting.splice(at, 1)
          resolve(undefined)
        }, this.timeout)
      const take = (one) => {
        if (timer) clearTimeout(timer)
        resolve(one)
      }
      this.#waiting.push(take)
    })
  }
}
