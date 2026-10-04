/**
 * The guided tour is run, not just shipped.
 *
 * A demo nobody runs rots into a document about a library that has moved
 * on, and it is among the first things a developer reads. So the suite
 * runs the whole of it against a real broker and checks it reached the
 * end, and that each section actually happened rather than the tour
 * quietly skipping one.
 *
 * The tour starts a broker of its own, so this file needs none.
 */

import assert from 'node:assert/strict'
import { execFile } from 'node:child_process'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { promisify } from 'node:util'
import { test } from 'node:test'

const run = promisify(execFile)
const HERE = dirname(fileURLToPath(import.meta.url))
const DEMO = join(HERE, '..', 'examples', 'demo.js')

test('the guided tour runs to the end', async () => {
  let done
  try {
    done = await run(process.execPath, [DEMO], {
      timeout: 300_000, maxBuffer: 10 * 1024 * 1024,
    })
  } catch (failed) {
    assert.fail(`the tour exited ${failed.code}:\n` +
      `${(failed.stdout ?? '').slice(-3000)}\n${(failed.stderr ?? '').slice(-2000)}`)
  }
  const plain = done.stdout.replace(/\u001b\[[0-9;]*m/g, '')

  // **Counted, not spot-checked.** A tour that stopped half way through
  // still prints a lot, and a check for one line would pass over the
  // rest.
  const sections = [...plain.matchAll(/^== (\d+)\./gm)].map(([, n]) => n)
  assert.deepEqual(sections, Array.from({ length: 12 }, (_, n) => String(n)),
    `the tour ran sections ${sections.join(', ')} - it should run 0 to 11 in order`)
  assert.match(plain, /^== Done/m)

  // The things a reader is there for, each from a different section.
  for (const wanted of [
    'iot/+/{device,sensor}/#',       // the filter it composes against
    'attempts_exhausted',            // a dead letter saying why
    'back on the queue',             // and being put back
    'application/avro',              // a schema chosen and named
    'application/x-protobuf',        // and the other format
    'latest, deserialized',          // and used on every channel type
    'queue, deserialized',
    'append, deserialized',
    'hung-up',                       // an operator's verb
    'no-such-client',                // and the answer that is not it
    'KeyDoesNotFit',                 // and what comes back when it is wrong
    'WrongChannelType',
    'UnknownChannel',
    'SchemaError',
    'RequestRefused',
  ]) {
    assert.ok(plain.includes(wanted), `the tour never showed '${wanted}'`)
  }

  // **The split, read out of the tour's numbers rather than its prose.**
  // A record delivered to two members and a record delivered to none both
  // leave every member looking healthy, so the tour has to account for
  // every record it published.
  const counted = plain.match(/^ {3}records in all\s+(\d+) of (\d+)/m)
  assert.ok(counted, 'the tour did not account for the records it split')
  assert.equal(counted[1], counted[2],
    `${counted[1]} records reached the members and ${counted[2]} were published`)

  // One dead letter, put back once, with the id it had: a tour that
  // redrove nothing would print the section and pass the checks above.
  const back = plain.match(/^ {3}back on the queue\s+(.+)$/m)
  assert.ok(back, 'nothing was put back on the queue')
  assert.match(back[1], /attempt 1$/, 'the work came back as a later attempt')
})
