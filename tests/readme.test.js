/**
 * The README names verbs; this checks the library still has them.
 *
 * A claim about behaviour is a thing to run, not a thing to write once: a
 * sentence the code has stopped agreeing with is believed by everyone who
 * reads it next, and no test catches it, because it is not code. The
 * README is the first thing anybody reads, so the names in it are checked
 * here.
 *
 * It counts what it read and refuses a suspiciously small number, because
 * a pattern that quietly stopped matching passes by comparing nothing.
 */

import assert from 'node:assert/strict'
import { readFileSync, readdirSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { test } from 'node:test'

import { Client } from '../src/index.js'
import { VERBS_OF } from '../src/client.js'

const HERE = dirname(fileURLToPath(import.meta.url))
const README = join(HERE, '..', 'README.md')

// `client.append.publish(`, `reader.queue.ack(`, and so on: an object, a
// namespace, a verb. The object's name varies through the document on
// purpose - client, reader, worker - so it is not anchored to one.
const CALLS = /\b\w+\.(append|latest|queue|admin)\.(\w+)\(/g

test('every verb the README names exists', () => {
  const text = readFileSync(README, 'utf8')
  const named = [...new Set([...text.matchAll(CALLS)].map(([, group, verb]) => `${group}.${verb}`))]
  assert.ok(named.length >= 10,
    `only found ${named.length} verbs in the README, which cannot be all of ` +
      'them - the pattern has stopped matching')

  // Resolved the way a reader would reach them.
  const made = new Client('readme-check')
  for (const one of named) {
    const [group, verb] = one.split('.')
    assert.equal(typeof made[group][verb], 'function',
      `the README calls client.${one}(), which this library does not have`)
  }
})

test('the advice in a refusal names verbs that exist', () => {
  // A wrong-channel refusal tells the caller which verbs to use instead,
  // so a verb renamed without it becomes advice to call something that is
  // not there, which is worse than no advice.
  //
  // **It counts before it compares.** A pattern that stopped matching
  // passes by comparing nothing.
  const made = new Client('verbs-check')
  const named = /\.?(\w+)\(/g
  let checked = 0
  for (const [kind, advice] of Object.entries(VERBS_OF)) {
    const found = [...advice.matchAll(named)].map(([, verb]) => verb)
    assert.ok(found.length >= 2,
      `read only ${found.length} verbs out of the ${kind} advice '${advice}' - ` +
        'the pattern has stopped matching')
    for (const verb of found) {
      checked += 1
      assert.equal(typeof made[kind][verb], 'function',
        `a ${kind} channel refusal tells the caller to use ${verb}(), which ` +
          `client.${kind} does not have`)
    }
  }
  assert.ok(checked >= 10, `checked only ${checked} verbs in all`)
})

test('every name the README shows on a record is on a record', () => {
  // The delivery table is the page a consumer writes its loop from, and a
  // field renamed out from under it reads as a library that lost a
  // feature.
  const text = readFileSync(README, 'utf8')
  const table = text.slice(text.indexOf('## What a delivery carries'))
  const shown = [...new Set(
    [...table.matchAll(/^record\.(\w+)/gm)].map(([, name]) => name),
  )]
  assert.ok(shown.length >= 8,
    `only found ${shown.length} record fields in the README - the pattern has ` +
      'stopped matching')

  const record = new Client('record-check').record({
    cmd: 'publish', topic: 'a/b', payload: Buffer.from(''), qos: 1, properties: {},
  })
  for (const name of shown) {
    assert.ok(name in record || name in Object.getPrototypeOf(record),
      `the README shows record.${name}, which a record does not have`)
  }
})

test('this library describes itself on its own terms', () => {
  // This SDK stands alone: its documents, comments and tests describe
  // what it does, never by comparison with another client library. A
  // mention that creeps back in points its readers at a project they
  // were never meant to need. The pattern is assembled from parts so
  // that this file can be swept along with the rest.
  const other = new RegExp(
    ['saguin-py' + 'thon', '\\bpa' + 'ho\\b', '\\bPy' + 'thon\\b'].join('|'), 'gi')
  const swept = ['README.md', 'CONTRIBUTING.md']
  for (const dir of ['src', 'tests', 'examples']) {
    for (const file of readdirSync(join(HERE, '..', dir))) {
      if (/\.(js|mjs|md|yaml)$/.test(file)) swept.push(join(dir, file))
    }
  }
  assert.ok(swept.length >= 15,
    `swept only ${swept.length} files, which cannot be the whole repository`)
  for (const name of swept) {
    const found = readFileSync(join(HERE, '..', name), 'utf8').match(other)
    assert.equal(found, null,
      `${name} mentions another client library (${[...new Set(found ?? [])]}), ` +
        'and this SDK describes itself on its own terms')
  }
})
