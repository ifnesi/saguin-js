/**
 * Building a topic from a channel's filter and a key, with no broker in it.
 *
 * What saguin promises, and what these measure against: a channel's
 * filter comes back from the broker **as written**, and every level of it
 * is either spelled out or a slot the caller fills. A `+` takes any one
 * level, a `{a,b}` level takes one of those spellings, and a trailing `#`
 * takes the rest or nothing.
 */

import assert from 'node:assert/strict'
import { test } from 'node:test'

import { ChannelInfo, KeyDoesNotFit, compose } from '../src/index.js'
import { caught } from './fixtures.js'

function aChannel({
  filter = 'iot/+/{device,sensor}/#',
  name = 'readings',
  type = 'append',
  pin = null,
} = {}) {
  return new ChannelInfo({ name, type, filter, verbs: ['write', 'read'], pin })
}

test('a topic is built from the filter and the key', () => {
  const info = aChannel()
  assert.equal(compose(info, ['site42', 'device', 'temp/1']), 'iot/site42/device/temp/1')
  // `#` stands for no levels as well as for many, so its value may be left out.
  assert.equal(compose(info, ['site42', 'sensor']), 'iot/site42/sensor')
  assert.equal(compose(aChannel({ filter: 'iot/hq/door' }), []), 'iot/hq/door')
  // One value is spelled without an array often enough to be worth taking.
  assert.equal(compose(aChannel({ filter: 'iot/+/door' }), 'site42'), 'iot/site42/door')
})

test('a key that does not fit says what the filter is', () => {
  const info = aChannel()

  const refused = caught(() => compose(info, ['site42', 'gadget', 'x']))
  assert.ok(refused instanceof KeyDoesNotFit, refused.message)
  assert.match(refused.message, /iot\/\+\/\{device,sensor\}\/#/)
  assert.match(refused.message, /'device', 'sensor'/)
  assert.equal(refused.filter, 'iot/+/{device,sensor}/#')
  assert.equal(refused.channel, 'readings')

  const slashed = caught(() => compose(info, ['site42/west', 'device', 'x']))
  assert.match(slashed.message, /iot\/\+\/\{device,sensor\}\/#/)

  const short = caught(() => compose(info, ['site42']))
  assert.match(short.message, /iot\/\+\/\{device,sensor\}\/#/)

  const wild = caught(() => compose(info, ['site42', 'device', 'a/+/b']))
  assert.match(wild.message, /wildcard/)
})

test('a channel carries what the broker answered about it', () => {
  const info = ChannelInfo.fromJSON({
    name: 'tasks',
    type: 'queue',
    filter: 'work/+/jobs/+',
    verbs: ['write', 'read'],
    pin: '$saguin/queue/tasks',
  })
  assert.equal(info.name, 'tasks')
  assert.equal(info.type, 'queue')
  assert.equal(info.pin, '$saguin/queue/tasks')
  assert.deepEqual([...info.verbs], ['write', 'read'])
  // A channel that is not a queue has no pin, and the absence is a null
  // rather than a missing property: a caller tests it.
  assert.equal(ChannelInfo.fromJSON({ name: 'e', type: 'append', filter: 'a/#' }).pin, null)
})
