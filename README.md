# saguin-js

**A JavaScript client library for interacting with the
[Saguin](https://github.com/ifnesi/saguin) MQTT 5 broker** - a thin layer
over [MQTT.js](https://github.com/mqttjs/MQTT.js) that adds what is
saguin's and nothing else.

`saguin.Client` **holds** an MQTT.js client and hands it to you as
`client.mqtt`, so anything MQTT.js does it does. What this adds sits
**beside** that, grouped by the kind of channel it works on:
`client.append`, `client.latest`, `client.queue` and `client.admin`. The
table of verbs is under *Working in channels* below, in one place rather
than two.

## Install

Not on npm yet - a release waits on saguin's own. From a checkout:

```sh
npm install /path/to/saguin-js
```

Node 20 and up. The only dependency is `mqtt`. Reading a payload through
a schema needs `avsc` or `protobufjs`, which are optional peers: an edge
box that publishes bytes should not install a protobuf compiler to do it.

## Working in channels

You name a channel and the library works out the MQTT: it asks the broker
what the channel is, builds the topic from the channel's filter and the
key you gave, and puts a record id on every write.

```js
import { Client } from 'saguin'

const client = new Client('gateway-1')
await client.start('mqtt://broker.local:1883')

await client.append.publish('readings', {
  key: ['site42', 'device', 'temp/1'],
  value: '21.5',
  headers: { unit: 'C' },
})
```

Awaiting a publish resolves on the broker's acknowledgement, and a
refusal arrives there as the broker's own reason code and sentence.
Supply `{ saguinId }` to know the record's id before the broker answers.

| | |
|---|---|
| `client.append` | `.publish(channel, { key, value, headers })`, `.consume(channel, { key, start })`, `.seek(channel, to)` |
| `client.latest` | `.set(...)`, `.get(channel, { key })`, `.delete(channel, { key })`, `.consume(channel, { key })` |
| `client.queue` | `.publish(...)`, `.fetch(channel)`, `.work(channel, handler)`, `.ack(job)`, `.nack(job)`, `.redrive(channel, record)` |
| `client.admin` | `.disconnect(clientId)` |

**Grouped rather than flat, because flat was three names for one act.**
Writing to an append channel, setting a key and queueing work are one
publish underneath, told apart only by which channel the topic lands in,
so a flat surface had to invent a different verb for each. Here the
channel type is the namespace and the verb is the same word.

It also keeps `client.admin.disconnect`, which hangs up somebody else,
well away from `client.close()`, which hangs up you.

Every verb takes the channel by name and then an options object.
Anything the object
carries beyond `key`, `value` and `headers` is passed to the publish
underneath, so `{ qos: 2 }` and `{ retain: true }` reach MQTT.js.

### The key fills in the channel's filter

One value per slot. For a channel filtered `iot/+/{device,sensor}/#`:

| filter level | `iot` | `+` | `{device,sensor}` | `#` |
|---|---|---|---|---|
| key | - | `site42` | `device` | `temp/1` |
| topic | `iot` | `site42` | `device` | `temp/1` |

A `+` takes any one level, a `{a,b}` level takes one of those spellings,
and a trailing `#` takes the rest, or nothing, since `#` stands for no
levels as well as for many. **A key that does not fit is refused before
anything is sent, and says what the filter is:**

```
'gadget' is not one of 'device', 'sensor' at level '{device,sensor}'
  for channel 'readings', whose filter is 'iot/+/{device,sensor}/#'
```

Nothing here validates a topic against a filter before sending it, and
that is the point of building the topic rather than checking it: a second
matcher that disagreed with the broker's would be worse than none.

**A verb on the wrong kind of channel is refused by name**, since the
three writes are one publish underneath and this is the only thing
between appending to a log and queueing work by mistake:

```
'tasks' is a queue channel, and this verb is for an append one
  - use client.queue.publish(), .fetch(), .work(), .ack(), .nack() or .redrive()
```

### Reading, and keeping your place

Any client can read. What needs `{ durable: true }` is **keeping your
place**: coming back tomorrow and carrying on where you stopped, rather
than at whatever the channel says a reader with no position gets. `seek`
needs it too, since there is no position to move without one.

A durable client is three things together - clean start off, a session
expiry that is not zero, and the same id next time - and the broker stores
a position against all three.

```js
const reader = new Client('orders-reader', { durable: true })
await reader.start('mqtt://broker.local:1883')

for await (const record of await reader.append.consume('readings', {
  key: ['site42'],
})) {
  handle(record)
}
```

The key narrows it. A value left out is every topic that level can hold,
and **a `{a,b}` level left out becomes one subscription per alternative
rather than a `+`**, because a `+` there would also reach topics beside
the channel, whose records are not its.

**A record is acknowledged when the loop asks for the next one**, not when
it arrives. The stored position advances on the acknowledgement, so
acknowledging on arrival would let a client that died half way through a
record resume *after* it, the one thing a position must never do. On an
append or `latest` channel, leaving the loop early leaves the record in
hand unacknowledged and it comes back: the position has not advanced, so
the next read is served it again.

**On a queue it does not come back until the client disconnects.** A job's
lease starts at the acknowledgement, so a job that was never acknowledged
has no lease to expire and nothing brings it round again: it is held for a
worker that has stopped asking, and the queue is one job short with
nothing to show for it. Acknowledge it or hand it back before leaving the
loop, which is what `queue.work` does on every path. Leave the loop
holding one and the library says so, because nothing else would.

A loop that runs out closes its reader; one you leave early through
`break` closes it too. A reader you stop asking without either is still
subscribed, so close it by hand:

```js
const records = await client.queue.fetch('tasks')
const { value: job } = await records.next()
await records.close()
```

After `start`, `client.sessionPresent` says whether the broker found your
session, and `client.grantedSessionExpiry` how long your position will be
kept, which is not always what you asked for, since the broker caps it.

**When the link drops**, MQTT.js reconnects on its own and a durable
client's subscription comes back with its session, so the loop carries on.
The record you had taken but not finished is sent again, because that is
what at-least-once means.

### Where to begin, and where to jump

Both take **saguin's own vocabulary** and nothing invented: an integer -
`0` the retention floor, `-1` the next offset, or a position - or a string
holding a duration (`'12h'`, `'7d'`) or an RFC 3339 moment.

```js
await reader.append.consume('readings', { start: 0 })  // only with no position
await reader.append.seek('readings', '12h')            // always
```

**They are two verbs because they answer two questions.** `start` is where
to begin when this client has never read here, or its session expired, and
it fires **once**, because a value written into your code would otherwise
replay the whole channel on every restart. `seek` is a deliberate move:
replaying a day after a bug, or skipping a backlog.

The library knows which case it is in from `sessionPresent`. One case it
cannot see: a session that exists but has never read *this* channel. There
is no verb for "where am I here", so the channel's own `start:` setting in
the broker's configuration decides that one.

**A bare integer always means an offset**, never a Unix time.
`1763000000` is a plausible offset and a plausible time, and seeking to
the wrong one reads on in order and reports success, so a time is a string
and the value is passed through unchanged.

### Taking a slice of a channel

Several readers can split one channel between them, each taking a share of
its topics, with **no coordination and no coordinator**. A reader says
which share is its own when it subscribes.

```js
for await (const record of await reader.append.consume('readings', {
  key: ['site42'], topicHash: [8, 1],
})) {
  handle(record)
}
```

That is member 1 of 8. The broker turns each topic into a number and
delivers a record only to the member whose share the remainder matches,
and **a reader declaring nothing gets everything**, which is every reader
that has never heard of this. It applies to a replay from a stored
position as well as to live records, and on a `latest` channel to the pass
of current state as well as to the changes after it.

Pass a list of pairs for a member holding more than one share -
`topicHash: [[8, 1], [8, 5]]` - which is most often a member covering for
a peer that died. Every share in one call names the same total, because a
subscription has one partition space.

`partition(topic, count)` is the same calculation, on its own, reaching no
broker:

```js
import { partition } from 'saguin'
partition('iot/site42/device/temp/1', 8)   // the member owed it
```

Use it rather than `topicHash(topic) % 8n`. The hash is only the first
half: RFC 0003 puts a mixing step after it, and without that step a topic
scheme carrying an identifier twice - `devices/<id>/msg/<id>` - sends every
identifier to a strict subset of the members and leaves the rest with
nothing. `topicHash` is exposed because the RFC gives both halves so that
an implementation which disagrees can tell which one is wrong; it answers
a BigInt, since a 64-bit value does not fit in a number.

`partition` is here because **nothing tells you about a share nobody
claimed**. The broker cannot tell "there is no member 2" from "member 2
has not started yet", so it says nothing, and every reader that is running
looks healthy. Covering every share is the application's job by design,
and working out where your own topics fall is how it does it.

Three things worth knowing, all of them the broker's behaviour rather than
this library's:

* **A member's position moves past records outside its share**, so
  widening a share tomorrow recovers none of what it skipped yesterday.
* **A share lasts as long as the session** and survives a reconnection
  that resumes one.
* **Refused on a shared subscription and on a queue.** Both already divide
  a stream between their members; the queue is refused here, before
  anything is sent.

Broadcast takes it too, through `client.subscribe`, since a topic no
channel claims has no verb of its own:

```js
await client.subscribe('shout/#', { qos: 1, topicHash: [8, 1] })
```

That call reads the SUBACK and throws `SubscriptionRefused` on a filter
the broker refused, which MQTT.js's own `subscribe` reports as an error
without the broker's sentence. A client that does not read the codes sits
connected, subscribed to nothing, and receives nothing for ever.

### Work

```js
const worker = new Client('packer', { durable: true })
await worker.start('mqtt://broker.local:1883')

for await (const job of await worker.queue.fetch('tasks')) {
  try {
    await doTheWork(job)
    await worker.queue.ack(job)
  } catch {
    await worker.queue.nack(job)
  }
}
```

`nack` sends the broker's own word, `return`, spelled `nack` here because
ack/nack is the pair everybody knows. The attempt is spent, and a job
whose attempts run out is dead-lettered.

Or let the library do the acking:

```js
await worker.queue.work('tasks', packTheOrder)
```

A handler that returns normally has its job acked. One that throws has its
job handed back and **the worker carries on to the next**, the shape
RabbitMQ's clients and the frameworks over them have, because a worker
that stopped on one bad job would stop everything behind it. The failure
is announced on the client's `warning` event unless you pass `onError`.

A handed-back job comes straight back, so a handler that always throws
spends that job's attempts quickly. The queue's `max_attempts` bounds it
and the job is dead-lettered when they run out.

`fetch` and `work` take no key: a queue admits one subscription form and
no other, because two spellings would be two consumer groups each taking a
copy of every job.

### State

A `latest` channel keeps the current value of every key, so whoever
subscribes next is sent it: a device that was away learns the state
without anybody replaying a log at it.

```js
await client.latest.set('state', { key: ['site42', 'temp'], value: '18' })
await client.latest.get('state', { key: ['site42', 'temp'] })    // <Buffer 31 38>
await client.latest.delete('state', { key: ['site42', 'temp'] })
await client.latest.get('state', { key: ['site42', 'temp'] })    // null
```

`get` answers `null` where there is no value, which is also what a deleted
key answers, since the two have always been the same here. It does not
subscribe, so reading a value once does not enrol you in every later
change to it.

To follow the state instead of asking for it:

```js
for await (const record of await reader.latest.consume('state', {
  key: ['site42'],
})) {
  // record.isCatchUp is true for the state you arrived to
}
```

### Hanging up a client

```js
await client.admin.disconnect('device-7')   // 'hung-up', or 'no-such-client'
```

It ends the connection and leaves the session alone, so the device
reconnects and resumes at its stored position: the whole cost is one
reconnection. On its own it withdraws nothing, and what the device may do
when it returns is whatever the broker's files say then. It needs a
`broker: sessions` rule in the broker's `acl_file`.

## Schemas

A schema registry needs nothing from the broker: a `latest` channel is
already a key-value store with delete, so a registry is **a channel and a
convention**. Register a schema by setting it, like any other value:

```js
await client.latest.set('schemas', { key: ['acme/weather/v1'], value: AVRO_TEXT })
```

Then name **that topic** when you write; the library serializes for you:

```js
const client = new Client('gateway-1', { schemaRegistry: 'schemas' })

await client.append.publish('readings', {
  key: ['site42', 'device', 't'],
  value: { site: 'site42', temp: 21.5 },
  schema: 'schemas/acme/weather/v1',
})
```

`schema` works on every write - `append.publish`, `latest.set`,
`queue.publish` and plain `publish` - so it covers broadcast topics too.
It serializes the payload, sets the Content Type, and adds a `schema` User
Property carrying the schema's topic.

Reading it back:

```js
for await (const record of await reader.append.consume('readings')) {
  record.schema                 // 'schemas/acme/weather/v1'
  await record.deserialized()   // { site: 'site42', temp: 21.5 }
}
```

**Avro and protobuf**, through `avsc` and `protobufjs`, which are optional
peers: `npm install avsc` or `npm install protobufjs` where you need them.

Four things worth knowing, each of which is a decision rather than an
accident:

* **The pointer is a whole topic, not an id.** A bare `weather-v1` lets
  two publishers in different domains pick the same name, the second
  silently replacing the first, and says nothing about where to look it
  up. A topic answers both.
* **There are no versions.** The topic is the identity, so **a new version
  is a new topic**. Rewriting a schema at the same topic changes what
  records already written against it mean, and nothing can recover the old
  text once it is gone.
* **The schema must be registered before you produce.** There is no
  serializing from a local file, deliberately: a record written against a
  schema no consumer can fetch is a record nobody can read.
* **A pointer is followed only inside its own registry**, which is why the
  client is told where that is. An ACL governs who may *write* a topic and
  never who may *name* one, so a publisher could otherwise point a
  consumer at a `latest` channel holding device state and have it read
  out.

`record.deserialized()` fetches the schema, so it needs the connection the
record was read on: deserialize inside the loop, not after it.

**Broadcast reads are the one path that hands you an MQTT.js packet**,
since a topic no channel claims is ordinary MQTT. `client.record` turns
one into a saguin record:

```js
client.on('message', async (record) => {
  console.log(await record.deserialized())
})
```

**A schema is remembered for the life of the connection**, and
republishing at the same topic is not noticed. That is a limit rather than
an oversight: Avro's schemaless deserializing cannot reliably tell that it
has the wrong schema, so there is no failure to invalidate on, and an
invalidation that worked only for protobuf would be worse than none. It
costs nothing where the convention is followed - a new version is a new
topic, and a new topic is a cache miss. `client.forgetSchema()` is there
for anyone who republishes at the same one anyway.

## What a delivery carries

```js
record.id           // the record's Message ID
record.offset       // its position in its channel, a number
record.timestamp    // broker receipt time, a Date
record.channel      // which channel - only where your filter reaches two
record.attempt      // which delivery of a queue job this is
record.isCatchUp    // state you are catching up on, not a change just made
record.headers      // the publisher's own User Properties
record.dlq          // why it failed out of a queue, or null
record.schema       // the topic of the schema it was written against
await record.deserialized()   // the payload read through that schema
record.packet       // the MQTT.js packet, untouched
```

`record.headers` is a sequence rather than a plain object, and
deliberately: MQTT 5 permits a repeated name and it is the standard way to
carry a list, so `headers.all('tag')` answers every value and
`headers.get('tag')` the first. `Object.fromEntries(record.headers)` is
there for whoever knows their own names are unique.

Two names are worth reading twice. `record.timestamp` is **the broker's**
receipt time, not the moment this process parsed the packet. And
`record.channel` being `null` does not mean "no channel": saguin sends the
name only where your filter reaches more than one, because a consumer
whose filter reaches exactly one already knows and would pay 26 bytes a
message to be told.

## Dead letters

A dead-letter channel is an `append` channel in every other respect - its
name is the queue's with `__dlq` on the end - so it is read like any
other:

```js
for await (const record of await reader.append.consume('tasks__dlq')) {
  // ...
}
```

What is different is on the record:

```js
record.dlq.channel    // the queue it came from
record.dlq.reason     // attempts_exhausted, or expired
record.dlq.attempts   // how many were made
record.dlq.at         // when it was dead-lettered, a Date
```

A publisher cannot forge any of it: everything sent under the reserved
prefix is stripped, so this is the broker's own account.

### Putting failed work back

```js
for await (const record of await reader.append.consume('tasks__dlq')) {
  if (worthRetrying(record)) await worker.queue.redrive('tasks', record)
}
```

You name **the queue**, as you do everywhere else: a dead-letter channel
is the queue's own with `__dlq` on the end of its name, so there is
nothing to name twice.

This is a read and a republish done here, not a broker verb: deciding that
failed work should be tried again is a judgement nobody but you can make.
**The record keeps its own id**, so the work is the same work rather than
a second piece of it.

The `__dlq` level comes off **where the channel's filter puts it**, which
is not always the end: a queue filtered `bulk/#` has its dead letters at
`bulk/__dlq/...`. So the filter is asked for rather than assumed, and a
record whose topic does not carry `__dlq` there is refused, since
republishing its topic unchanged would put the work straight back into the
dead-letter channel it came from.

**The dead letter stays where it is.** A dead-letter channel is an
`append` channel and reading one removes nothing, so redriving twice
queues the work twice; the id is what makes that noticeable.

## Plain MQTT is still there

`client.mqtt` is the MQTT.js client, so connecting, TLS, credentials,
automatic reconnect and every option MQTT.js takes behave as they always
have. `start` passes its second argument straight through. A **broadcast**
topic, one no channel claims, is ordinary MQTT and is published to the
ordinary way:

```js
await client.publish('iot/site42/hello', 'anyone there')
```

Two defaults differ from MQTT.js's, and both are saguin's answer rather
than a preference:

* **The protocol is MQTT 5** and nothing else is accepted. 3.1.1 has no
  User Properties, no Response Topic, no shared subscriptions and no
  session expiry, which is everything this library adds. (The broker
  itself serves 3.1.1 clients; this library does not speak for them.)
* **A publish is QoS 1** unless you ask for another. Saguin answers an
  ordinary publish refusal on the PUBACK, and at QoS 0 there is no PUBACK,
  so every one of those arrives as silence. (The refusals that close the
  connection instead do so at any QoS.)

Every write carries a `saguin-id`, a UUIDv7 the broker stores as the
record's Message ID, stable across redelivery, dead-lettering and replay,
and what a consumer deduplicates on. **The library handles it**: supply
your own with `{ saguinId }` and it is used unchanged, which is what makes
a retry the same record rather than a second one; supply none and one is
minted. Either way the id that went out is on the object `publish`
resolves to.

**What no reader is reading reaches `client.on('message')`**, as a saguin
record. While a `consume` loop is reading a filter, records matching it go
to that reader and not to this event, which is what makes reading
broadcast beside a channel work. `client.mqtt.on('message')` is MQTT.js's
own and still sees every delivery, this library's answers included.

**And what reaches nobody is kept rather than dropped.** A durable client
that reconnects is served records for subscriptions made on its previous
connection, before your code has called `consume` for them; `consume`
sweeps those out when it starts. The rest wait, bounded at ten thousand,
and the library says so on the `warning` event rather than losing them
quietly.

**Nothing is acknowledged except by whoever reads it**, which is the one
rule for every delivery. A `consume` loop
acknowledges the record it handed you when you ask for the next one. What
arrives on `message` is yours to answer for:

```js
client.on('message', (record) => {
  handle(record)
  record.ack()
})
```

**A record you never acknowledge is one the broker still holds.** That is
the point of it on a durable session - an unacknowledged QoS 1 delivery
is sent again on the next connection, and for a broadcast topic that
redelivery is the only replay there is, since no position is stored
behind it. It is also the cost: acknowledge nothing for long enough and
the broker's window of records in flight to you fills, and it stops
sending. `new Client(id, { manualAck: false })` hands the job back to
MQTT.js, which answers every delivery on arrival.

**A `message` listener that throws does not take the client down.** It
runs inside MQTT.js's packet reading, where an uncaught exception would
end the process, so this library catches it and says so on the `warning`
event. The record stays unacknowledged, so it survives for the next
connection.

**Reading a channel twice on one client takes it over.** Two readers of
one filter cannot both hold it, and read, seek, read again is ordinary, so
the second wins and the first is closed, keeping whatever it was still
holding.

One thing in this library reaches past MQTT.js's public surface, and it is
worth knowing about: **the acknowledgement is sent by this library rather
than by MQTT.js.** MQTT.js sends the PUBACK from the same callback that
drives its packet pump, so holding it until your code has read the record
would stop the client reading anything at all, answers to its own
questions included. A held record answers that callback with a sentinel,
which skips MQTT.js's PUBACK, and the acknowledgement goes out when your
loop asks for the next record. The alternative was acknowledging on
arrival, which would let a client that died half way through a record
resume after it.

## Status

Early. What is here: `client.append`, `client.latest`, `client.queue`
including a callback worker and putting dead letters back,
`client.admin.disconnect`, slices, and schemas for Avro and protobuf.

**The operations listener is deliberately not here.** Its routes -
sessions, config, the ACL, the metrics - take the *operator's* credential,
and putting that in the library a fleet installs is the one shape worth
not offering: a fleet's credentials live on the fleet, where anyone
holding one device can read them. It is eight GETs with Basic auth and
JSON for anyone who needs them.

**Nothing here decides what a broker will accept.** The broker is the
enforcement point and refusals come back as its own reason code and
sentence, raised verbatim. What this library does check is its own
contract: that a key fits the filter it is being composed against, that a
schema pointer stays inside its registry, and that a slice declaration
cannot be right. None is authorization.

## A guided tour

`examples/demo.js` starts a broker of its own from `examples/saguin.yaml`
and walks through every verb here, then goes out of its way to break
things, because what a developer needs is not only the shape of the
working call but what comes back when the call is wrong.

```sh
SAGUIN_BROKER=/path/to/saguin node examples/demo.js
```

Nothing in it is set up behind your back: the channels are the ones in
that file, every record it shows is announced before it is shown, and the
suite runs the whole tour so that it cannot rot into a document about a
library that has moved on.

## Testing

The suite drives a **real broker** rather than a mock: what is being
tested is what saguin does with what a client sent, and a mock would
answer with what this library believes saguin does. Until saguin publishes
a release binary, point the suite at one you built (`make build` in a
saguin checkout writes `./bin/saguin`):

```sh
make install
SAGUIN_BROKER=/path/to/saguin make test
```

The suite writes its own configuration and starts the broker on a free
port, one per test file, since `node --test` runs each file in its own
process. Where a test is about **what the broker put on the wire**, it
reads the record back with plain MQTT.js rather than with this library: a
probe that asks the code under test what the answer should be asserts only
that the code agrees with itself. Where the broker rewrites what it
received, MQTT.js's own `packetsend` is the oracle for what the client
sent.

## Contributing

Contributions are welcome. The most useful one is finding a claim here
that the broker does not keep. [CONTRIBUTING.md](CONTRIBUTING.md) has the
rules, and there are only two unusual ones: nothing in this library may
be a rule the broker does not enforce, and a new test is watched failing
before it is trusted.

## Development approach

**saguin-js is written by Claude.**

## Licence

[Apache-2.0](LICENSE).

Copyright 2026 Italo F L Nesi.
