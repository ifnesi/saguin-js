# Contributing to saguin-js

Contributions are welcome, and the barrier is deliberately low. This
document is short because it should be.

## The most useful thing you can do today

This library is a thin layer over MQTT.js, so the highest-value
contribution is not new surface:

**Find a claim here that the broker does not keep.** The README, the
comments and the tests all say what saguin does. Run one of them against a
broker and show where the sentence and the behaviour part company. That is
a defect whichever of the two is wrong.

Also welcome: a verb that composes a topic wrongly for a filter shape
nobody tried, a refusal whose message does not say what to do instead, and
tests for a promise that has none.

## The one rule that is unusual

**Nothing here may be a rule.** The broker is the only enforcement point,
and everything this library sends is an ordinary MQTT 5 packet, so
anything it does a generic MQTT.js client can do by sending the same
packets. A change that makes this library the place a rule lives is a
change that makes a generic MQTT client a way around it.

What the library does check is its own contract: that a key fits the
filter it is being composed against. Everything else comes back from the
broker as its own reason code and sentence, raised verbatim.

## Two things that will get a change sent back

**An MQTT extension.** saguin defines no packets, properties or flags of
its own, and neither does this. If a change cannot name the standard MQTT
5 mechanism it rides on, the answer is no.

**A surface that is not in the broker's documents.** A verb here
implements something an RFC in the [broker's
repository](https://github.com/ifnesi/saguin) states. If the behaviour is
not written down there, the RFC changes first, in that repository.

## Conventions in the code

- **MQTT.js is reachable.** `client.mqtt` is the MQTT.js client and
  nothing may hide it or make its methods unsafe to call. A caller who
  needs a verb this library has not grown reaches past it, and that has to
  keep working.
- Plain ES modules, no build step, and MQTT.js as the only runtime
  dependency. A dependency an edge box has to install is a reason not to
  install the SDK.
- **Comments say why, not what.** The diff says what. A comment earns its
  place by naming the failure the line prevents.
- Refusals name what to do instead. A message that says only what was
  wrong sends the reader back to the source.

## Tests

The suite drives a **real broker**, never a mock: what is under test is
what saguin does with what a client sent, and a mock answers with what
this library believes saguin does. `README.md` has how to run it.

- Where a test is about **what went on the wire**, the oracle is plain
  MQTT.js rather than this library. A probe that asks the code under test
  what the answer should be asserts only that the code agrees with itself.
  Where the broker rewrites what it received, MQTT.js's own `packetsend`
  is what the client sent.
- **Assert the failure, not only the happy path.** Most of what saguin
  promises fails silently: a broken implementation reports success while
  losing or duplicating a record.
- **Watch a new test fail without its fix**, and say in the pull request
  what it printed. A test that cannot fail is not evidence.

## Issue or pull request?

A defect you are not fixing yourself, or anything that needs the broker's
documents to change, starts as an issue. A fix that makes this library
match what the broker already does can arrive directly as a pull request.

## Pull requests

Fork, work on a short-lived branch, and open the pull request against
`main`. One logical change each. In the commit message, say *why*: the
diff already says what.

Changes land by rebase or squash, never a merge commit, so the history
stays linear. `main` is never force-pushed.

## Writing

The documents here follow the broker's: state the failure a rule prevents
rather than only the rule, describe what is rather than what was, and put
new information into a document that already exists. Punctuation included:
this project writes a dash as ` - ` rather than an em dash, in prose,
comments and commit messages alike.

## Sign-off

Sign your commits with `git commit -s`, which adds a
[DCO](https://developercertificate.org/) line certifying you have the
right to submit the contribution. No CLA. Apache-2.0 section 5 already
places your contribution under the project's licence.

## Conduct

Be decent to each other. Assume the person on the other end is trying to
help. Disagreement about a design is normal and welcome; contempt is not.
