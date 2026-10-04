/**
 * Turning a channel's filter into the topic to publish on.
 *
 * A saguin channel is a name and an MQTT topic filter, and the filter is
 * the one thing a client cannot work out for itself: it lives in the
 * operator's configuration. Once the broker has told you the filter, a
 * topic in that channel is not something to validate but something to
 * **build**: every level of the filter is either spelled out, or a slot
 * you fill.
 *
 * That is why nothing here validates a topic against a filter before
 * sending it. A composed topic matches the filter because it was made
 * from it, so there is nothing to check afterwards and nothing that could
 * disagree with the broker about what matches what. `inside` at the
 * bottom does match, and says at its own site why it has to.
 *
 * Four kinds of level, and only the last three take an argument:
 *
 *     iot                 spelled out, nothing to supply
 *     +                   any one level, so no "/" in what you give it
 *     {device,sensor}     one of these exactly
 *     #                   the rest, or nothing; always last if present
 */

/** What the broker says a channel is.
 *
 * `filter` is as the operator wrote it, braces included, because that is
 * what says which levels take an argument and what each will accept.
 */
export class ChannelInfo {
  constructor({ name, type, filter, verbs = [], pin = null }) {
    this.name = name
    /** `append`, `latest` or `queue`. */
    this.type = type
    this.filter = filter
    /** What this client may do here, as the broker answered when asked. A
     * permission is still checked when it is used: an operator may
     * withdraw one while you are connected. */
    this.verbs = Object.freeze([...verbs])
    /** A queue's subscription form. Only a queue has one, and it is the
     * only form a queue admits. */
    this.pin = pin ?? null
    Object.freeze(this)
  }

  static fromJSON(data) {
    return new ChannelInfo({
      name: data.name,
      type: data.type,
      filter: data.filter,
      verbs: data.verbs ?? [],
      pin: data.pin ?? null,
    })
  }

  /** The levels of this filter that take an argument, in order.
   *
   * Each is `null` for a `+`, an array of the permitted spellings for a
   * braced level, or the string `"#"` for a trailing `#`.
   */
  slots() {
    const found = []
    for (const level of this.filter.split('/')) {
      if (level === '+') found.push(null)
      else if (level === '#') found.push('#')
      else if (isBraced(level)) found.push(alternatives(level))
    }
    return found
  }
}

/** The key given does not fit the channel's filter.
 *
 * **Every one of these names the filter**, because the person reading it
 * is writing a client against a channel somebody else configured, and the
 * filter is the whole of what they need to know to fix it.
 */
export class KeyDoesNotFit extends Error {
  constructor(info, said) {
    super(`${said} for channel '${info.name}', whose filter is '${info.filter}'`)
    this.name = 'KeyDoesNotFit'
    this.channel = info.name
    this.filter = info.filter
  }
}

/** The topic to publish on, built from the filter and the key.
 *
 * `key` supplies one value per argument-taking level, in order. A
 * trailing `#` may be left out, since `#` stands for no levels as well as
 * for many.
 */
export function compose(info, key = []) {
  const given = asList(info, key)
  const slots = info.slots()
  const trailingHash = slots.length > 0 && slots[slots.length - 1] === '#'

  const least = trailingHash ? slots.length - 1 : slots.length
  if (given.length < least || given.length > slots.length) {
    throw new KeyDoesNotFit(
      info,
      `the key has ${given.length} ${given.length === 1 ? 'value' : 'values'} ` +
        `and the filter takes ${expected(slots)}`,
    )
  }

  const out = []
  const left = [...given]
  for (const level of info.filter.split('/')) {
    if (level === '+') {
      out.push(oneLevel(info, left.shift(), level))
    } else if (level === '#') {
      if (left.length) {
        const rest = noWildcards(info, left.shift(), level)
        if (rest) out.push(rest)
      }
    } else if (isBraced(level)) {
      out.push(alternative(info, left.shift(), level))
    } else {
      out.push(level)
    }
  }
  return out.join('/')
}

/** The topic filters to subscribe to, narrowed by whatever key was given.
 *
 * A value left out is every topic that level can hold: `+` for a `+`
 * level and `#` for a trailing `#`. **A braced level left out becomes one
 * filter per alternative rather than a `+`**: a `+` there would also
 * reach topics beside the channel, which this channel does not claim and
 * whose records are not its.
 */
export function subscriptions(info, key = []) {
  const given = asList(info, key)
  const slots = info.slots()
  if (given.length > slots.length) {
    throw new KeyDoesNotFit(
      info,
      `the key has ${given.length} values and the filter takes ${expected(slots)}`,
    )
  }

  let filters = ['']
  const left = [...given]
  for (const level of info.filter.split('/')) {
    let chosen
    if (level === '+') {
      chosen = left.length ? [oneLevel(info, left.shift(), level)] : ['+']
    } else if (level === '#') {
      chosen = left.length ? [noWildcards(info, left.shift(), level) || '#'] : ['#']
    } else if (isBraced(level)) {
      chosen = left.length
        ? [alternative(info, left.shift(), level)]
        : alternatives(level)
    } else {
      chosen = [level]
    }
    filters = filters.flatMap(
      (prefix) => chosen.map((one) => (prefix ? `${prefix}/${one}` : one)),
    )
  }
  return filters
}

/** The plain filters a written one stands for, resolving `{a,b}` levels.
 *
 * The broker does this before it matches, so a check made here against
 * the written form would answer about a filter nothing uses.
 */
export function expand(filter) {
  let out = ['']
  for (const level of filter.split('/')) {
    const chosen = isBraced(level) ? alternatives(level) : [level]
    out = out.flatMap((prefix) => chosen.map((one) => (prefix ? `${prefix}/${one}` : one)))
  }
  return out
}

/** Whether a topic lands inside a filter: ordinary MQTT matching.
 *
 * **The one place this library matches rather than composes.** It is here
 * because the reading loop has to know which reader a delivery belongs
 * to, and because a consumer must not follow a schema pointer out of its
 * registry: an ACL governs who may *write* a topic and never who may
 * *name* one.
 *
 * Nothing competes with the broker here. Routing a delivery is this
 * library's own bookkeeping, and the schema check fails safe: a pointer
 * is refused rather than data misrouted.
 */
export function inside(filter, topic) {
  const parts = topic.split('/')
  for (const one of expand(filter)) {
    const levels = one.split('/')
    let matched = true
    for (let i = 0; i < levels.length; i += 1) {
      const level = levels[i]
      if (level === '#') {
        // `#` stands in for nothing as well as for something.
        if (i <= parts.length) return true
        matched = false
        break
      }
      if (i >= parts.length || (level !== '+' && level !== parts[i])) {
        matched = false
        break
      }
    }
    if (matched && levels.length === parts.length) return true
  }
  return false
}

function isBraced(level) {
  return level.startsWith('{') && level.endsWith('}')
}

function alternatives(level) {
  return level.slice(1, -1).split(',')
}

function asList(info, key) {
  if (key === null || key === undefined) return []
  if (typeof key === 'string') {
    // One value spelled without an array is the common case and reads
    // well; the character-by-character iteration that would otherwise
    // happen is the kind of quiet wrong answer this library exists to
    // avoid.
    return [key]
  }
  if (!Array.isArray(key)) return [key]
  return [...key]
}

function expected(slots) {
  if (!slots.length) return 'none'
  const said = slots.map((one) => {
    if (one === null) return 'any one level'
    if (one === '#') return 'the rest, or nothing'
    return 'one of ' + one.map((a) => `'${a}'`).join(', ')
  })
  return `${slots.length}: ${said.join('; ')}`
}

function oneLevel(info, value, level) {
  const said = String(value)
  if (said.includes('/')) {
    throw new KeyDoesNotFit(info, `'${said}' holds a '/' and '${level}' is one level`)
  }
  return noWildcards(info, said, level)
}

function alternative(info, value, level) {
  const said = String(value)
  const permitted = alternatives(level)
  if (!permitted.includes(said)) {
    throw new KeyDoesNotFit(
      info,
      `'${said}' is not one of ${permitted.map((a) => `'${a}'`).join(', ')} ` +
        `at level '${level}'`,
    )
  }
  return said
}

function noWildcards(info, value, level) {
  const said = String(value)
  if (said.includes('+') || said.includes('#')) {
    throw new KeyDoesNotFit(
      info,
      `'${said}' holds a wildcard at level '${level}', and MQTT allows none ` +
        'in a topic published to',
    )
  }
  return said
}

// -- taking a slice of what a filter reaches --------------------------------

/** The one User Property a subscriber declares itself with. It is on the
 * packet rather than on a filter, so one declaration applies to every
 * filter in that SUBSCRIBE, and repeating the property is an OR, which is
 * how a member holds more than one slice. */
export const DECLARATION = 'saguin-filter'

/** 2147483647, and it is not a performance limit: it is the largest
 * number a 32-bit gateway and a 64-bit server agree on, so the same
 * SUBSCRIBE is legal on both. */
export const MAX_PARTITIONS = 2147483647

const SIXTY_FOUR = 0xffffffffffffffffn

/** FNV-1a, 64-bit, over the topic's UTF-8 bytes, as a BigInt.
 *
 * The first half of what a slice is taken from, and on its own it is
 * **not** the answer to "which member holds this topic"; `partition` is.
 * It is exposed because RFC 0003 states the two halves separately so that
 * an implementation which disagrees can tell which of them is wrong.
 *
 * **A BigInt rather than a number**: a 64-bit value does not fit in a
 * JavaScript number, and a hash that silently lost its low bits would
 * send every topic to the wrong share.
 *
 * The bytes are the topic's UTF-8 bytes and not its characters, and the
 * multiplication wraps at 64 bits.
 */
export function topicHash(topic) {
  let value = 14695981039346656037n
  for (const byte of Buffer.from(String(topic), 'utf8')) {
    value ^= BigInt(byte)
    value = (value * 1099511628211n) & SIXTY_FOUR
  }
  return value
}

/** Which slice of `partitions` holds `topic`: 0 to partitions - 1.
 *
 * **A constant of the protocol rather than an implementation detail**,
 * which is why it is here rather than only in the broker: a consumer can
 * work out which of its own topics are its own share without asking
 * anybody, and a test can say which member should receive a record rather
 * than accepting whichever one did.
 *
 * Nothing reports a share nobody claimed, since the broker cannot tell
 * that from a member which has not started, so covering every share is
 * the application's job and this is what it does it with.
 *
 * **The mixing step is not optional and not this library's invention.**
 * FNV-1a stirs the top of its accumulator and hardly touches the bottom,
 * which is the half a modulus reads, so a topic scheme carrying an
 * identifier twice, `devices/<id>/messages/devicebound/<id>`, cancels that
 * identifier out of the bottom and sends every one of them to a strict
 * subset of the shares. Every modern hash ends with a step like this;
 * FNV-1a is the unusual one for stopping early.
 */
export function partition(topic, partitions) {
  if (!Number.isInteger(partitions) || partitions < 1) {
    throw new Error(
      `partitions is a whole number of 1 or more, and ${partitions} is not`,
    )
  }
  let value = topicHash(topic)
  value ^= value >> 30n
  value = (value * 0xbf58476d1ce4e5b9n) & SIXTY_FOUR
  value ^= value >> 27n
  value = (value * 0x94d049bb133111ebn) & SIXTY_FOUR
  value ^= value >> 31n
  return Number(value % BigInt(partitions))
}

/** The `saguin-filter` User Properties a SUBSCRIBE should carry.
 *
 * `topicHash` is a `[partitions, index]` pair, or several of them for a
 * member holding several slices, which are an OR. The option is named for
 * the function that goes on the wire, so a second function arrives here
 * as a second option rather than as a larger language.
 *
 * `saguinFilter` is the door for a function this library has not heard
 * of: its strings go on the packet unchanged, so a broker that has grown
 * one is usable from a client that predates it.
 *
 * **What is checked here is what cannot depend on the broker**: a count
 * outside its bound, an index at or above its count, and two slices
 * naming different counts. Each is refused by every saguin there will
 * ever be, so refusing them here turns a `0x83` arriving later on a
 * SUBACK, after the caller has a reader in hand, into a sentence at the
 * call that caused it. Everything else is the broker's to answer.
 */
export function declarations({ topicHash: slices, saguinFilter } = {}) {
  const out = []
  let agreed = null
  for (const [partitions, index] of asSlices(slices)) {
    if (agreed === null) agreed = partitions
    else if (partitions !== agreed) {
      throw new Error(
        'every slice on one subscribe must name the same number of partitions, ' +
          `and ${partitions} is not ${agreed} - a subscription has one ` +
          'partition space, and mixed ones are refused',
      )
    }
    out.push([DECLARATION, `topic_hash(${partitions}, ${index})`])
  }
  for (const one of asRaw(saguinFilter)) out.push([DECLARATION, one])
  return out
}

function asSlices(declared) {
  if (declared === undefined || declared === null) return []
  if (!Array.isArray(declared)) {
    throw new Error(
      'a slice is a [partitions, index] pair of whole numbers, and ' +
        `${JSON.stringify(declared)} is not`,
    )
  }
  if (!declared.length) {
    // **An empty list is refused, and leaving it out is the way to
    // declare nothing.** RFC 0003 refuses an empty `saguin-filter` value
    // rather than ignoring it, because a client that asked for a share
    // and was quietly served the whole channel has nowhere to notice, and
    // the same reasoning reaches an empty list. The caller most likely to
    // pass one is a member whose computed share came out empty by
    // mistake, and every such member would process everything.
    throw new Error(
      'topicHash is empty, which would declare nothing and be served the whole ' +
        'channel - leave it out to ask for everything on purpose',
    )
  }
  // One pair written without a list of pairs, which is the common case.
  const pairs = Array.isArray(declared[0]) ? declared : [declared]
  return pairs.map(oneSlice)
}

function oneSlice(one) {
  if (!Array.isArray(one) || one.length !== 2 || !one.every(Number.isInteger)) {
    throw new Error(
      'a slice is a [partitions, index] pair of whole numbers, and ' +
        `${JSON.stringify(one)} is not`,
    )
  }
  const [partitions, index] = one
  if (partitions < 1 || partitions > MAX_PARTITIONS) {
    throw new Error(
      `${partitions} partitions is outside 1 to ${MAX_PARTITIONS} - a count of ` +
        '1 is legal and means one slice holding everything',
    )
  }
  if (index < 0 || index >= partitions) {
    // **The check that makes a swapped pair safe.** An index must be
    // below its count, so if [8, 1] is a slice then [1, 8] cannot be,
    // which holds for every valid pair, so arguments given the wrong way
    // round are always refused rather than quietly serving a slice nobody
    // asked for.
    throw new Error(
      `index ${index} is not below the ${partitions} partitions it is an index ` +
        `into - an index runs 0 to ${partitions - 1}, so a [partitions, index] ` +
        'pair given the other way round is refused here',
    )
  }
  return [partitions, index]
}

function asRaw(declared) {
  if (declared === undefined || declared === null) return []
  const values = typeof declared === 'string' ? [declared] : [...declared]
  if (!values.length) {
    // The same rule as an empty topicHash, for the same reason.
    throw new Error(
      'saguinFilter is empty, which would declare nothing and be served the ' +
        'whole channel - leave it out to ask for everything on purpose',
    )
  }
  for (const one of values) {
    if (typeof one !== 'string' || !one) {
      // An empty value is refused rather than dropped: a client that
      // asked for a slice and was served the whole channel has nowhere to
      // notice it, which is what the broker refuses it for as well.
      throw new Error(
        "a saguin-filter value is a non-empty call such as 'topic_hash(8, 1)', " +
          `and ${JSON.stringify(one)} is not`,
      )
    }
  }
  return values
}
