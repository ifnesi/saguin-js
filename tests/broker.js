/**
 * Starting a broker, and finding the binary to start it with.
 *
 * The rule for where the broker is, and how to wait for it to listen,
 * lives in one place so that two callers cannot refuse differently when
 * it is not there.
 */

import { spawn, spawnSync } from 'node:child_process'
import { accessSync, constants, closeSync, openSync, readFileSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'
import net from 'node:net'
import { delimiter } from 'node:path'

/** Names the broker to run against. Until saguin publishes a release
 * binary there is nothing to install, so this points at one you built:
 * `make build` in a saguin checkout writes ./bin/saguin. */
export const BROKER_ENV = 'SAGUIN_BROKER'

export function brokerBinary() {
  const named = process.env[BROKER_ENV]
  if (named) {
    try {
      accessSync(named, constants.X_OK)
    } catch {
      throw new Error(`${BROKER_ENV}=${named} is not an executable file`)
    }
    return named
  }
  const found = onPath('saguin')
  if (found) return found
  throw new Error(
    `no saguin broker to test against: set ${BROKER_ENV} to the binary, or ` +
      'put `saguin` on PATH. `make build` in a saguin checkout writes ' +
      './bin/saguin.',
  )
}

function onPath(name) {
  for (const dir of (process.env.PATH ?? '').split(delimiter)) {
    if (!dir) continue
    const candidate = join(dir, name)
    try {
      accessSync(candidate, constants.X_OK)
      return candidate
    } catch {
      // not here, keep looking
    }
  }
  return null
}

export function aFreePort() {
  return new Promise((resolve, reject) => {
    const probe = net.createServer()
    probe.on('error', reject)
    probe.listen(0, '127.0.0.1', () => {
      const { port } = probe.address()
      probe.close(() => resolve(port))
    })
  })
}

/** A saguin process, its configuration, and everything it said.
 *
 * The log is kept and printed on any failure to start or stop, because a
 * check that reports "the broker did not come up" without the broker's
 * own words sends the reader to guess.
 */
export class Broker {
  /** `config` is the configuration to run, as text.
   *
   * Passed in rather than known here, because callers want different
   * brokers: the suite wants channels to test against, and a tour wants
   * the file a reader can start themselves. What they share is everything
   * below.
   */
  constructor(binary, workdir, config) {
    this.binary = binary
    this.configText = config
    this.workdir = workdir
    this.port = null
    this.wsPort = null
    this.address = null
    /** The door that wants a password: a WebSocket listener, since a
     * broker takes one listener of each kind and the plain one is in
     * use. */
    this.guarded = null
    this.passwd = join(workdir, 'clients.passwd')
    this.config = join(workdir, 'saguin.yaml')
    this.logfile = join(workdir, 'saguin.log')
    this.proc = null
    this.exited = null
    this.log = null
  }

  /** `mqtt://127.0.0.1:<port>`, the plain door every test that does not
   * care about credentials connects to. */
  get url() {
    return `mqtt://127.0.0.1:${this.port}`
  }

  get guardedUrl() {
    return `ws://127.0.0.1:${this.wsPort}`
  }

  async start() {
    this.port = await aFreePort()
    this.wsPort = await aFreePort()
    this.address = { host: '127.0.0.1', port: this.port }
    this.guarded = { host: '127.0.0.1', port: this.wsPort }

    if (this.configText.includes('{passwd}')) {
      const made = spawnSync(
        this.binary,
        ['--passwd', 'add', this.passwd, 'operator', 'hunter2'],
        { encoding: 'utf8' },
      )
      if (made.status !== 0) {
        throw new Error(
          'the broker could not write the password file:\n' +
            (made.stdout ?? '') + (made.stderr ?? ''),
        )
      }
    }

    // **Replaced rather than formatted.** A channel filter may carry
    // `{a,b}`, a level with a fixed set of spellings, and a formatter
    // reads that as a field to substitute. Naming the three placeholders
    // cannot.
    let written = this.configText
    for (const [name, value] of [
      ['{port}', this.port], ['{ws_port}', this.wsPort], ['{passwd}', this.passwd],
    ]) {
      written = written.split(name).join(String(value))
    }
    writeFileSync(this.config, written)

    // The broker's own reading of the file, before anything blames the
    // network for a typo in it.
    const checked = spawnSync(this.binary, ['--check-config', this.config], {
      encoding: 'utf8',
    })
    if (checked.status !== 0) {
      throw new Error(
        'the broker refused the test configuration:\n' +
          (checked.stdout ?? '') + (checked.stderr ?? ''),
      )
    }

    this.log = openSync(this.logfile, 'w')
    this.proc = spawn(this.binary, ['-config', this.config], {
      stdio: ['ignore', this.log, this.log],
    })
    this.exited = null
    this.proc.on('exit', (code, signal) => {
      this.exited = { code, signal }
    })
    await this.#waitForThePort()
    return this
  }

  async #waitForThePort(timeout = 15_000) {
    const deadline = Date.now() + timeout
    while (Date.now() < deadline) {
      if (this.exited) {
        throw new Error(
          `the broker exited with ${this.exited.code ?? this.exited.signal} ` +
            `before it listened:\n${this.said()}`,
        )
      }
      if (await knocks(this.address, 250)) return
      await sleep(50)
    }
    throw new Error(
      `the broker did not listen on ${this.port} within ${timeout}ms:\n${this.said()}`,
    )
  }

  said() {
    try {
      return readFileSync(this.logfile, 'utf8')
    } catch {
      return '(no log)'
    }
  }

  async stop() {
    if (!this.proc) return
    const ended = new Promise((resolve) => this.proc.once('exit', resolve))
    this.proc.kill('SIGTERM')
    const inTime = await Promise.race([ended.then(() => true), sleep(10_000).then(() => false)])
    if (!inTime) {
      this.proc.kill('SIGKILL')
      await ended
      closeSync(this.log)
      throw new Error('the broker ignored SIGTERM and had to be killed:\n' + this.said())
    }
    closeSync(this.log)
  }

  /** A crashed broker makes every later test fail for the wrong reason,
   * and the run reports on a broker it was not driving. */
  checkItStayedUp() {
    if (this.exited) {
      throw new Error(
        `the broker exited during the run with ` +
          `${this.exited.code ?? this.exited.signal}:\n${this.said()}`,
      )
    }
  }
}

function knocks(address, timeout) {
  return new Promise((resolve) => {
    const socket = net.connect({ host: address.host, port: address.port })
    const done = (answer) => {
      socket.destroy()
      resolve(answer)
    }
    socket.setTimeout(timeout, () => done(false))
    socket.once('connect', () => done(true))
    socket.once('error', () => done(false))
  })
}

export function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}
