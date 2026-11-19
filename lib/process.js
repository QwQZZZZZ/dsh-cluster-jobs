/**
 * Bounded child-process execution for the cluster-jobs plugin.
 *
 * Every remote interaction goes through one short-lived local process, so the
 * plugin never keeps a connection open between tool calls and cannot deadlock
 * on a broken stream.
 */
import { spawn } from 'node:child_process'

/** Default stdout budget for one remote command. */
export const DEFAULT_MAX_STDOUT = 120_000
/** Default stderr budget for one remote command. */
export const DEFAULT_MAX_STDERR = 24_000

/**
 * Run one local process to completion with bounded output.
 * @param {string} binary - executable to start.
 * @param {string[]} args - arguments passed without shell parsing.
 * @param {{ signal?: AbortSignal, timeoutMs?: number, maxStdout?: number, maxStderr?: number, env?: Record<string, string> }} [options] - limits and environment.
 * @returns {Promise<{ code: number|null, stdout: string, stderr: string, truncated: { out: boolean, err: boolean }, timedOut: boolean, aborted: boolean, spawnError?: Error }>} outcome.
 */
export function runProcess(binary, args, options = {}) {
  const {
    signal,
    timeoutMs = 60_000,
    maxStdout = DEFAULT_MAX_STDOUT,
    maxStderr = DEFAULT_MAX_STDERR,
    env,
  } = options

  return new Promise((resolve) => {
    const chunks = { out: [], err: [] }
    const sizes = { out: 0, err: 0 }
    const truncated = { out: false, err: false }
    let settled = false
    let timedOut = false
    let aborted = false
    let killTimer
    let timeoutTimer

    const collect = (key, cap) => (chunk) => {
      if (sizes[key] >= cap) {
        truncated[key] = true
        return
      }
      const room = cap - sizes[key]
      const kept = chunk.length > room ? chunk.subarray(0, room) : chunk
      chunks[key].push(kept)
      sizes[key] += kept.length
      if (kept.length < chunk.length) truncated[key] = true
    }

    let child
    try {
      child = spawn(binary, args, {
        windowsHide: true,
        stdio: ['ignore', 'pipe', 'pipe'],
        ...(env === undefined ? {} : { env }),
      })
    } catch (error) {
      resolve({
        code: null,
        stdout: '',
        stderr: '',
        truncated,
        timedOut: false,
        aborted: false,
        spawnError: error instanceof Error ? error : new Error(String(error)),
      })
      return
    }

    const cleanup = () => {
      clearTimeout(timeoutTimer)
      clearTimeout(killTimer)
      signal?.removeEventListener('abort', onAbort)
    }

    const finish = (code) => {
      if (settled) return
      settled = true
      cleanup()
      resolve({
        code: code ?? null,
        stdout: Buffer.concat(chunks.out).toString('utf8'),
        stderr: Buffer.concat(chunks.err).toString('utf8'),
        truncated,
        timedOut,
        aborted,
      })
    }

    const terminate = () => {
      if (child.exitCode !== null || child.signalCode !== null) return
      child.kill()
      killTimer = setTimeout(() => child.kill('SIGKILL'), 2000)
      killTimer.unref?.()
    }

    const onAbort = () => {
      aborted = true
      terminate()
    }

    child.stdout?.on('data', collect('out', maxStdout))
    child.stderr?.on('data', collect('err', maxStderr))
    child.on('error', (error) => {
      child.off('close', finish)
      if (settled) return
      settled = true
      cleanup()
      resolve({
        code: null,
        stdout: Buffer.concat(chunks.out).toString('utf8'),
        stderr: Buffer.concat(chunks.err).toString('utf8'),
        truncated,
        timedOut,
        aborted,
        spawnError: error instanceof Error ? error : new Error(String(error)),
      })
    })
    child.on('close', (code) => finish(code))

    if (Number.isFinite(timeoutMs) && timeoutMs > 0) {
      timeoutTimer = setTimeout(() => {
        timedOut = true
        terminate()
      }, timeoutMs)
      timeoutTimer.unref?.()
    }

    if (signal !== undefined) {
      if (signal.aborted) onAbort()
      else signal.addEventListener('abort', onAbort, { once: true })
    }
  })
}
