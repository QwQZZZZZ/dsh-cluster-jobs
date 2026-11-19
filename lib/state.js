/**
 * Local job registry for the cluster-jobs plugin.
 *
 * Slurm reports job state, but the output directory of a submitted job is only
 * known locally. This registry maps `(host, job id)` to that directory so that
 * `logs` and `cancel` work without extra parameters. It never stores secrets.
 */
import { appendFileSync, existsSync, mkdirSync, readFileSync, renameSync, statSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'

/** Maximum retained jobs; the oldest submissions are dropped first. */
const MAX_JOBS = 400
/** Maximum debug log size before it is rotated. */
const MAX_LOG_BYTES = 2 * 1024 * 1024
/** Registry schema version. */
const VERSION = 1

/**
 * @typedef {object} JobRecord
 * @property {string} jobId - Slurm job id.
 * @property {string} host - cluster host the job belongs to.
 * @property {string} name - job name.
 * @property {string} directory - remote directory holding the script and output.
 * @property {string} scriptPath - remote path of the submitted script.
 * @property {string} submittedAt - ISO timestamp on the local machine.
 */

/**
 * Registry file path.
 * @param {{ stateDir: string }} config - effective configuration.
 * @returns {string} absolute path.
 */
function statePath(config) {
  return join(config.stateDir, 'jobs.json')
}

/**
 * Read the registry, tolerating a missing or corrupt file.
 * @param {{ stateDir: string }} config - effective configuration.
 * @returns {{ version: number, jobs: Record<string, JobRecord> }} registry contents.
 */
export function readState(config) {
  const path = statePath(config)
  if (!existsSync(path)) return { version: VERSION, jobs: {} }
  try {
    const parsed = JSON.parse(readFileSync(path, 'utf8'))
    if (parsed === null || typeof parsed !== 'object' || typeof parsed.jobs !== 'object' || parsed.jobs === null) {
      return { version: VERSION, jobs: {} }
    }
    return { version: VERSION, jobs: parsed.jobs }
  } catch {
    return { version: VERSION, jobs: {} }
  }
}

/**
 * Persist the registry, oldest records dropped past the retention limit.
 * @param {{ stateDir: string }} config - effective configuration.
 * @param {{ jobs: Record<string, JobRecord> }} state - registry contents.
 * @returns {void}
 */
export function writeState(config, state) {
  mkdirSync(config.stateDir, { recursive: true })
  const entries = Object.entries(state.jobs)
  entries.sort((left, right) => String(right[1].submittedAt).localeCompare(String(left[1].submittedAt)))
  const jobs = Object.fromEntries(entries.slice(0, MAX_JOBS))
  const path = statePath(config)
  const temporary = `${path}.tmp`
  writeFileSync(temporary, JSON.stringify({ version: VERSION, jobs }, null, 2), 'utf8')
  renameSync(temporary, path)
}

/** Registry key for one cluster job. */
export function jobKey(host, jobId) {
  return `${host}:${jobId}`
}

/**
 * Record a submission.
 * @param {{ stateDir: string }} config - effective configuration.
 * @param {JobRecord} record - submission details.
 * @returns {void}
 */
export function recordJob(config, record) {
  const state = readState(config)
  state.jobs[jobKey(record.host, record.jobId)] = record
  writeState(config, state)
}

/**
 * Look up one submission.
 * @param {{ stateDir: string, host: string }} config - effective configuration.
 * @param {string} jobId - Slurm job id.
 * @returns {JobRecord|undefined} the record when known.
 */
export function findJob(config, jobId) {
  return readState(config).jobs[jobKey(config.host, jobId)]
}

/**
 * Merge fields into one known submission.
 * @param {{ stateDir: string, host: string }} config - effective configuration.
 * @param {string} jobId - Slurm job id.
 * @param {object} patch - fields to update.
 * @returns {void}
 */
export function updateJob(config, jobId, patch) {
  const state = readState(config)
  const key = jobKey(config.host, jobId)
  const existing = state.jobs[key]
  if (existing === undefined) return
  state.jobs[key] = { ...existing, ...patch }
  writeState(config, state)
}

/**
 * Append one diagnostic line when debug logging is enabled.
 * @param {{ stateDir: string, debug: boolean }} config - effective configuration.
 * @param {object} event - command and its outcome.
 * @returns {void}
 */
export function debugLog(config, event) {
  if (!config.debug) return
  try {
    mkdirSync(config.stateDir, { recursive: true })
    const path = join(config.stateDir, 'commands.log')
    if (existsSync(path) && statSync(path).size > MAX_LOG_BYTES) {
      renameSync(path, `${path}.1`)
    }
    const { command, result } = event
    const summary = result === undefined
      ? ''
      : ` exit=${result.code} timedOut=${result.timedOut} stderr=${JSON.stringify(String(result.stderr).slice(0, 400))}`
    appendFileSync(path, `${new Date().toISOString()} ${command}${summary}\n`, 'utf8')
  } catch {
    // Diagnostics must never fail a tool call.
  }
}
