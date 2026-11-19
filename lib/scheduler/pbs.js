/**
 * PBS Professional backend: qsub, qstat, qdel.
 *
 * ASPIRE 2A and similar NSCC systems use PBS rather than SLURM, request
 * resources with `-l select=...`, need a project code with `-P`, and report job
 * ids such as `1852792.pbs101`.
 */
import { execRemote, joinRemote, remotePathExpr, shellQuote } from '../remote.js'
import { findJob, recordJob, updateJob } from '../state.js'
import { prepareSubmission, sequence, writeScriptCommands } from './common.js'

/** Backend identifier. */
export const name = 'pbs'

/** Default number of log lines returned by `logs`. */
const DEFAULT_LOG_LINES = 200

/** PBS job states mapped to readable labels. */
const STATE_LABELS = {
  Q: 'QUEUED',
  R: 'RUNNING',
  H: 'HELD',
  E: 'EXITING',
  F: 'FINISHED',
  X: 'FINISHED',
  S: 'SUSPENDED',
  W: 'WAITING',
  T: 'MOVED',
}

/**
 * Parse the `key = value` attributes of a `qstat -f` block.
 * @param {string} stdout - command output.
 * @returns {Map<string, string>} attributes keyed exactly as printed.
 */
function parseAttributes(stdout) {
  const attributes = new Map()
  for (const rawLine of stdout.split('\n')) {
    const line = rawLine.trim()
    if (line === '' || line.startsWith('Job Id:')) continue
    const separator = line.indexOf(' = ')
    if (separator <= 0) continue
    attributes.set(line.slice(0, separator).trim(), line.slice(separator + 3).trim())
  }
  return attributes
}

/**
 * Drop a login-node prefix from a PBS path attribute.
 * @param {string|undefined} value - `Output_Path` or `Error_Path`.
 * @returns {string} remote path, or an empty string.
 */
function remotePathOf(value) {
  if (value === undefined) return ''
  const separator = value.indexOf(':')
  const path = separator >= 0 ? value.slice(separator + 1) : value
  return path.trim()
}

/** Numeric part of a PBS job id (`1852792.pbs101` → `1852792`). */
function numericJobId(jobId) {
  return String(jobId).split('.')[0]
}

/**
 * Translate PBS submission options into qsub flags.
 * @param {object} options - per-call overrides.
 * @param {object} defaults - configured defaults.
 * @returns {string[]} shell-ready flags.
 */
function qsubFlags(options, defaults) {
  const merged = { ...defaults, ...options }
  const flags = []
  const add = (flag, value) => {
    if (value === undefined || value === null) return
    const text = String(value).trim()
    if (text === '') return
    flags.push(`${flag}=${shellQuote(text)}`)
  }
  add('-N', merged.jobName)
  add('-q', merged.queue)
  add('-P', merged.project)
  const select = String(merged.select ?? '').trim()
  if (select !== '') {
    add('-l', select.startsWith('select=') ? select : `select=${select}`)
  } else {
    const resources = []
    for (const [key, value] of [['ngpus', merged.ngpus], ['ncpus', merged.ncpus], ['mem', merged.mem]]) {
      if (value !== undefined && value !== null && String(value).trim() !== '') resources.push(`${key}=${String(value).trim()}`)
    }
    if (resources.length > 0) add('-l', `select=1:${resources.join(':')}`)
  }
  add('-l', merged.walltime === undefined || merged.walltime === null || String(merged.walltime).trim() === ''
    ? undefined
    : `walltime=${String(merged.walltime).trim()}`)
  add('-o', merged.output)
  add('-e', merged.error)
  const extra = Array.isArray(merged.extra) ? merged.extra : []
  for (const entry of extra) {
    const text = String(entry).trim()
    if (text !== '') flags.push(text)
  }
  return flags
}

/**
 * Read queue and history attributes for one job.
 * @param {{ cluster: object }} env - execution environment.
 * @param {string} jobId - PBS job id.
 * @returns {Promise<{ attributes: Map<string, string>, history: boolean }>} attributes plus whether history was needed.
 */
async function readJob(env, jobId) {
  const active = await execRemote(env, `qstat -f ${shellQuote(jobId)}`, {
    timeoutMs: 30_000,
    what: `qstat for job ${jobId}`,
    tolerate: true,
  })
  const activeAttributes = parseAttributes(active.stdout)
  if (activeAttributes.has('job_state')) return { attributes: activeAttributes, history: false }
  const history = await execRemote(env, `qstat -x -f ${shellQuote(jobId)}`, {
    timeoutMs: 30_000,
    what: `qstat history for job ${jobId}`,
    tolerate: true,
  })
  return { attributes: parseAttributes(history.stdout), history: true }
}

/**
 * Upload a job script and submit it with qsub.
 * @param {{ cluster: object }} env - execution environment.
 * @param {object} args - tool arguments.
 * @returns {Promise<object>} canonical result.
 */
export async function submit(env, args) {
  const { cluster } = env
  const prepared = prepareSubmission(env, args, 'pbs')
  const { content, name, directory, scriptPath, origin, lineEndingsChanged } = prepared

  const flags = qsubFlags({ jobName: name, ...(args.options ?? {}) }, cluster.pbs)
  const command = sequence([
    ...writeScriptCommands(directory, scriptPath, content),
    `qsub ${flags.join(' ')} ${remotePathExpr(scriptPath)}`.replace(/\s+/g, ' ').trim(),
  ])

  const outcome = await execRemote(env, command, { timeoutMs: 120_000, what: 'qsub' })
  const jobId = outcome.stdout.split('\n').map(line => line.trim()).filter(Boolean)
    .filter(line => /^\d+(\.[A-Za-z0-9._-]+)?$/.test(line)).pop()
  if (jobId === undefined) {
    throw new Error(`qsub produced no job id. Output: ${outcome.stdout.trim() || '(empty)'} ${outcome.stderr.trim()}`.trim())
  }

  // Ask PBS where it writes the logs; the naming convention alone is ambiguous.
  let outputPath = ''
  let errorPath = ''
  try {
    const { attributes } = await readJob(env, jobId)
    outputPath = remotePathOf(attributes.get('Output_Path'))
    errorPath = remotePathOf(attributes.get('Error_Path'))
  } catch {
    // A queued job may not expose paths yet; fall back to the documented names.
  }
  const numeric = numericJobId(jobId)
  if (outputPath === '') outputPath = joinRemote(directory, `${name}.o${numeric}`)
  if (errorPath === '') errorPath = joinRemote(directory, `${name}.e${numeric}`)

  recordJob(cluster, {
    jobId,
    host: cluster.host,
    cluster: cluster.name,
    scheduler: 'pbs',
    name,
    directory,
    scriptPath,
    outputPath,
    errorPath,
    submittedAt: new Date().toISOString(),
  })

  const queue = args.options?.queue || cluster.pbs.queue || 'cluster default'
  const summary = `Submitted job ${jobId} "${name}" to ${queue}; work directory ${directory}`
  return {
    action: 'submit',
    ok: true,
    summary,
    jobId,
    jobName: name,
    directory,
    scriptPath,
    partition: queue,
    scheduler: 'pbs',
    cluster: cluster.name,
    origin,
    lineEndingsNormalized: lineEndingsChanged,
  }
}

/**
 * Report one job's current state.
 * @param {{ cluster: object }} env - execution environment.
 * @param {object} args - tool arguments.
 * @returns {Promise<object>} canonical result.
 */
export async function status(env, args) {
  const { cluster } = env
  const jobId = String(args.jobId).trim()
  const { attributes, history } = await readJob(env, jobId)
  const rawState = attributes.get('job_state') ?? ''
  if (rawState === '') {
    throw new Error(`Job ${jobId} is unknown to PBS on ${cluster.host}. `
      + 'Check the job id, or use action "queue" to list active jobs.')
  }
  const state = STATE_LABELS[rawState.toUpperCase()] ?? rawState
  const jobName = attributes.get('Job_Name') ?? ''
  const queueName = attributes.get('queue') ?? ''
  const walltime = attributes.get('resources_used.walltime') ?? ''
  const exitStatus = attributes.get('Exit_status') ?? ''
  const execHost = (attributes.get('exec_host') ?? '').split(/[+/]/)[0].trim()
  const job = {
    jobId,
    name: jobName,
    state,
    elapsed: walltime,
    partition: queueName,
    nodes: execHost,
    nodeCount: '',
    exitCode: exitStatus,
  }
  const summary = `Job ${jobId} "${jobName}" is ${state}`
    + (exitStatus !== '' && history ? ` (exit ${exitStatus})` : '')
    + (walltime !== '' ? ` after ${walltime}` : '')
    + (execHost !== '' ? ` on ${execHost}` : '')
    + (queueName !== '' ? ` in ${queueName}` : '')
  const record = findJob(cluster, jobId)
  return {
    action: 'status',
    ok: true,
    summary,
    job,
    directory: record?.directory ?? '',
    jobs: [job],
  }
}

/**
 * List the user's active jobs.
 * @param {{ cluster: object }} env - execution environment.
 * @returns {Promise<object>} canonical result.
 */
export async function queue(env) {
  const { cluster } = env
  const outcome = await execRemote(env, `qstat -u ${shellQuote(cluster.user)}`, {
    timeoutMs: 30_000,
    what: 'qstat',
  })
  const jobs = []
  for (const rawLine of outcome.stdout.split('\n')) {
    const line = rawLine.trim()
    if (line === '' || line.startsWith('Job ID') || line.startsWith('-')) continue
    const fields = line.split(/\s+/)
    if (fields.length < 3 || !/^\d+(\.[A-Za-z0-9._-]+)?$/.test(fields[0])) continue
    jobs.push({
      jobId: fields[0],
      name: fields[1] ?? '',
      state: STATE_LABELS[(fields[4] ?? '').toUpperCase()] ?? fields[4] ?? '',
      elapsed: fields[3] ?? '',
      partition: fields[5] ?? '',
      nodes: '',
      nodeCount: '',
    })
  }
  const summary = jobs.length === 0
    ? `No active jobs for ${cluster.user} on ${cluster.host}`
    : `${jobs.length} active job${jobs.length === 1 ? '' : 's'} for ${cluster.user}: `
      + jobs.map(job => `${job.jobId} ${job.state}`).join(', ')
  return { action: 'queue', ok: true, summary, jobs }
}

/**
 * Tail a job's output or error log.
 * @param {{ cluster: object }} env - execution environment.
 * @param {object} args - tool arguments.
 * @returns {Promise<object>} canonical result.
 */
export async function logs(env, args) {
  const jobId = String(args.jobId).trim()
  const stream = args.stream === 'err' ? 'err' : 'out'
  const requested = Number.isFinite(Number(args.lines)) ? Number(args.lines) : DEFAULT_LOG_LINES
  const lines = Math.min(2000, Math.max(20, Math.trunc(requested)))
  const record = findJob(env.cluster, jobId)
  const explicit = typeof args.path === 'string' && args.path.trim() !== '' ? args.path.trim() : ''
  const path = explicit !== ''
    ? explicit
    : (stream === 'err' ? record?.errorPath : record?.outputPath) ?? ''
  if (path === '') {
    throw new Error(`No log path is known for job ${jobId}. Pass \`path\` with the remote log file.`)
  }
  let outcome
  try {
    outcome = await execRemote(env, `tail -n ${lines} ${remotePathExpr(path)}`, {
      timeoutMs: 45_000,
      what: `tail of ${path}`,
    })
  } catch (error) {
    if (String(error?.stderr ?? '').includes('No such file')) {
      throw new Error(`Log file ${path} does not exist yet. The job may still be queued; pass \`path\` to read a specific file.`)
    }
    throw error
  }
  return {
    action: 'logs',
    ok: true,
    summary: `Last ${lines} line${lines === 1 ? '' : 's'} of ${path}`,
    jobId,
    path,
    stream,
    lines,
    text: outcome.stdout,
    truncated: outcome.truncated.out,
  }
}

/**
 * Cancel one queued or running job.
 * @param {{ cluster: object }} env - execution environment.
 * @param {object} args - tool arguments.
 * @returns {Promise<object>} canonical result.
 */
export async function cancel(env, args) {
  const jobId = String(args.jobId).trim()
  await execRemote(env, `qdel ${shellQuote(jobId)}`, { timeoutMs: 30_000, what: `qdel ${jobId}` })
  updateJob(env.cluster, jobId, { cancelledAt: new Date().toISOString() })
  return { action: 'cancel', ok: true, summary: `Cancelled job ${jobId}`, jobId }
}
