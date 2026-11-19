/**
 * SLURM backend: sbatch, squeue, sacct, scancel.
 */
import { execRemote, joinRemote, remotePathExpr, shellQuote } from '../remote.js'
import { findJob, recordJob, updateJob } from '../state.js'
import { prepareSubmission, sequence, writeScriptCommands } from './common.js'

/** Backend identifier. */
export const name = 'slurm'

/** Default number of log lines returned by `logs`. */
const DEFAULT_LOG_LINES = 200

/**
 * Parse `squeue` rows produced with `-o '%i|%j|%T|%M|%P|%R|%D'`.
 * @param {string} stdout - command output.
 * @returns {Array<object>} structured jobs.
 */
function parseQueue(stdout) {
  const jobs = []
  for (const rawLine of stdout.split('\n')) {
    const line = rawLine.trim()
    if (line === '') continue
    const [jobId, jobName, state, elapsed, partition, nodes, nodeCount] = line.split('|')
    if (jobId === undefined || !/^\d+/.test(jobId)) continue
    jobs.push({
      jobId,
      name: jobName ?? '',
      state: state ?? '',
      elapsed: elapsed ?? '',
      partition: partition ?? '',
      nodes: nodes ?? '',
      nodeCount: nodeCount ?? '',
    })
  }
  return jobs
}

/**
 * Parse one `sacct -P` row group.
 * @param {string} stdout - command output.
 * @param {string} jobId - requested job id.
 * @returns {object|undefined} the job's own accounting row.
 */
function parseAccounting(stdout, jobId) {
  for (const rawLine of stdout.split('\n')) {
    const line = rawLine.trim()
    if (line === '') continue
    const fields = line.split('|')
    if (fields[0] !== jobId) continue
    return {
      jobId,
      name: fields[1] ?? '',
      state: fields[2] ?? '',
      elapsed: fields[3] ?? '',
      partition: fields[4] ?? '',
      nodes: fields[5] ?? '',
      exitCode: fields[6] ?? '',
      nodeCount: '',
    }
  }
  return undefined
}

/**
 * Translate SLURM submission options into sbatch flags.
 * @param {object} options - per-call overrides.
 * @param {object} defaults - configured defaults.
 * @returns {string[]} shell-ready flags.
 */
function sbatchFlags(options, defaults) {
  const merged = { ...defaults, ...options }
  const flags = []
  const add = (flag, value) => {
    if (value === undefined || value === null) return
    const text = String(value).trim()
    if (text === '') return
    flags.push(`${flag}=${shellQuote(text)}`)
  }
  add('--job-name', merged.jobName)
  add('--partition', merged.partition)
  add('--qos', merged.qos)
  add('--account', merged.account)
  add('--time', merged.time)
  add('--nodes', merged.nodes)
  add('--mem', merged.mem)
  add('--cpus-per-task', merged.cpusPerTask ?? merged.cpus)
  add('--gres', merged.gres ?? (merged.gpus === undefined || merged.gpus === null || String(merged.gpus).trim() === ''
    ? undefined
    : `gpu:${String(merged.gpus).trim()}`))
  add('--dependency', merged.dependency)
  add('--output', merged.output)
  add('--error', merged.error)
  if (merged.exclusive === true) flags.push('--exclusive')
  const extra = Array.isArray(merged.extra) ? merged.extra : []
  for (const entry of extra) {
    const text = String(entry).trim()
    if (text !== '') flags.push(text)
  }
  return flags
}

/**
 * Upload a job script and submit it with sbatch.
 * @param {{ cluster: object }} env - execution environment.
 * @param {object} args - tool arguments.
 * @returns {Promise<object>} canonical result.
 */
export async function submit(env, args) {
  const { cluster } = env
  const prepared = prepareSubmission(env, args, 'sbatch')
  const { content, name, directory, scriptPath, origin, lineEndingsChanged } = prepared

  const options = {
    jobName: name,
    ...(args.options ?? {}),
    // Relative paths resolve inside the submission directory, so Slurm never
    // has to expand `~` or `$HOME`, which it does not do.
    output: args.options?.output ?? 'slurm-%j.out',
    error: args.options?.error ?? 'slurm-%j.err',
  }
  const flags = sbatchFlags(options, cluster.slurm)
  const command = sequence([
    ...writeScriptCommands(directory, scriptPath, content),
    `sbatch --parsable ${flags.join(' ')} ${remotePathExpr(scriptPath)}`.replace(/\s+/g, ' ').trim(),
  ])

  const outcome = await execRemote(env, command, { timeoutMs: 120_000, what: 'sbatch' })
  const jobId = outcome.stdout.split('\n').map(line => line.trim()).filter(Boolean)
    .map(line => line.split(';')[0].trim()).filter(line => /^\d+$/.test(line)).pop()
  if (jobId === undefined) {
    throw new Error(`sbatch produced no job id. Output: ${outcome.stdout.trim() || '(empty)'} ${outcome.stderr.trim()}`.trim())
  }

  recordJob(cluster, {
    jobId,
    host: cluster.host,
    cluster: cluster.name,
    scheduler: 'slurm',
    name,
    directory,
    scriptPath,
    outputPath: joinRemote(directory, `slurm-${jobId}.out`),
    errorPath: joinRemote(directory, `slurm-${jobId}.err`),
    submittedAt: new Date().toISOString(),
  })

  const partition = options.partition || cluster.slurm.partition || 'cluster default'
  const summary = `Submitted job ${jobId} "${name}" to ${partition}; work directory ${directory}`
  return {
    action: 'submit',
    ok: true,
    summary,
    jobId,
    jobName: name,
    directory,
    scriptPath,
    partition,
    scheduler: 'slurm',
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
  const cluster = env.cluster
  const jobId = String(args.jobId).trim()
  const active = await execRemote(env, `squeue -h -j ${shellQuote(jobId)} -o '%i|%j|%T|%M|%P|%R|%D'`, {
    timeoutMs: 30_000,
    what: `squeue for job ${jobId}`,
    tolerate: true,
  })
  const running = parseQueue(active.stdout)
  const record = findJob(cluster, jobId)
  if (running.length > 0) {
    const job = running[0]
    const summary = `Job ${jobId} "${job.name}" is ${job.state}`
      + (job.nodes && job.nodes !== '(Priority)' ? ` on ${job.nodes}` : '')
      + (job.elapsed ? ` (elapsed ${job.elapsed})` : '')
      + (job.partition ? ` in ${job.partition}` : '')
    return { action: 'status', ok: true, summary, job, directory: record?.directory ?? '', jobs: running }
  }

  const accounting = await execRemote(env, `sacct -n -P -j ${shellQuote(jobId)} --format=JobID,JobName,State,Elapsed,Partition,NodeList,ExitCode`, {
    timeoutMs: 30_000,
    what: `sacct for job ${jobId}`,
    tolerate: true,
  })
  const finished = parseAccounting(accounting.stdout, jobId)
  if (finished === undefined) {
    throw new Error(`Job ${jobId} is not in the queue and has no accounting record on ${cluster.host}. `
      + 'Check the job id, or use action "queue" to list active jobs.')
  }
  const summary = `Job ${jobId} "${finished.name}" is ${finished.state}`
    + (finished.exitCode ? ` (exit ${finished.exitCode})` : '')
    + (finished.elapsed ? ` after ${finished.elapsed}` : '')
    + (finished.nodes ? ` on ${finished.nodes}` : '')
  return {
    action: 'status',
    ok: true,
    summary,
    job: finished,
    directory: record?.directory ?? '',
    jobs: [finished],
  }
}

/**
 * List the user's active jobs.
 * @param {{ cluster: object }} env - execution environment.
 * @returns {Promise<object>} canonical result.
 */
export async function queue(env) {
  const cluster = env.cluster
  const outcome = await execRemote(env, `squeue -h -u ${shellQuote(cluster.user)} -o '%i|%j|%T|%M|%P|%R|%D'`, {
    timeoutMs: 30_000,
    what: 'squeue',
  })
  const jobs = parseQueue(outcome.stdout)
  const summary = jobs.length === 0
    ? `No active jobs for ${cluster.user} on ${cluster.host}`
    : `${jobs.length} active job${jobs.length === 1 ? '' : 's'} for ${cluster.user}: `
      + jobs.map(job => `${job.jobId} ${job.state}`).join(', ')
  return { action: 'queue', ok: true, summary, jobs }
}

/**
 * Locate one job's log file on the cluster.
 * @param {{ cluster: object }} env - execution environment.
 * @param {string} jobId - Slurm job id.
 * @param {'out'|'err'} stream - stdio stream.
 * @param {string} [explicit] - caller-supplied remote path.
 * @returns {Promise<string>} remote path, or an empty string when unknown.
 */
async function resolveLogPath(env, jobId, stream, explicit) {
  if (typeof explicit === 'string' && explicit.trim() !== '') return explicit.trim()
  const record = findJob(env.cluster, jobId)
  if (record !== undefined) return joinRemote(record.directory, `slurm-${jobId}.${stream}`)
  const search = await execRemote(
    env,
    `find ${remotePathExpr(env.cluster.remoteRoot)} -maxdepth 2 -name ${shellQuote(`slurm-${jobId}.${stream}`)} -print -quit 2>/dev/null`,
    { timeoutMs: 30_000, what: 'log lookup', tolerate: true },
  )
  return search.stdout.split('\n').map(line => line.trim()).filter(Boolean)[0] ?? ''
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
  const path = await resolveLogPath(env, jobId, stream, args.path)
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
      throw new Error(`Log file ${path} does not exist yet. The job may still be queued, or Slurm writes its output elsewhere; pass \`path\` to read a specific file.`)
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
  await execRemote(env, `scancel ${shellQuote(jobId)}`, { timeoutMs: 30_000, what: `scancel ${jobId}` })
  updateJob(env.cluster, jobId, { cancelledAt: new Date().toISOString() })
  return { action: 'cancel', ok: true, summary: `Cancelled job ${jobId}`, jobId }
}
