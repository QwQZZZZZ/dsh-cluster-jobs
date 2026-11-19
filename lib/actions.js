/**
 * Cluster job actions for the cluster-jobs plugin.
 *
 * Each action runs one short SSH or scp invocation against the configured login
 * node, parses a bounded result, and returns a canonical value for the tool
 * renderer. Nothing here stores or prints credentials.
 */
import { existsSync, mkdirSync, readFileSync, statSync } from 'node:fs'
import { basename, dirname, isAbsolute, resolve } from 'node:path'
import { execRemote, joinRemote, remotePathExpr, runScp, shellQuote, describeTarget } from './remote.js'
import { findJob, recordJob, updateJob } from './state.js'
import { expandHome } from './config.js'

/** Default number of log lines returned by `logs`. */
const DEFAULT_LOG_LINES = 200

/**
 * Resolve a local path against the session working directory.
 * @param {string} cwd - session working directory.
 * @param {string} value - possibly relative local path.
 * @returns {string} absolute local path.
 */
function localPath(cwd, value) {
  const expanded = expandHome(String(value))
  return isAbsolute(expanded) ? expanded : resolve(cwd, expanded)
}

/**
 * Build the remote directory name for a new submission.
 * @param {string} name - sanitized job name.
 * @returns {string} directory name.
 */
function submissionStamp() {
  return new Date().toISOString().replace(/\.\d+Z$/, '').replace('T', '-').replaceAll(':', '')
}

/**
 * Sanitize a job name for use as a directory component.
 * @param {string} value - proposed name.
 * @returns {string} safe name.
 */
function safeJobName(value) {
  const cleaned = String(value).replace(/[^A-Za-z0-9._-]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 64)
  return cleaned || 'dsh-job'
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
 * Parse `squeue` rows produced with `-o '%i|%j|%T|%M|%P|%R|%D'`.
 * @param {string} stdout - command output.
 * @returns {Array<object>} structured jobs.
 */
function parseQueue(stdout) {
  const jobs = []
  for (const rawLine of stdout.split('\n')) {
    const line = rawLine.trim()
    if (line === '') continue
    const [jobId, name, state, elapsed, partition, nodes, nodeCount] = line.split('|')
    if (jobId === undefined || !/^\d+/.test(jobId)) continue
    jobs.push({
      jobId,
      name: name ?? '',
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
 * Join a shell command sequence.
 * @param {string[]} parts - commands that must each succeed.
 * @returns {string} `&&`-joined command.
 */
function sequence(parts) {
  return parts.join(' && ')
}

/**
 * Connectivity and environment probe.
 * @param {object} env - execution environment.
 * @returns {Promise<object>} canonical result.
 */
async function check(env) {
  const { config } = env
  const probe = [
    'printf "hostname=%s\\n" "$(hostname)"',
    'printf "user=%s\\n" "$(whoami)"',
    'printf "home=%s\\n" "$HOME"',
    'printf "sbatch=%s\\n" "$(command -v sbatch || echo missing)"',
    'printf "sbatch-version=%s\\n" "$(sbatch --version 2>/dev/null | head -1 || echo unknown)"',
    'printf "sinfo=%s\\n" "$(sinfo -V 2>/dev/null || echo missing)"',
    'printf "qos=%s\\n" "$(sacctmgr show user "$(whoami)" withassoc format=qos -n 2>/dev/null | head -1 | tr -d " " || echo unknown)"',
  ].join('; ')
  const outcome = await execRemote(env, probe, { timeoutMs: 30_000, what: 'Cluster probe' })
  const facts = {}
  for (const line of outcome.stdout.split('\n')) {
    const index = line.indexOf('=')
    if (index > 0) facts[line.slice(0, index).trim()] = line.slice(index + 1).trim()
  }
  const warnings = []
  if (facts.sbatch === 'missing' || facts.sbatch === '') warnings.push('sbatch was not found on the login node')
  if (facts.sinfo === 'missing') warnings.push('sinfo was not found; Slurm may not be installed as expected')
  const summary = `Connected to ${describeTarget(config)} as ${facts.user ?? config.user} on ${facts.hostname ?? 'unknown host'}`
    + (facts['sbatch-version'] ? ` (${facts['sbatch-version']})` : '')
  return {
    action: 'check',
    ok: warnings.length === 0,
    summary,
    target: describeTarget(config),
    configSource: env.source || '(defaults only)',
    remoteRoot: config.remoteRoot,
    identityFile: config.identityFile ? expandHome(config.identityFile) : '',
    facts,
    warnings,
    stdout: outcome.stdout,
  }
}

/**
 * Upload a job script and submit it with sbatch.
 * @param {object} env - execution environment.
 * @param {object} args - tool arguments.
 * @returns {Promise<object>} canonical result.
 */
async function submit(env, args) {
  const { config } = env
  let content = ''
  let origin = 'inline script'
  if (typeof args.script === 'string' && args.script.trim() !== '') {
    content = args.script
  } else if (typeof args.scriptPath === 'string' && args.scriptPath.trim() !== '') {
    const path = localPath(env.cwd, args.scriptPath)
    if (!existsSync(path)) throw new Error(`Job script not found: ${path}`)
    if (!statSync(path).isFile()) throw new Error(`Job script is not a regular file: ${path}`)
    content = readFileSync(path, 'utf8')
    origin = path
  } else {
    throw new Error('submit needs `script` (inline bash) or `scriptPath` (local file)')
  }
  if (content.trim() === '') throw new Error('The job script is empty')

  const normalized = content.replace(/\r\n/g, '\n')
  const lineEndingsChanged = normalized !== content
  const finalContent = normalized.endsWith('\n') ? normalized : `${normalized}\n`

  const requestedName = args.jobName
    ?? (typeof args.scriptPath === 'string' ? basename(args.scriptPath).replace(/\.[^.]+$/, '') : 'dsh-job')
  const name = safeJobName(requestedName)
  const directory = typeof args.remoteDir === 'string' && args.remoteDir.trim() !== ''
    ? args.remoteDir.trim()
    : joinRemote(config.remoteRoot, `${name}-${submissionStamp()}`)
  const scriptPath = joinRemote(directory, `${name}.sbatch`)

  const options = {
    jobName: name,
    ...(args.options ?? {}),
    // Relative paths resolve inside the submission directory, so Slurm never
    // has to expand `~` or `$HOME`, which it does not do.
    output: args.options?.output ?? 'slurm-%j.out',
    error: args.options?.error ?? 'slurm-%j.err',
  }
  const flags = sbatchFlags(options, config.slurm)
  const encoded = Buffer.from(finalContent, 'utf8').toString('base64')
  const command = sequence([
    `mkdir -p ${remotePathExpr(directory)}`,
    `printf '%s' ${shellQuote(encoded)} | base64 -d > ${remotePathExpr(scriptPath)}`,
    `cd ${remotePathExpr(directory)}`,
    `sbatch --parsable ${flags.join(' ')} ${remotePathExpr(scriptPath)}`.replace(/\s+/g, ' ').trim(),
  ])

  const outcome = await execRemote(env, command, { timeoutMs: 120_000, what: 'sbatch' })
  const jobId = outcome.stdout.split('\n').map(line => line.trim()).filter(Boolean)
    .map(line => line.split(';')[0].trim()).filter(line => /^\d+$/.test(line)).pop()
  if (jobId === undefined) {
    throw new Error(`sbatch produced no job id. Output: ${outcome.stdout.trim() || '(empty)'} ${outcome.stderr.trim()}`.trim())
  }

  recordJob(config, {
    jobId,
    host: config.host,
    name,
    directory,
    scriptPath,
    submittedAt: new Date().toISOString(),
  })

  const partition = options.partition || config.slurm.partition || 'cluster default'
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
    origin,
    lineEndingsNormalized: lineEndingsChanged,
  }
}

/**
 * Report one job's current state.
 * @param {object} env - execution environment.
 * @param {object} args - tool arguments.
 * @returns {Promise<object>} canonical result.
 */
async function status(env, args) {
  const jobId = String(args.jobId).trim()
  const active = await execRemote(env, `squeue -h -j ${shellQuote(jobId)} -o '%i|%j|%T|%M|%P|%R|%D'`, {
    timeoutMs: 30_000,
    what: `squeue for job ${jobId}`,
    tolerate: true,
  })
  const running = parseQueue(active.stdout)
  const record = findJob(env.config, jobId)
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
    throw new Error(`Job ${jobId} is not in the queue and has no accounting record on ${describeTarget(env.config)}. `
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
 * @param {object} env - execution environment.
 * @returns {Promise<object>} canonical result.
 */
async function queue(env) {
  const { config } = env
  const outcome = await execRemote(env, `squeue -h -u ${shellQuote(config.user)} -o '%i|%j|%T|%M|%P|%R|%D'`, {
    timeoutMs: 30_000,
    what: 'squeue',
  })
  const jobs = parseQueue(outcome.stdout)
  const summary = jobs.length === 0
    ? `No active jobs for ${config.user} on ${describeTarget(config)}`
    : `${jobs.length} active job${jobs.length === 1 ? '' : 's'} for ${config.user}: `
      + jobs.map(job => `${job.jobId} ${job.state}`).join(', ')
  return { action: 'queue', ok: true, summary, jobs }
}

/**
 * Locate one job's log file on the cluster.
 * @param {object} env - execution environment.
 * @param {string} jobId - Slurm job id.
 * @param {'out'|'err'} stream - stdio stream.
 * @param {string} [explicit] - caller-supplied remote path.
 * @returns {Promise<string>} remote path, or an empty string when unknown.
 */
async function resolveLogPath(env, jobId, stream, explicit) {
  if (typeof explicit === 'string' && explicit.trim() !== '') return explicit.trim()
  const record = findJob(env.config, jobId)
  if (record !== undefined) return joinRemote(record.directory, `slurm-${jobId}.${stream}`)
  const search = await execRemote(
    env,
    `find ${remotePathExpr(env.config.remoteRoot)} -maxdepth 2 -name ${shellQuote(`slurm-${jobId}.${stream}`)} -print -quit 2>/dev/null`,
    { timeoutMs: 30_000, what: 'log lookup', tolerate: true },
  )
  return search.stdout.split('\n').map(line => line.trim()).filter(Boolean)[0] ?? ''
}

/**
 * Tail a job's output or error log.
 * @param {object} env - execution environment.
 * @param {object} args - tool arguments.
 * @returns {Promise<object>} canonical result.
 */
async function logs(env, args) {
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
  const text = outcome.stdout
  return {
    action: 'logs',
    ok: true,
    summary: `Last ${lines} line${lines === 1 ? '' : 's'} of ${path}`,
    jobId,
    path,
    stream,
    lines,
    text,
    truncated: outcome.truncated.out,
  }
}

/**
 * Cancel one queued or running job.
 * @param {object} env - execution environment.
 * @param {object} args - tool arguments.
 * @returns {Promise<object>} canonical result.
 */
async function cancel(env, args) {
  const jobId = String(args.jobId).trim()
  await execRemote(env, `scancel ${shellQuote(jobId)}`, { timeoutMs: 30_000, what: `scancel ${jobId}` })
  updateJob(env.config, jobId, { cancelledAt: new Date().toISOString() })
  return {
    action: 'cancel',
    ok: true,
    summary: `Cancelled job ${jobId}`,
    jobId,
  }
}

/**
 * Run one shell command on the login node.
 * @param {object} env - execution environment.
 * @param {object} args - tool arguments.
 * @returns {Promise<object>} canonical result.
 */
async function exec(env, args) {
  const command = String(args.command ?? '')
  if (command.trim() === '') throw new Error('exec needs `command`')
  const requested = Number(args.timeoutMs)
  const timeoutMs = Number.isFinite(requested) ? Math.min(600_000, Math.max(1000, requested)) : env.config.commandTimeoutMs
  const outcome = await execRemote(env, `bash -lc ${shellQuote(command)}`, {
    timeoutMs,
    tolerate: true,
    what: 'remote command',
  })
  const code = outcome.code ?? 0
  return {
    action: 'exec',
    ok: code === 0,
    summary: code === 0 ? 'Command finished successfully' : `Command exited with code ${code}`,
    command,
    exitCode: code,
    stdout: outcome.stdout,
    stderr: outcome.stderr,
    truncated: outcome.truncated.out || outcome.truncated.err,
  }
}

/**
 * Copy one local file or directory to the cluster.
 * @param {object} env - execution environment.
 * @param {object} args - tool arguments.
 * @returns {Promise<object>} canonical result.
 */
async function upload(env, args) {
  const source = localPath(env.cwd, args.localPath)
  if (!existsSync(source)) throw new Error(`Local path not found: ${source}`)
  const info = statSync(source)
  if (!info.isFile() && !info.isDirectory()) throw new Error(`Local path is neither file nor directory: ${source}`)
  const recursive = args.recursive === true || info.isDirectory()
  await runScp(env, { direction: 'upload', localPath: source, remotePath: args.remotePath, recursive })
  return {
    action: 'upload',
    ok: true,
    summary: `Uploaded ${source} to ${describeTarget(env.config)}:${args.remotePath}`,
    localPath: source,
    remotePath: args.remotePath,
    bytes: info.isFile() ? info.size : undefined,
    recursive,
  }
}

/**
 * Copy one remote file or directory to the local machine.
 * @param {object} env - execution environment.
 * @param {object} args - tool arguments.
 * @returns {Promise<object>} canonical result.
 */
async function download(env, args) {
  const destination = localPath(env.cwd, args.localPath)
  if (args.localPath.endsWith('/') || args.localPath.endsWith('\\')) mkdirSync(destination, { recursive: true })
  else mkdirSync(dirname(destination), { recursive: true })
  await runScp(env, { direction: 'download', localPath: destination, remotePath: args.remotePath, recursive: args.recursive === true })
  return {
    action: 'download',
    ok: true,
    summary: `Downloaded ${describeTarget(env.config)}:${args.remotePath} to ${destination}`,
    localPath: destination,
    remotePath: args.remotePath,
  }
}

/** Actions that require a job id. */
const NEEDS_JOB_ID = new Set(['cancel', 'logs'])

/**
 * Dispatch one tool call.
 * @param {object} env - execution environment.
 * @param {object} args - validated tool arguments.
 * @returns {Promise<object>} canonical result value.
 */
export async function runAction(env, args) {
  const action = String(args.action ?? '')
  if (NEEDS_JOB_ID.has(action) && (args.jobId === undefined || String(args.jobId).trim() === '')) {
    throw new Error(`action "${action}" needs \`jobId\``)
  }
  if (action === 'status' && (args.jobId === undefined || String(args.jobId).trim() === '')) return queue(env)
  switch (action) {
    case 'check': return check(env)
    case 'submit': return submit(env, args)
    case 'status': return status(env, args)
    case 'queue': return queue(env)
    case 'logs': return logs(env, args)
    case 'cancel': return cancel(env, args)
    case 'exec': return exec(env, args)
    case 'upload': return upload(env, args)
    case 'download': return download(env, args)
    default: throw new Error(`Unknown action "${action}". Use check, submit, status, queue, logs, cancel, exec, upload, or download.`)
  }
}
