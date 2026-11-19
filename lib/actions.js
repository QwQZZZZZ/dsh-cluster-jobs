/**
 * Cluster job actions for the cluster-jobs plugin.
 *
 * Transport actions (check, exec, upload, download) are scheduler-agnostic; job
 * actions dispatch to the SLURM or PBS backend selected by the cluster entry.
 */
import { existsSync, mkdirSync, statSync } from 'node:fs'
import { dirname } from 'node:path'
import { describeTarget, execRemote, runScp, shellQuote } from './remote.js'
import { localPath } from './scheduler/common.js'
import * as pbs from './scheduler/pbs.js'
import * as slurm from './scheduler/slurm.js'

/** Scheduler backends keyed by their configuration name. */
const BACKENDS = { slurm, pbs }

/** Job actions delegated to the scheduler backend. */
const SCHEDULED_ACTIONS = new Set(['submit', 'status', 'queue', 'logs', 'cancel'])

/**
 * Probe the login node for connectivity and scheduler tooling.
 * @param {{ cluster: object, source: string }} env - execution environment.
 * @returns {Promise<object>} canonical result.
 */
async function check(env) {
  const cluster = env.cluster
  const probe = [
    'printf "hostname=%s\\n" "$(hostname)"',
    'printf "user=%s\\n" "$(whoami)"',
    'printf "home=%s\\n" "$HOME"',
    'printf "sbatch=%s\\n" "$(command -v sbatch || echo missing)"',
    'printf "sbatch-version=%s\\n" "$(sbatch --version 2>/dev/null | head -1 || echo unknown)"',
    'printf "qsub=%s\\n" "$(command -v qsub || echo missing)"',
    'printf "qsub-version=%s\\n" "$(qsub --version 2>/dev/null | head -1 || echo unknown)"',
    'printf "sinfo=%s\\n" "$(sinfo -V 2>/dev/null || echo missing)"',
    'printf "qstat=%s\\n" "$(command -v qstat || echo missing)"',
    'printf "qos=%s\\n" "$(sacctmgr show user "$(whoami)" withassoc format=qos -n 2>/dev/null | head -1 | tr -d " " || echo unknown)"',
  ].join('; ')
  const outcome = await execRemote(env, probe, { timeoutMs: 30_000, what: 'Cluster probe' })
  const facts = {}
  for (const line of outcome.stdout.split('\n')) {
    const index = line.indexOf('=')
    if (index > 0) facts[line.slice(0, index).trim()] = line.slice(index + 1).trim()
  }
  const warnings = []
  if (cluster.scheduler === 'slurm') {
    if (facts.sbatch === 'missing' || facts.sbatch === '') {
      warnings.push('sbatch was not found on the login node')
      if (facts.qsub !== 'missing') warnings.push('qsub exists; this cluster may use PBS, so set scheduler to "pbs"')
    }
    if (facts.sinfo === 'missing') warnings.push('sinfo was not found; Slurm may not be installed as expected')
  } else {
    if (facts.qsub === 'missing' || facts.qsub === '') {
      warnings.push('qsub was not found on the login node')
      if (facts.sbatch !== 'missing') warnings.push('sbatch exists; this cluster may use Slurm, so set scheduler to "slurm"')
    }
  }
  const version = cluster.scheduler === 'pbs'
    ? facts['qsub-version']
    : facts['sbatch-version']
  const summary = `Connected to ${describeTarget(cluster)} as ${facts.user ?? cluster.user} on ${facts.hostname ?? 'unknown host'}`
    + ` via ${cluster.scheduler}`
    + (version && version !== 'unknown' ? ` (${version})` : '')
  return {
    action: 'check',
    ok: warnings.length === 0,
    summary,
    cluster: cluster.name,
    scheduler: cluster.scheduler,
    target: describeTarget(cluster),
    configSource: env.source || '(defaults only)',
    remoteRoot: cluster.remoteRoot,
    identityFile: cluster.identityFile || '',
    facts,
    warnings,
    stdout: outcome.stdout,
  }
}

/**
 * Run one shell command on the login node.
 * @param {{ cluster: object }} env - execution environment.
 * @param {object} args - tool arguments.
 * @returns {Promise<object>} canonical result.
 */
async function exec(env, args) {
  const command = String(args.command ?? '')
  if (command.trim() === '') throw new Error('exec needs `command`')
  const requested = Number(args.timeoutMs)
  const timeoutMs = Number.isFinite(requested) ? Math.min(600_000, Math.max(1000, requested)) : env.cluster.commandTimeoutMs
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
    cluster: env.cluster.name,
    command,
    exitCode: code,
    stdout: outcome.stdout,
    stderr: outcome.stderr,
    truncated: outcome.truncated.out || outcome.truncated.err,
  }
}

/**
 * Copy one local file or directory to the cluster.
 * @param {{ cluster: object }} env - execution environment.
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
    summary: `Uploaded ${source} to ${describeTarget(env.cluster)}:${args.remotePath}`,
    cluster: env.cluster.name,
    localPath: source,
    remotePath: args.remotePath,
    bytes: info.isFile() ? info.size : undefined,
    recursive,
  }
}

/**
 * Copy one remote file or directory to the local machine.
 * @param {{ cluster: object }} env - execution environment.
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
    summary: `Downloaded ${describeTarget(env.cluster)}:${args.remotePath} to ${destination}`,
    cluster: env.cluster.name,
    localPath: destination,
    remotePath: args.remotePath,
  }
}

/**
 * Pick the backend for the selected cluster.
 * @param {object} cluster - cluster configuration.
 * @returns {{ submit: Function, status: Function, queue: Function, logs: Function, cancel: Function }} backend.
 */
function backendOf(cluster) {
  const backend = BACKENDS[cluster.scheduler]
  if (backend === undefined) throw new Error(`Unsupported scheduler "${cluster.scheduler}" on cluster "${cluster.name}"`)
  return backend
}

/** Actions that require a job id. */
const NEEDS_JOB_ID = new Set(['cancel', 'logs'])

/**
 * Dispatch one tool call.
 * @param {{ cluster: object, clusters: object, cwd: string, signal?: AbortSignal }} env - execution environment.
 * @param {object} args - validated tool arguments.
 * @returns {Promise<object>} canonical result value.
 */
export async function runAction(env, args) {
  const action = String(args.action ?? '')
  if (NEEDS_JOB_ID.has(action) && (args.jobId === undefined || String(args.jobId).trim() === '')) {
    throw new Error(`action "${action}" needs \`jobId\``)
  }
  if (action === 'check') return check(env)
  if (action === 'exec') return exec(env, args)
  if (action === 'upload') return upload(env, args)
  if (action === 'download') return download(env, args)
  if (!SCHEDULED_ACTIONS.has(action)) {
    throw new Error(`Unknown action "${action}". Use check, submit, status, queue, logs, cancel, exec, upload, or download.`)
  }
  if (action === 'status' && (args.jobId === undefined || String(args.jobId).trim() === '')) {
    return backendOf(env.cluster).queue(env, args)
  }
  const backend = backendOf(env.cluster)
  const value = await backend[action](env, args)
  return { ...value, cluster: env.cluster.name, scheduler: env.cluster.scheduler }
}

/** Re-exported for the tool definition. */
export { SCHEDULED_ACTIONS }
