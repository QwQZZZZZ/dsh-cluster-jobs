/**
 * Connection configuration for the cluster-jobs plugin.
 *
 * The configuration file is user-owned JSON so credentials never live in the
 * DSH profile patch. Precedence: explicit patch config fields, then the first
 * existing configuration file, then built-in defaults.
 */
import { existsSync, readFileSync } from 'node:fs'
import { homedir } from 'node:os'
import { isAbsolute, join, resolve } from 'node:path'

/** Built-in defaults, overridden by the configuration file and patch config. */
export const DEFAULTS = Object.freeze({
  /** SSH host name or address of the cluster login node. */
  host: '',
  /** Remote login account. */
  user: '',
  /** SSH port. */
  port: 22,
  /** Authentication mechanism: a private key or a stored password. */
  authMode: 'key',
  /** Private key used for authentication; empty lets the SSH client choose. */
  identityFile: '',
  /** File whose first line is the account password; used when authMode is password. */
  passwordFile: '',
  /** Local SSH client executable. */
  sshBinary: 'ssh',
  /** Local scp executable. */
  scpBinary: 'scp',
  /** Extra arguments placed before the SSH destination (wrappers, jump hosts). */
  sshPrefixArgs: [],
  /** Extra arguments placed before the scp paths. */
  scpPrefixArgs: [],
  /** Remote directory that receives submitted job directories. */
  remoteRoot: '~/dsh-jobs',
  /** TCP connect timeout passed to the SSH client. */
  connectTimeoutSeconds: 15,
  /** Default deadline for one remote command. */
  commandTimeoutMs: 90_000,
  /** Host key policy passed to the SSH client. */
  knownHostsPolicy: 'accept-new',
  /** Local directory for the job registry; empty means `<DSH_HOME>/cluster-jobs`. */
  stateDir: '',
  /** Append every remote command and its outcome to `commands.log`. */
  debug: false,
  /** Scheduler on the cluster: `slurm` (sbatch) or `pbs` (qsub). */
  scheduler: 'slurm',
  /** Default SLURM submission options merged under per-call overrides. */
  slurm: {
    partition: '',
    qos: '',
    account: '',
    time: '',
    gpus: '',
    cpusPerTask: '',
    mem: '',
    nodes: '',
    extra: [],
  },
  /** Default PBS Professional submission options merged under per-call overrides. */
  pbs: {
    queue: '',
    project: '',
    walltime: '',
    ngpus: '',
    ncpus: '',
    mem: '',
    select: '',
    extra: [],
  },
})

/**
 * Expand a leading `~` in a local path.
 * @param {string} value - possibly home-relative local path.
 * @returns {string} absolute local path.
 */
export function expandHome(value) {
  if (value === '~') return homedir()
  if (value.startsWith('~/') || value.startsWith('~\\')) return join(homedir(), value.slice(2))
  return value
}

/** Directory holding DSH user state. */
function dshHome() {
  const configured = process.env.DSH_HOME?.trim()
  return configured ? configured : join(homedir(), '.dsh')
}

/**
 * Candidate configuration files, in precedence order.
 * @param {{ configFile?: string }} patchConfig - plugin row configuration.
 * @param {string} [cwd] - session working directory for the workspace file.
 * @returns {string[]} absolute paths to test.
 */
export function configCandidates(patchConfig, cwd = process.cwd()) {
  const candidates = []
  if (typeof patchConfig?.configFile === 'string' && patchConfig.configFile.trim() !== '') {
    candidates.push(expandHome(patchConfig.configFile.trim()))
  }
  const fromEnv = process.env.DSH_CLUSTER_CONFIG?.trim()
  if (fromEnv) candidates.push(expandHome(fromEnv))
  candidates.push(join(cwd, '.dsh-cluster.json'))
  candidates.push(join(dshHome(), 'cluster.json'))
  return candidates.map(path => (isAbsolute(path) ? path : resolve(cwd, path)))
}

/**
 * Read the first existing configuration file.
 * @param {{ configFile?: string }} patchConfig - plugin row configuration.
 * @param {string} [cwd] - session working directory for the workspace file.
 * @returns {{ values: object, source: string, problems: string[] }} file contents and their path.
 */
export function readConfigFile(patchConfig, cwd = process.cwd()) {
  for (const path of configCandidates(patchConfig, cwd)) {
    if (!existsSync(path)) continue
    try {
      const parsed = JSON.parse(readFileSync(path, 'utf8'))
      if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
        return { values: {}, source: path, problems: [`${path} must contain a JSON object`] }
      }
      return { values: parsed, source: path, problems: [] }
    } catch (error) {
      return { values: {}, source: path, problems: [`Cannot read ${path}: ${error.message}`] }
    }
  }
  return { values: {}, source: '', problems: [] }
}

/**
 * Merge defaults, configuration file and patch fields into one effective config.
 * @param {object} patchConfig - plugin row configuration.
 * @param {{ cwd?: string }} [context] - session working directory for the workspace file.
 * @returns {{ config: object, source: string, problems: string[] }} effective settings.
 */
export function loadConfig(patchConfig = {}, context = {}) {
  const file = readConfigFile(patchConfig, context.cwd)
  const problems = [...file.problems]
  const config = {
    ...DEFAULTS,
    ...file.values,
    ...stripConfigFile(patchConfig),
    slurm: { ...DEFAULTS.slurm, ...(file.values.slurm ?? {}), ...(patchConfig.slurm ?? {}) },
    pbs: { ...DEFAULTS.pbs, ...(file.values.pbs ?? {}), ...(patchConfig.pbs ?? {}) },
  }
  if (!Array.isArray(config.sshPrefixArgs)) {
    problems.push('sshPrefixArgs must be an array of strings')
    config.sshPrefixArgs = []
  }
  if (!Array.isArray(config.scpPrefixArgs)) {
    problems.push('scpPrefixArgs must be an array of strings')
    config.scpPrefixArgs = []
  }
  if (!Array.isArray(config.slurm.extra)) {
    problems.push('slurm.extra must be an array of strings')
    config.slurm.extra = []
  }
  if (!Array.isArray(config.pbs.extra)) {
    problems.push('pbs.extra must be an array of strings')
    config.pbs.extra = []
  }
  config.port = Number(config.port)
  config.connectTimeoutSeconds = Number(config.connectTimeoutSeconds)
  config.commandTimeoutMs = Number(config.commandTimeoutMs)
  for (const [key, value] of [['port', config.port], ['connectTimeoutSeconds', config.connectTimeoutSeconds], ['commandTimeoutMs', config.commandTimeoutMs]]) {
    if (!Number.isFinite(value) || value <= 0) problems.push(`${key} must be a positive number`)
  }
  if (!config.stateDir) config.stateDir = join(dshHome(), 'cluster-jobs')
  else config.stateDir = expandHome(config.stateDir)
  return { config, source: file.source, problems }
}

/**
 * Remove keys that only steer configuration loading.
 * @param {object} value - plugin row configuration.
 * @returns {object} fields that override the configuration file.
 */
function stripConfigFile(value) {
  const { configFile: _ignored, slurm: _slurm, pbs: _pbs, clusters: _clusters, ...rest } = value ?? {}
  return rest
}

/**
 * Describe why a connection cannot be attempted.
 * @param {object} config - effective configuration.
 * @returns {string[]} human-readable problems, empty when usable.
 */
export function connectionProblems(config) {
  const problems = []
  if (!config.host) problems.push('host is empty')
  if (!config.user) problems.push('user is empty')
  if (config.host && /[\s]/.test(config.host)) problems.push('host must not contain spaces')
  if (config.user && /[\s@]/.test(config.user)) problems.push('user must not contain spaces or @')
  if (config.authMode !== 'key' && config.authMode !== 'password') {
    problems.push('authMode must be "key" or "password"')
  }
  if (config.authMode === 'password') {
    if (!config.passwordFile) problems.push('passwordFile is empty for authMode "password"')
    else if (!existsSync(expandHome(config.passwordFile))) problems.push(`passwordFile does not exist: ${config.passwordFile}`)
  }
  if (config.identityFile && !existsSync(expandHome(config.identityFile))) {
    problems.push(`identityFile does not exist: ${config.identityFile}`)
  }
  return problems
}
