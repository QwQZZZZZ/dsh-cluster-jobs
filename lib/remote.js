/**
 * SSH and scp command construction for the cluster-jobs plugin.
 *
 * Options are passed as separate argv entries, so neither the account name nor
 * the remote command is ever reinterpreted by a local shell.
 */
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { expandHome } from './config.js'
import { runProcess } from './process.js'

/** Helper that feeds the stored password to the SSH client on request. */
const ASKPASS = join(dirname(fileURLToPath(import.meta.url)), 'askpass.cmd')

/**
 * Whether the connection authenticates with a password file.
 * @param {object} config - effective configuration.
 * @returns {boolean} true for password mode.
 */
export function usesPassword(config) {
  return config.authMode === 'password'
}

/**
 * Environment for the SSH and scp clients.
 * @param {object} config - effective configuration.
 * @returns {Record<string, string>|undefined} environment, or undefined to inherit.
 */
export function sshEnv(config) {
  if (!usesPassword(config)) return undefined
  const file = expandHome(config.passwordFile)
  return {
    ...process.env,
    SSH_ASKPASS: ASKPASS,
    SSH_ASKPASS_REQUIRE: 'force',
    DISPLAY: process.env.DISPLAY ?? 'localhost:0',
    // `type` in the askpass batch file treats a forward-slash path as a switch.
    DSH_CLUSTER_ASKPASS_FILE: process.platform === 'win32' ? file.replaceAll('/', '\\') : file,
  }
}

/**
 * Authentication arguments for one SSH-family invocation.
 * @param {object} config - effective configuration.
 * @returns {string[]} client options.
 */
function authArgs(config) {
  if (usesPassword(config)) {
    return [
      '-o', 'BatchMode=no',
      '-o', 'PreferredAuthentications=password',
      '-o', 'PubkeyAuthentication=no',
      '-o', 'NumberOfPasswordPrompts=1',
    ]
  }
  const args = ['-o', 'BatchMode=yes']
  if (config.identityFile) args.push('-o', 'IdentitiesOnly=yes', '-i', expandHome(config.identityFile))
  return args
}

/** Failure reported by one remote command. */
export class RemoteError extends Error {
  /**
   * @param {string} message - model-facing explanation.
   * @param {{ command?: string, code?: number|null, stdout?: string, stderr?: string }} [details] - command outcome.
   */
  constructor(message, details = {}) {
    super(message)
    this.name = 'RemoteError'
    this.command = details.command
    this.code = details.code
    this.stdout = details.stdout ?? ''
    this.stderr = details.stderr ?? ''
  }
}

/**
 * Quote one value for a POSIX shell.
 * @param {string} value - raw value.
 * @returns {string} single-quoted shell word.
 */
export function shellQuote(value) {
  return `'${String(value).replaceAll("'", "'\\''")}'`
}

/**
 * Render a remote path so that a leading `~` still expands.
 * @param {string} path - remote absolute or home-relative path.
 * @returns {string} shell expression for the path.
 */
export function remotePathExpr(path) {
  const value = String(path)
  if (value === '~') return '"$HOME"'
  if (value.startsWith('~/')) return `"$HOME"/${shellQuote(value.slice(2))}`
  if (value.startsWith('/')) return shellQuote(value)
  // Relative paths still work after a `cd`, but quoting keeps spaces intact.
  return shellQuote(value)
}

/**
 * Join a remote directory and a child name.
 * @param {string} directory - parent path.
 * @param {string} name - child name.
 * @returns {string} joined path using POSIX separators.
 */
export function joinRemote(directory, name) {
  return `${String(directory).replace(/\/+$/, '')}/${name}`
}

/**
 * Build SSH client arguments for one remote command.
 * @param {object} config - effective connection configuration.
 * @param {string} remoteCommand - command executed by the remote shell.
 * @returns {string[]} arguments for the SSH client.
 */
export function sshArgs(config, remoteCommand) {
  const args = [
    '-o', `ConnectTimeout=${config.connectTimeoutSeconds}`,
    '-o', `StrictHostKeyChecking=${config.knownHostsPolicy}`,
    '-o', 'LogLevel=ERROR',
    '-p', String(config.port),
    ...authArgs(config),
  ]
  args.push(`${config.user}@${config.host}`, remoteCommand)
  return args
}

/**
 * Build scp client arguments for one transfer.
 * @param {object} config - effective connection configuration.
 * @param {{ direction: 'upload'|'download', localPath: string, remotePath: string, recursive?: boolean }} transfer - paths and direction.
 * @returns {string[]} arguments for the scp client.
 */
export function scpArgs(config, transfer) {
  const args = [
    '-o', `ConnectTimeout=${config.connectTimeoutSeconds}`,
    '-o', `StrictHostKeyChecking=${config.knownHostsPolicy}`,
    '-o', 'LogLevel=ERROR',
    '-P', String(config.port),
    ...authArgs(config),
  ]
  if (transfer.recursive) args.push('-r')
  const destination = `${config.user}@${config.host}:${transfer.remotePath}`
  if (transfer.direction === 'upload') args.push(transfer.localPath, destination)
  else args.push(destination, transfer.localPath)
  return args
}

/** Short description of the configured login target. */
export function describeTarget(config) {
  const port = config.port === 22 ? '' : `:${config.port}`
  return `${config.user}@${config.host}${port}`
}

/**
 * Explain a failed local process without echoing the command line.
 * @param {object} result - outcome of {@link runProcess}.
 * @param {object} config - effective connection configuration.
 * @param {string} what - operation being attempted.
 * @returns {string} explanation.
 */
function explainFailure(result, config, what) {
  if (result.spawnError) {
    return `Cannot start "${config.sshBinary}": ${result.spawnError.message}. Set sshBinary to the SSH client path.`
  }
  if (result.timedOut) {
    return `${what} timed out connecting to ${describeTarget(config)}. Check the VPN, host, and port.`
  }
  if (result.aborted) return `${what} was cancelled.`
  if (result.code === 255 && /Permission denied/i.test(result.stderr)) {
    const mode = usesPassword(config)
      ? `passwordFile ${config.passwordFile}`
      : `key ${config.identityFile || '(SSH default)'}`
    return `${what} was denied by ${describeTarget(config)}. Check the account and the ${mode}, `
      + 'or set authMode to "password" with a passwordFile.'
  }
  return ''
}

/**
 * Run one remote command and require a zero exit status.
 * @param {{ config: object, signal?: AbortSignal, debug?: Function }} env - execution environment.
 * @param {string} command - remote shell command.
 * @param {{ timeoutMs?: number, tolerate?: boolean, what?: string }} [options] - limits and tolerance.
 * @returns {Promise<{ stdout: string, stderr: string, code: number|null, truncated: object }>} outcome.
 */
export async function execRemote(env, command, options = {}) {
  const { config, signal } = env
  const what = options.what ?? 'Remote command'
  const args = [...config.sshPrefixArgs, ...sshArgs(config, command)]
  const result = await runProcess(config.sshBinary, args, {
    signal,
    timeoutMs: options.timeoutMs ?? config.commandTimeoutMs,
    maxStdout: options.maxStdout,
    maxStderr: options.maxStderr,
    env: sshEnv(config),
  })
  env.debug?.({ command, result })

  const transportFailure = explainFailure(result, config, what)
  if (transportFailure) throw new RemoteError(transportFailure, { command, code: result.code, stderr: result.stderr })
  if (result.code !== 0 && !options.tolerate) {
    const detail = result.stderr.trim() || result.stdout.trim() || `exit code ${result.code}`
    throw new RemoteError(`${what} failed on ${describeTarget(config)}: ${detail}`, {
      command, code: result.code, stdout: result.stdout, stderr: result.stderr,
    })
  }
  return result
}

/**
 * Run one local-to-remote or remote-to-local copy.
 * @param {{ config: object, signal?: AbortSignal, debug?: Function }} env - execution environment.
 * @param {{ direction: 'upload'|'download', localPath: string, remotePath: string, recursive?: boolean }} transfer - transfer request.
 * @returns {Promise<void>} resolves when scp exits successfully.
 */
export async function runScp(env, transfer) {
  const { config, signal } = env
  const args = [...config.scpPrefixArgs, ...scpArgs(config, transfer)]
  const result = await runProcess(config.scpBinary, args, {
    signal,
    timeoutMs: config.commandTimeoutMs,
    env: sshEnv(config),
  })
  env.debug?.({ command: `${transfer.direction} ${transfer.localPath} <-> ${transfer.remotePath}`, result })
  const what = `${transfer.direction === 'upload' ? 'Upload' : 'Download'}`
  const transportFailure = explainFailure(result, config, what)
  if (transportFailure) throw new RemoteError(transportFailure, { code: result.code, stderr: result.stderr })
  if (result.code !== 0) {
    const detail = result.stderr.trim() || result.stdout.trim() || `exit code ${result.code}`
    throw new RemoteError(`${what} failed on ${describeTarget(config)}: ${detail}`, {
      code: result.code, stdout: result.stdout, stderr: result.stderr,
    })
  }
}
