/**
 * Submission plumbing shared by the SLURM and PBS backends.
 */
import { existsSync, readFileSync, statSync } from 'node:fs'
import { basename, isAbsolute, resolve } from 'node:path'
import { expandHome } from '../config.js'
import { joinRemote, remotePathExpr, shellQuote } from '../remote.js'

/**
 * Resolve a local path against the session working directory.
 * @param {string} cwd - session working directory.
 * @param {string} value - possibly relative local path.
 * @returns {string} absolute local path.
 */
export function localPath(cwd, value) {
  const expanded = expandHome(String(value))
  return isAbsolute(expanded) ? expanded : resolve(cwd, expanded)
}

/** Timestamp component for a submission directory. */
export function submissionStamp() {
  return new Date().toISOString().replace(/\.\d+Z$/, '').replace('T', '-').replaceAll(':', '')
}

/**
 * Sanitize a job name for use as a directory and file component.
 * @param {string} value - proposed name.
 * @returns {string} safe name.
 */
export function safeJobName(value) {
  const cleaned = String(value).replace(/[^A-Za-z0-9._-]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 64)
  return cleaned || 'dsh-job'
}

/**
 * Join shell commands so that each one must succeed.
 * @param {string[]} parts - commands.
 * @returns {string} `&&`-joined command.
 */
export function sequence(parts) {
  return parts.join(' && ')
}

/**
 * Read and normalize the job script, then choose its directory and file name.
 * @param {{ cluster: object, cwd: string }} env - execution environment.
 * @param {object} args - tool arguments.
 * @param {string} extension - script extension without the dot.
 * @returns {{ content: string, origin: string, name: string, directory: string, scriptPath: string, lineEndingsChanged: boolean }} prepared submission.
 */
export function prepareSubmission(env, args, extension) {
  const { cluster, cwd } = env
  let content = ''
  let origin = 'inline script'
  if (typeof args.script === 'string' && args.script.trim() !== '') {
    content = args.script
  } else if (typeof args.scriptPath === 'string' && args.scriptPath.trim() !== '') {
    const path = localPath(cwd, args.scriptPath)
    if (!existsSync(path)) throw new Error(`Job script not found: ${path}`)
    if (!statSync(path).isFile()) throw new Error(`Job script is not a regular file: ${path}`)
    content = readFileSync(path, 'utf8')
    origin = path
  } else {
    throw new Error('submit needs `script` (inline) or `scriptPath` (local file)')
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
    : joinRemote(cluster.remoteRoot, `${name}-${submissionStamp()}`)
  const scriptPath = joinRemote(directory, `${name}.${extension}`)
  return { content: finalContent, origin, name, directory, scriptPath, lineEndingsChanged }
}

/**
 * Build the commands that create the job directory and upload the script.
 * @param {string} directory - remote job directory.
 * @param {string} scriptPath - remote script path.
 * @param {string} content - script text.
 * @returns {string[]} command fragments.
 */
export function writeScriptCommands(directory, scriptPath, content) {
  const encoded = Buffer.from(content, 'utf8').toString('base64')
  return [
    `mkdir -p ${remotePathExpr(directory)}`,
    `printf '%s' ${shellQuote(encoded)} | base64 -d > ${remotePathExpr(scriptPath)}`,
    `cd ${remotePathExpr(directory)}`,
  ]
}
