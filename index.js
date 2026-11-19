/**
 * Cluster jobs: submit and follow SLURM work on a remote GPU cluster over SSH.
 *
 * The plugin registers one Host tool and keeps every setting in a user-owned
 * JSON file so that nothing about the cluster (least of all a key) lives in the
 * DSH profile patch.
 */
import { runAction } from './lib/actions.js'
import { configCandidates, loadConfig } from './lib/config.js'
import { resolveClusters, selectCluster } from './lib/clusters.js'
import { debugLog } from './lib/state.js'

/** Stable Loader identity. */
export const name = 'cluster-jobs'

/** Services required before the tool can be registered. */
export const inject = ['tools']

/** Actions accepted by the single tool. */
const ACTIONS = ['check', 'submit', 'status', 'queue', 'logs', 'cancel', 'exec', 'upload', 'download']

/**
 * Render one canonical result for the model.
 * @param {object} value - canonical tool result.
 * @returns {string} model-facing text.
 */
function renderResult(value) {
  const parts = [String(value.summary ?? '')]
  if (value.cluster) parts.push(`cluster: ${value.cluster}${value.scheduler ? ` (${value.scheduler})` : ''}`)
  if (value.target) parts.push(`target: ${value.target}`)
  if (value.configSource) parts.push(`config: ${value.configSource}`)
  if (value.directory) parts.push(`remote directory: ${value.directory}`)
  if (value.scriptPath) parts.push(`script: ${value.scriptPath}`)
  if (value.path) parts.push(`path: ${value.path}`)

  if (Array.isArray(value.warnings) && value.warnings.length > 0) {
    parts.push(`warnings: ${value.warnings.join('; ')}`)
  }

  if (Array.isArray(value.jobs) && value.jobs.length > 0 && value.action !== 'status') {
    const header = 'jobId\tstate\telapsed\tpartition\tnodes\tname'
    const rows = value.jobs.map(job => [job.jobId, job.state, job.elapsed, job.partition, job.nodes, job.name].join('\t'))
    parts.push([header, ...rows].join('\n'))
  }

  if (value.action === 'check' && value.facts !== undefined) {
    const facts = Object.entries(value.facts).map(([key, fact]) => `${key}=${fact}`).join('\n')
    if (facts) parts.push(facts)
  }

  if (typeof value.text === 'string' && value.text.trim() !== '') parts.push(value.text.replace(/\s+$/, ''))
  if (typeof value.stdout === 'string' && value.stdout.trim() !== '' && value.action === 'exec') {
    parts.push(`stdout:\n${value.stdout.replace(/\s+$/, '')}`)
  }
  if (typeof value.stderr === 'string' && value.stderr.trim() !== '') {
    parts.push(`stderr:\n${value.stderr.replace(/\s+$/, '')}`)
  }
  if (value.truncated === true) parts.push('(output truncated)')
  if (value.action === 'submit' && value.jobId) {
    parts.push(`Next: cluster_job { action: "status", jobId: "${value.jobId}" } then { action: "logs", jobId: "${value.jobId}" }`)
  }
  return parts.filter(part => part !== '').join('\n')
}

/**
 * Resolve the session working directory for local paths.
 * @param {object} ctx - plugin context.
 * @param {object} exec - tool execution context.
 * @returns {Promise<string>} absolute local directory.
 */
async function sessionCwd(ctx, exec) {
  try {
    const workingDirectory = typeof ctx.get === 'function' ? ctx.get('workingDirectory') : undefined
    if (workingDirectory !== undefined && exec.agent !== undefined) {
      return await workingDirectory.ensure(exec.agent, exec.signal)
    }
  } catch {
    // Fall back to the Harness process directory.
  }
  return process.cwd()
}

/**
 * Register the cluster_job tool.
 * @param {object} ctx - Host plugin context.
 * @param {object} [patchConfig] - plugin row configuration.
 * @returns {void}
 */
export function apply(ctx, patchConfig = {}) {
  // Connection settings are read per call so editing the JSON file takes effect
  // without reloading the plugin.
  const tool = {
    name: 'cluster_job',
    description:
      'Submit and follow SLURM or PBS jobs on a configured remote cluster over SSH. '
      + 'Actions: check (connectivity and scheduler tooling), submit (upload `script` or `scriptPath` and run sbatch or qsub, returns a job id), '
      + 'status (one job; omit jobId to list the queue), queue (all your active jobs), logs (tail the job stdout or stderr), '
      + 'cancel, exec (run a shell command on the login node), upload and download (files between this machine and the cluster). '
      + 'Set `cluster` to choose between configured clusters; it defaults to defaultCluster. '
      + 'Remote paths are POSIX and may start with `~/`; local paths resolve against the session working directory. '
      + 'GPU work must be submitted, not started on the login node. '
      + 'Connection settings come from a JSON file (see the bundle README); an SSH key authorized on the cluster or a stored password, plus network access such as the campus VPN, are required.',
    parameters: {
      type: 'object',
      additionalProperties: false,
      properties: {
        action: {
          type: 'string',
          enum: ACTIONS,
          description: 'Operation to perform.',
        },
        cluster: {
          type: 'string',
          description: 'Configured cluster name; defaults to defaultCluster.',
        },
        jobId: {
          type: 'string',
          description: 'Job id: a Slurm numeric id or a PBS id such as 1852792.pbs101. Required by status, logs, and cancel.',
        },
        jobName: {
          type: 'string',
          description: 'Job name for submit; defaults to the script file name or "dsh-job".',
        },
        script: {
          type: 'string',
          description: 'Inline job script for submit, including any #SBATCH or #PBS lines.',
        },
        scriptPath: {
          type: 'string',
          description: 'Local job script for submit; ignored when script is given.',
        },
        remoteDir: {
          type: 'string',
          description: 'Remote directory that receives the script and output. Defaults to a timestamped directory under remoteRoot.',
        },
        options: {
          type: 'object',
          additionalProperties: true,
          description: 'Submission overrides for submit. SLURM: partition, qos, account, time, gpus, gres, cpusPerTask, mem, nodes, dependency, output, error, exclusive. PBS: queue, project, walltime, ngpus, ncpus, mem, select. Both: extra (array of raw scheduler arguments).',
          properties: {
            partition: { type: 'string' },
            qos: { type: 'string' },
            account: { type: 'string' },
            time: { type: 'string' },
            gpus: { type: 'string' },
            gres: { type: 'string' },
            cpusPerTask: { type: 'string' },
            cpus: { type: 'string' },
            mem: { type: 'string' },
            nodes: { type: 'string' },
            dependency: { type: 'string' },
            output: { type: 'string' },
            error: { type: 'string' },
            exclusive: { type: 'boolean' },
            queue: { type: 'string' },
            project: { type: 'string' },
            walltime: { type: 'string' },
            ngpus: { type: 'string' },
            ncpus: { type: 'string' },
            select: { type: 'string' },
            extra: { type: 'array', items: { type: 'string' } },
          },
        },
        lines: {
          type: 'integer',
          minimum: 20,
          maximum: 2000,
          description: 'Log lines returned by logs; default 200.',
        },
        stream: {
          type: 'string',
          enum: ['out', 'err'],
          description: 'Which job stream logs reads; default out.',
        },
        path: {
          type: 'string',
          description: 'Explicit remote log path for logs; defaults to the path recorded at submit time.',
        },
        command: {
          type: 'string',
          description: 'Shell command for exec, run through bash -lc on the login node.',
        },
        localPath: {
          type: 'string',
          description: 'Local file or directory for upload and download.',
        },
        remotePath: {
          type: 'string',
          description: 'Remote file or directory for upload and download.',
        },
        recursive: {
          type: 'boolean',
          description: 'Copy directories recursively.',
        },
        timeoutMs: {
          type: 'integer',
          description: 'Deadline for one exec command, in milliseconds.',
        },
      },
      required: ['action'],
    },
    output: {
      schema: { type: 'object' },
      render: (_args, value) => [{ type: 'text', text: renderResult(value ?? {}) }],
    },
    timeoutMs: 180_000,
    isConcurrencySafe: (args) => ['check', 'status', 'queue', 'logs'].includes(String(args?.action ?? '')),
    async execute(args, exec) {
      const cwd = await sessionCwd(ctx, exec)
      const loaded = loadConfig(patchConfig, { cwd })
      if (loaded.problems.length > 0) {
        throw new Error(`Cluster jobs configuration is invalid: ${loaded.problems.join('; ')}. Fix ${loaded.source || 'the configuration file'}.`)
      }
      const resolved = resolveClusters(loaded.config)
      if (resolved.problems.length > 0) {
        throw new Error(
          `Cluster jobs configuration is invalid: ${resolved.problems.join('; ')}. `
          + `Fix ${loaded.source || 'the configuration file'}.`,
        )
      }
      const cluster = selectCluster(resolved, args?.cluster)
      const clusterProblems = resolved.clusterProblems[cluster.name] ?? []
      if (clusterProblems.length > 0) {
        throw new Error(
          `Cluster "${cluster.name}" is not configured: ${clusterProblems.join('; ')}. `
          + `Fix that entry in ${loaded.source || 'the configuration file'} (searched: ${configCandidates(patchConfig, cwd).join(', ')}).`,
        )
      }
      const env = {
        config: cluster,
        cluster,
        clusters: resolved.clusters,
        source: loaded.source,
        cwd,
        signal: exec.signal,
        debug: (event) => debugLog(cluster, event),
      }
      return runAction(env, args ?? {})
    },
  }
  ctx.tools.register(tool)
}
