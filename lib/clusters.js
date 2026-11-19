/**
 * Cluster selection for the cluster-jobs plugin.
 *
 * A configuration file may describe one cluster at the top level (the original
 * format) or several under `clusters`. Each entry carries its own connection,
 * authentication, and scheduler defaults.
 */
import { connectionProblems, DEFAULTS, expandHome } from './config.js'

/** Connection and transport fields that a cluster entry can override. */
const CONNECTION_KEYS = [
  'host',
  'user',
  'port',
  'authMode',
  'identityFile',
  'passwordFile',
  'sshBinary',
  'scpBinary',
  'sshPrefixArgs',
  'scpPrefixArgs',
  'remoteRoot',
  'connectTimeoutSeconds',
  'commandTimeoutMs',
  'knownHostsPolicy',
  'stateDir',
  'debug',
]

/** Supported schedulers. */
export const SCHEDULERS = ['slurm', 'pbs']

/**
 * Build one effective cluster entry.
 * @param {object} config - merged configuration.
 * @param {object} entry - cluster entry from the file, empty in single-cluster mode.
 * @param {string} name - cluster name.
 * @returns {object} effective cluster configuration.
 */
function mergeCluster(config, entry, name) {
  const cluster = { name }
  for (const key of CONNECTION_KEYS) cluster[key] = entry[key] ?? config[key] ?? DEFAULTS[key]
  cluster.scheduler = String(entry.scheduler ?? config.scheduler ?? DEFAULTS.scheduler).toLowerCase()
  cluster.slurm = { ...DEFAULTS.slurm, ...(config.slurm ?? {}), ...(entry.slurm ?? {}) }
  cluster.pbs = { ...DEFAULTS.pbs, ...(config.pbs ?? {}), ...(entry.pbs ?? {}) }
  cluster.stateDir = cluster.stateDir ? expandHome(cluster.stateDir) : config.stateDir
  return cluster
}

/**
 * Resolve every configured cluster and the default selection.
 * @param {object} config - merged configuration.
 * @returns {{ clusters: Record<string, object>, defaultCluster: string, problems: string[], clusterProblems: Record<string, string[]> }} resolution result.
 */
export function resolveClusters(config) {
  const problems = []
  const clusterProblems = {}
  const clusters = {}
  let defaultCluster = typeof config.defaultCluster === 'string' ? config.defaultCluster.trim() : ''
  const raw = config.clusters
  const multi = raw !== undefined && raw !== null

  if (multi && (typeof raw !== 'object' || Array.isArray(raw))) {
    problems.push('clusters must be an object keyed by cluster name')
  } else if (multi && Object.keys(raw).length > 0) {
    for (const [name, entry] of Object.entries(raw)) {
      if (entry === null || typeof entry !== 'object' || Array.isArray(entry)) {
        problems.push(`cluster "${name}" must be an object`)
        continue
      }
      clusters[name] = mergeCluster(config, entry, name)
    }
    if (Object.keys(clusters).length === 0) problems.push('clusters is empty')
    if (defaultCluster === '') defaultCluster = Object.keys(clusters)[0] ?? ''
    if (defaultCluster !== '' && clusters[defaultCluster] === undefined) {
      problems.push(`defaultCluster "${defaultCluster}" is not defined in clusters`)
    }
  } else {
    clusters.default = mergeCluster(config, {}, 'default')
    defaultCluster = 'default'
  }

  for (const [name, cluster] of Object.entries(clusters)) {
    const issues = []
    if (!SCHEDULERS.includes(cluster.scheduler)) {
      issues.push(`scheduler must be one of ${SCHEDULERS.join(', ')}`)
    }
    issues.push(...connectionProblems(cluster))
    clusterProblems[name] = issues
  }
  return { clusters, defaultCluster, problems, clusterProblems }
}

/**
 * Pick one cluster by name, or the configured default.
 * @param {{ clusters: Record<string, object>, defaultCluster: string }} resolved - resolution result.
 * @param {string|undefined} requested - requested cluster name.
 * @returns {object} the selected cluster configuration.
 * @throws {Error} when the name is unknown.
 */
export function selectCluster(resolved, requested) {
  const name = typeof requested === 'string' && requested.trim() !== '' ? requested.trim() : resolved.defaultCluster
  const cluster = resolved.clusters[name]
  if (cluster === undefined) {
    const available = Object.keys(resolved.clusters).join(', ')
    throw new Error(`Unknown cluster "${name}". Configured clusters: ${available}.`)
  }
  return cluster
}
