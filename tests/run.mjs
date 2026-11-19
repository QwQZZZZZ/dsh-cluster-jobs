#!/usr/bin/env node
/**
 * Offline verification for the cluster-jobs plugin.
 *
 * The harness replaces the SSH and scp clients with mocks, then drives the
 * registered tool through every action and asserts the parsed results.
 */
import assert from 'node:assert/strict'
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { apply } from '../index.js'

const here = dirname(fileURLToPath(import.meta.url))
const mockDir = join(here, '.mock')
rmSync(mockDir, { recursive: true, force: true })
mkdirSync(mockDir, { recursive: true })

const baseConfig = {
  host: 'test-cluster.example',
  user: 'testuser',
  port: 22,
  identityFile: '',
  sshBinary: process.execPath,
  sshPrefixArgs: [join(here, 'mock-ssh.mjs')],
  scpBinary: process.execPath,
  scpPrefixArgs: [join(here, 'mock-scp.mjs')],
  remoteRoot: '~/dsh-jobs',
  stateDir: join(mockDir, 'state'),
  debug: true,
  slurm: { partition: 'MGPU-TC2', qos: 'normal', gpus: '1', time: '00:10:00' },
}
writeFileSync(join(mockDir, 'config.json'), JSON.stringify(baseConfig, null, 2))

/**
 * Register the plugin against one configuration file.
 * @param {object} patch - extra plugin row configuration.
 * @param {string} configFile - configuration path.
 * @returns {object} the registered tool definition.
 */
function loadTool(patch = {}, configFile = join(mockDir, 'config.json')) {
  let registered
  const ctx = {
    tools: { register(definition) { registered = definition } },
    get() { return undefined },
  }
  apply(ctx, { configFile, ...patch })
  assert.ok(registered, 'the plugin must register one tool')
  return registered
}

let passed = 0
let failed = 0

/**
 * Run one async check and record the outcome.
 * @param {string} label - check name.
 * @param {Function} body - assertions.
 * @returns {Promise<void>} resolves after the check.
 */
async function test(label, body) {
  try {
    await body()
    passed += 1
    console.log(`PASS ${label}`)
  } catch (error) {
    failed += 1
    console.error(`FAIL ${label}\n  ${error.message}`)
  }
}

const tool = loadTool()
const signal = new AbortController().signal
const call = (args) => tool.execute(args, { signal })
const render = (value) => tool.output.render(undefined, value)[0].text

await test('check reports the remote environment', async () => {
  const value = await call({ action: 'check' })
  assert.equal(value.ok, true, JSON.stringify(value))
  assert.equal(value.facts.hostname, 'login01')
  assert.equal(value.facts.sbatch, '/usr/bin/sbatch')
  assert.match(value.summary, /login01/)
  assert.match(render(value), /user=testuser/)
})

await test('submit records the job id and work directory', async () => {
  const value = await call({
    action: 'submit',
    jobName: 'train',
    script: '#!/bin/bash\n#SBATCH -J train\npython train.py\n',
  })
  assert.equal(value.jobId, '424242')
  assert.match(value.directory, /^~\/dsh-jobs\/train-/)
  assert.equal(value.scriptPath, `${value.directory}/train.sbatch`)
  assert.match(render(value), /Submitted job 424242/)
  const registry = JSON.parse(readFileSync(join(baseConfig.stateDir, 'jobs.json'), 'utf8'))
  assert.ok(registry.jobs['test-cluster.example:424242'], 'registry must hold the submission')
})

await test('submit normalizes CRLF scripts and honours options', async () => {
  const scriptPath = join(mockDir, 'crlf.sbatch')
  writeFileSync(scriptPath, '#!/bin/bash\r\necho crlf\r\n', 'utf8')
  const value = await call({ action: 'submit', scriptPath, options: { partition: 'MGPU-TC2', time: '00:05:00' } })
  assert.equal(value.lineEndingsNormalized, true)
  assert.equal(value.origin, scriptPath)
  assert.equal(value.jobId, '424242')
  assert.match(value.summary, /MGPU-TC2/)
})

await test('status reads the active queue', async () => {
  const value = await call({ action: 'status', jobId: '424242' })
  assert.equal(value.job.state, 'RUNNING')
  assert.equal(value.job.nodes, 'gpu-node07')
  assert.match(value.summary, /RUNNING/)
})

await test('status falls back to accounting for finished jobs', async () => {
  const value = await call({ action: 'status', jobId: '555' })
  assert.equal(value.job.state, 'COMPLETED')
  assert.equal(value.job.exitCode, '0:0')
  assert.match(value.summary, /COMPLETED/)
})

await test('queue lists every active job', async () => {
  const value = await call({ action: 'queue' })
  assert.equal(value.jobs.length, 2)
  assert.match(render(value), /424243\tPENDING/)
})

await test('logs tails the recorded output file', async () => {
  const value = await call({ action: 'logs', jobId: '424242' })
  assert.match(value.text, /epoch 2 loss 1.04/)
  assert.equal(value.stream, 'out')
})

await test('logs finds an unregistered job through remoteRoot', async () => {
  const value = await call({ action: 'logs', jobId: '555', lines: 50 })
  assert.equal(value.path, '/home/testuser/dsh-jobs/eval-1/slurm-555.out')
  assert.equal(value.lines, 50)
})

await test('cancel scancel without a queue wait', async () => {
  const value = await call({ action: 'cancel', jobId: '424242' })
  assert.equal(value.ok, true)
  const registry = JSON.parse(readFileSync(join(baseConfig.stateDir, 'jobs.json'), 'utf8'))
  assert.ok(registry.jobs['test-cluster.example:424242'].cancelledAt)
})

await test('exec reports stdout and a successful exit', async () => {
  const value = await call({ action: 'exec', command: 'echo hi' })
  assert.equal(value.ok, true)
  assert.equal(value.exitCode, 0)
  assert.equal(value.stdout.trim(), 'hi')
})

await test('exec reports a failing exit code instead of throwing', async () => {
  const value = await call({ action: 'exec', command: 'run-fail' })
  assert.equal(value.ok, false)
  assert.equal(value.exitCode, 7)
  assert.match(value.stderr, /boom/)
})

await test('upload sends a local file', async () => {
  const source = join(mockDir, 'data.txt')
  writeFileSync(source, 'payload\n', 'utf8')
  const value = await call({ action: 'upload', localPath: source, remotePath: '~/dsh-jobs/data.txt' })
  assert.equal(value.bytes, 8)
  assert.match(value.summary, /Uploaded/)
})

await test('download creates the local parent directory', async () => {
  const target = join(mockDir, 'results', 'out.txt')
  const value = await call({ action: 'download', remotePath: '~/dsh-jobs/eval-1/slurm-555.out', localPath: target })
  assert.equal(value.localPath, target)
  assert.equal(readFileSync(target, 'utf8'), 'downloaded payload\n')
})

await test('password mode drives SSH_ASKPASS instead of a key', async () => {
  const passwordFile = join(mockDir, 'password.txt')
  writeFileSync(passwordFile, 'secret-password\n', 'utf8')
  const passwordConfig = join(mockDir, 'password.json')
  writeFileSync(passwordConfig, JSON.stringify({ ...baseConfig, authMode: 'password', passwordFile }), 'utf8')
  const withPassword = loadTool({}, passwordConfig)
  const value = await withPassword.execute({ action: 'check' }, { signal })
  assert.equal(value.ok, true)
  const argv = JSON.parse(readFileSync(join(mockDir, 'ssh-commands.log'), 'utf8').trim().split('\n').pop())
  assert.ok(argv.includes('BatchMode=no'), 'password mode must not short-circuit authentication')
  assert.ok(!argv.includes('BatchMode=yes'))
  assert.ok(argv.includes('PreferredAuthentications=password'))
  assert.ok(argv.includes('PubkeyAuthentication=no'))
  const env = JSON.parse(readFileSync(join(mockDir, 'askpass-env.log'), 'utf8').trim().split('\n').pop())
  assert.match(env.askpass, /askpass\.cmd$/)
  assert.equal(env.require, 'force')
  assert.equal(env.passwordFile, process.platform === 'win32' ? passwordFile.replaceAll('/', '\\') : passwordFile)
})

await test('password mode requires an existing password file', async () => {
  const missingConfig = join(mockDir, 'password-missing.json')
  writeFileSync(missingConfig, JSON.stringify({
    ...baseConfig,
    authMode: 'password',
    passwordFile: join(mockDir, 'nope.txt'),
  }), 'utf8')
  const missing = loadTool({}, missingConfig)
  await assert.rejects(() => missing.execute({ action: 'check' }, { signal }), /passwordFile does not exist/)
})

const multiConfigPath = join(mockDir, 'multi.json')
writeFileSync(multiConfigPath, JSON.stringify({
  defaultCluster: 'ccds',
  clusters: {
    ccds: { ...baseConfig, stateDir: join(mockDir, 'state-multi') },
    nscc: {
      host: 'pbs-cluster.example',
      user: 'testuser',
      scheduler: 'pbs',
      authMode: 'key',
      remoteRoot: '~/dsh-jobs',
      stateDir: join(mockDir, 'state-multi'),
      sshBinary: baseConfig.sshBinary,
      sshPrefixArgs: baseConfig.sshPrefixArgs,
      scpBinary: baseConfig.scpBinary,
      scpPrefixArgs: baseConfig.scpPrefixArgs,
      pbs: { queue: 'normal', project: '11001234', ngpus: '1', ncpus: '14', mem: '110gb', walltime: '00:05:00' },
    },
  },
}, null, 2))
const multi = loadTool({}, multiConfigPath)
const callMulti = (args) => multi.execute(args, { signal })

await test('the default cluster is used when none is named', async () => {
  const value = await callMulti({ action: 'check' })
  assert.equal(value.cluster, 'ccds')
  assert.equal(value.scheduler, 'slurm')
})

await test('check reports the PBS client on the nscc cluster', async () => {
  const value = await callMulti({ action: 'check', cluster: 'nscc' })
  assert.equal(value.cluster, 'nscc')
  assert.equal(value.scheduler, 'pbs')
  assert.equal(value.ok, true)
  assert.equal(value.facts.qsub, '/usr/local/bin/qsub')
})

await test('submit translates options into qsub flags', async () => {
  const value = await callMulti({
    action: 'submit',
    cluster: 'nscc',
    jobName: 'pbs-smoke',
    script: '#!/bin/bash\n#PBS -N pbs-smoke\necho hi\n',
  })
  assert.equal(value.jobId, '1852792.pbs101')
  assert.equal(value.scheduler, 'pbs')
  assert.match(value.directory, /^~\/dsh-jobs\/pbs-smoke-/)
  assert.match(value.summary, /Submitted job 1852792\.pbs101/)
  const logged = readFileSync(join(mockDir, 'ssh-commands.log'), 'utf8').trim().split('\n')
    .map(line => JSON.parse(line))
  const command = logged.map(argv => argv[argv.length - 1]).filter(text => text.includes('qsub ')).pop()
  assert.match(command, /qsub /)
  assert.match(command, /-N='pbs-smoke'/)
  assert.match(command, /-q='normal'/)
  assert.match(command, /-P='11001234'/)
  assert.match(command, /select=1:ngpus=1:ncpus=14:mem=110gb/)
  assert.match(command, /walltime=00:05:00/)
})

await test('pbs status reads a running job', async () => {
  const value = await callMulti({ action: 'status', cluster: 'nscc', jobId: '1852792.pbs101' })
  assert.equal(value.job.state, 'RUNNING')
  assert.equal(value.job.partition, 'normal')
  assert.match(value.summary, /RUNNING/)
})

await test('pbs status falls back to history for a finished job', async () => {
  const value = await callMulti({ action: 'status', cluster: 'nscc', jobId: '1852793.pbs101' })
  assert.equal(value.job.state, 'FINISHED')
  assert.equal(value.job.exitCode, '0')
  assert.match(value.summary, /FINISHED.*exit 0/)
})

await test('pbs logs use the output path reported by qstat', async () => {
  const value = await callMulti({ action: 'logs', cluster: 'nscc', jobId: '1852792.pbs101' })
  assert.equal(value.path, '/home/testuser/dsh-jobs/pbs-smoke.o1852792')
  assert.match(value.text, /epoch 2 loss 1.04/)
})

await test('pbs queue parses qstat rows', async () => {
  const value = await callMulti({ action: 'queue', cluster: 'nscc' })
  assert.equal(value.jobs.length, 1)
  assert.equal(value.jobs[0].jobId, '1852792.pbs101')
  assert.equal(value.jobs[0].state, 'RUNNING')
})

await test('pbs cancel uses qdel', async () => {
  const value = await callMulti({ action: 'cancel', cluster: 'nscc', jobId: '1852792.pbs101' })
  assert.equal(value.ok, true)
  assert.match(value.summary, /Cancelled job 1852792\.pbs101/)
})

await test('an unknown cluster name is rejected', async () => {
  await assert.rejects(() => callMulti({ action: 'check', cluster: 'nowhere' }), /Unknown cluster/)
})

await test('unknown actions are rejected', async () => {
  await assert.rejects(() => call({ action: 'explode' }), /Unknown action/)
})

await test('a missing job id is rejected for logs', async () => {
  await assert.rejects(() => call({ action: 'logs' }), /needs `jobId`/)
})

await test('an unconfigured host is rejected before connecting', async () => {
  const emptyConfig = join(mockDir, 'empty.json')
  writeFileSync(emptyConfig, JSON.stringify({ host: '', user: '' }), 'utf8')
  const unconfigured = loadTool({}, emptyConfig)
  await assert.rejects(() => unconfigured.execute({ action: 'check' }, { signal }), /not configured/)
})

await test('invalid JSON configuration is reported', async () => {
  const brokenConfig = join(mockDir, 'broken.json')
  writeFileSync(brokenConfig, '{ not json', 'utf8')
  const broken = loadTool({}, brokenConfig)
  await assert.rejects(() => broken.execute({ action: 'check' }, { signal }), /configuration is invalid/)
})

await test('an unreachable host surfaces the SSH error', async () => {
  const offlineConfig = join(mockDir, 'offline.json')
  writeFileSync(offlineConfig, JSON.stringify({
    ...baseConfig,
    host: 'unreachable.example',
    stateDir: join(mockDir, 'state-offline'),
  }), 'utf8')
  const offline = loadTool({}, offlineConfig)
  await assert.rejects(() => offline.execute({ action: 'check' }, { signal }), /Connection timed out|connect to host/)
})

const commands = existsSync(join(mockDir, 'ssh-commands.log'))
  ? readFileSync(join(mockDir, 'ssh-commands.log'), 'utf8').trim().split('\n').length
  : 0
console.log(`\n${passed} passed, ${failed} failed (${commands} mocked SSH calls)`)
if (failed > 0) process.exitCode = 1
