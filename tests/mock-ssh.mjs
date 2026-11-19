#!/usr/bin/env node
/**
 * Mock OpenSSH client used by the offline test harness.
 *
 * It receives the exact argv the plugin would give `ssh`, records it, and
 * answers the small set of commands the plugin issues.
 */
import { appendFileSync, mkdirSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
const logDir = join(here, '.mock')
mkdirSync(logDir, { recursive: true })

const argv = process.argv.slice(2)
const command = argv[argv.length - 1] ?? ''
const destination = argv[argv.length - 2] ?? ''
appendFileSync(join(logDir, 'ssh-commands.log'), `${JSON.stringify(argv)}\n`, 'utf8')
appendFileSync(join(logDir, 'askpass-env.log'), `${JSON.stringify({
  askpass: process.env.SSH_ASKPASS ?? '',
  require: process.env.SSH_ASKPASS_REQUIRE ?? '',
  passwordFile: process.env.DSH_CLUSTER_ASKPASS_FILE ?? '',
})}\n`, 'utf8')

const out = (text) => process.stdout.write(`${text}\n`)

if (destination.includes('unreachable')) {
  process.stderr.write('ssh: connect to host unreachable.example port 22: Connection timed out\n')
  process.exit(255)
}

const jobId = /squeue -h -j '(\d+)'/.exec(command)?.[1]

if (command.includes('hostname=')) {
  out('hostname=login01')
  out('user=testuser')
  out('home=/home/testuser')
  out('sbatch=/usr/bin/sbatch')
  out('sbatch-version=slurm 24.05.4')
  out('sinfo=slurm 24.05.4')
  out('qos=normal')
  process.exit(0)
}

if (command.includes('sbatch --parsable')) {
  out('424242;MGPU-TC2')
  process.exit(0)
}

if (jobId !== undefined) {
  if (jobId === '424242') {
    out('424242|train|RUNNING|00:12:33|MGPU-TC2|gpu-node07|1')
    process.exit(0)
  }
  if (jobId === '555') {
    process.exit(0)
  }
  process.stderr.write('slurm_load_jobs error: Invalid job id specified\n')
  process.exit(1)
}

if (command.includes('sacct -n -P -j')) {
  out('555|eval|COMPLETED|00:01:00|MGPU-TC2|gpu-node03|0:0')
  process.exit(0)
}

if (command.includes('squeue -h -u')) {
  out('424242|train|RUNNING|00:12:33|MGPU-TC2|gpu-node07|1')
  out('424243|eval|PENDING|0:00|MGPU-TC2|(Priority)|1')
  process.exit(0)
}

if (command.includes('tail -n')) {
  out('epoch 1 loss 2.31')
  out('epoch 2 loss 1.04')
  out('done')
  process.exit(0)
}

if (command.includes('scancel')) {
  process.exit(0)
}

if (command.includes("find ")) {
  out('/home/testuser/dsh-jobs/eval-1/slurm-555.out')
  process.exit(0)
}

if (command.includes('bash -lc')) {
  if (command.includes('fail')) {
    process.stderr.write('boom\n')
    process.exit(7)
  }
  out('hi')
  process.exit(0)
}

process.stderr.write(`mock-ssh: unhandled command: ${command}\n`)
process.exit(1)
