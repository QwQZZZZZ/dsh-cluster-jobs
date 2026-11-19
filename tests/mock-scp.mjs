#!/usr/bin/env node
/**
 * Mock scp client used by the offline test harness.
 *
 * Uploads are only recorded. Downloads write a small file at the local target so
 * the plugin's parent-directory handling is exercised.
 */
import { appendFileSync, mkdirSync, writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
const logDir = join(here, '.mock')
mkdirSync(logDir, { recursive: true })

const argv = process.argv.slice(2)
const first = argv[argv.length - 2] ?? ''
const second = argv[argv.length - 1] ?? ''
appendFileSync(join(logDir, 'scp-commands.log'), `${JSON.stringify(argv)}\n`, 'utf8')

/** Whether one scp operand names a remote destination. */
const isRemote = (value) => /@[^:]+:/.test(value)

if (isRemote(second) && !isRemote(first)) {
  // Upload: local source, remote destination.
  process.exit(0)
}

if (isRemote(first)) {
  // Download: remote source, local destination.
  try {
    writeFileSync(second, 'downloaded payload\n', 'utf8')
    process.exit(0)
  } catch (error) {
    process.stderr.write(`${error.message}\n`)
    process.exit(1)
  }
}

process.stderr.write('mock-scp: unrecognized arguments\n')
process.exit(1)
