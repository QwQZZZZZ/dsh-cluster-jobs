#!/usr/bin/env node
/**
 * Development helper: write one recorded mock SSH command to a shell file so a
 * real POSIX shell can validate the generated quoting.
 *
 * Usage: node tests/extract.mjs <1-based call index> <output file>
 */
import { readFileSync, writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
const index = Number(process.argv[2] ?? '1')
const target = process.argv[3] ?? join(here, '.mock', `command-${index}.sh`)
const lines = readFileSync(join(here, '.mock', 'ssh-commands.log'), 'utf8').trim().split('\n')
const argv = JSON.parse(lines[index - 1])
writeFileSync(target, argv[argv.length - 1], 'utf8')
console.log(`${target}: ${argv[argv.length - 1].length} bytes`)
