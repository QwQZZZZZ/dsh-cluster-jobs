# Cluster jobs

A DeepSeek Harness bundle that registers one Host tool, `cluster_job`, for working
with a remote GPU cluster over SSH: submit SLURM scripts, follow the queue, read
job output, cancel, and move files.

The Harness, the model, and the session stay on this machine. The plugin only
runs local `ssh`/`scp` processes, so nothing has to be installed on the cluster.

## Tool actions

| Action | Purpose |
|---|---|
| `check` | Verify connectivity, the login account, and the Slurm client tools. |
| `submit` | Upload an inline (`script`) or local (`scriptPath`) job script, run `sbatch --parsable`, record the job id and work directory. |
| `status` | One job from `squeue`, falling back to `sacct` for finished jobs. Omitting `jobId` lists your queue. |
| `queue` | All of your active jobs. |
| `logs` | Tail the job's stdout (`stream: "out"`) or stderr (`stream: "err"`), 20–2000 lines. |
| `cancel` | `scancel` one job. |
| `exec` | Run one command on the login node through `bash -lc`. |
| `upload` / `download` | Copy files or directories between this machine and the cluster with `scp`. |

`submit` writes the script to `<remoteRoot>/<name>-<timestamp>/`, sets
`--output`/`--error` to `slurm-%j.out`/`slurm-%j.err` in that directory, and
returns the job id. `logs` then finds that file through the local registry at
`<stateDir>/jobs.json`; pass `path` to read any other file.

## Configuration

Create one JSON file. The first existing path wins:

1. `configFile` in the plugin row configuration (Plugin Manager → this bundle → configuration).
2. `$DSH_CLUSTER_CONFIG`.
3. `./.dsh-cluster.json` relative to the Harness working directory.
4. `<DSH_HOME>/cluster.json` (normally `~/.dsh/cluster.json`).

Example configuration (also available as `cluster.config.example.json`):

```json
{
  "host": "cluster.example.edu",
  "user": "your-account",
  "port": 22,
  "identityFile": "C:/Users/you/.ssh/cluster_key",
  "remoteRoot": "~/dsh-jobs",
  "connectTimeoutSeconds": 15,
  "commandTimeoutMs": 90000,
  "slurm": {
    "partition": "gpu-partition",
    "qos": "normal",
    "gpus": "1",
    "time": "01:00:00"
  }
}
```

| Field | Default | Meaning |
|---|---|---|
| `host`, `user` | required | Login node and account. |
| `port` | `22` | SSH port. |
| `authMode` | `key` | `key` uses `identityFile` (or the agent); `password` reads `passwordFile`. |
| `identityFile` | client default | Private key. Required when the server does not accept an agent key. |
| `passwordFile` | empty | File whose first line is the account password; used by `authMode: "password"`. |
| `remoteRoot` | `~/dsh-jobs` | Parent directory for job directories. |
| `sshBinary` / `scpBinary` | `ssh` / `scp` | Local client executables, for a non-default install. |
| `sshPrefixArgs` / `scpPrefixArgs` | `[]` | Extra client arguments, for example a jump host (`-J`). |
| `knownHostsPolicy` | `accept-new` | `StrictHostKeyChecking` value. |
| `connectTimeoutSeconds` | `15` | TCP connect timeout. |
| `commandTimeoutMs` | `90000` | Deadline for one remote command. |
| `stateDir` | `<DSH_HOME>/cluster-jobs` | Local job registry and optional diagnostics. |
| `debug` | `false` | Append each remote command and outcome to `<stateDir>/commands.log`. |
| `slurm.*` | empty | Defaults merged under each `submit` call's `options`. `slurm.extra` is an array of raw `sbatch` arguments. |

Per-call `options` override `slurm.*` for one submission: `partition`, `qos`,
`account`, `time`, `gpus` (`--gres=gpu:N`), `gres`, `cpusPerTask`, `mem`,
`nodes`, `dependency`, `output`, `error`, `exclusive`, `extra`.

## Password accounts

Clusters that only accept a password can still be used non-interactively. Set
`authMode` to `password` and point `passwordFile` at a file whose first line is
the password (LF line ending, no trailing spaces):

```json
{
  "host": "cluster.example.edu",
  "user": "your-account",
  "authMode": "password",
  "passwordFile": "C:/Users/you/.dsh/cluster-password.txt"
}
```

The plugin passes the password to `ssh`/`scp` through `SSH_ASKPASS`
(`SSH_ASKPASS_REQUIRE=force`), so it never appears in a command line or in the
process list. The helper script is `lib/askpass.cmd`, and it only knows the file
path. Restrict that file to your account, for example:

```
icacls "%USERPROFILE%\.dsh\cluster-password.txt" /inheritance:r /grant:r "%USERNAME%:(R)"
```

A key is still preferable when the cluster accepts one: it can be revoked
individually and never sits on disk in plain text.

## Requirements and limits

- **Authentication.** `authMode: "key"` uses `BatchMode=yes`; `authMode:
  "password"` uses `SSH_ASKPASS` with a stored password. Password-only accounts
  work without a key.
- **Network access.** A private cluster address needs the campus VPN or an
  equivalent route.
- **Login node rules.** Only schedule and inspect work there; GPU work belongs in
  a submitted job.
- **One connection per action.** Each tool call opens and closes its own SSH
  connection, which keeps failures isolated but adds a little latency.
- **Bounded output.** stdout is capped at 120 KB and stderr at 24 KB per command;
  logs default to 200 lines. Truncation is reported in the result.
- **Secrets.** The key stays in its own file; the registry and logs contain only
  paths, job ids, and commands.
- **Live configuration.** The JSON file is read on every call, so editing it takes
  effect immediately without reloading the plugin or restarting the Harness.

## Development

```
node tests/run.mjs
```

The test harness points `sshBinary`/`scpBinary` at mock clients under `tests/`
and exercises check, submit, status, queue, logs, cancel, exec, upload, and
download without a network, so the SSH transport and Slurm parsing can be
verified offline. The mock accepts a scripted outcome file and logs every argv it
receives.

`tests/bash-check.sh` replays the generated `check` and `submit` commands in a
real POSIX shell with a fake `sbatch`, verifying quoting, base64 round-tripping,
and per-option argv handling. It also confirms that CRLF script files become LF.

## Verification status

- `node tests/run.mjs`: 20 checks pass against the mock cluster.
- `bash tests/bash-check.sh` (WSL Ubuntu): generated commands run correctly and
  the uploaded script and sbatch arguments match.
- Verified live against a Slurm 24.11.5 GPU cluster (NVIDIA A40 nodes) over a
  VPN: `check`, `submit`, `status` (SACCT `COMPLETED 0:0`), `logs`, `queue`, and
  `upload`/`download` (SHA-256 match) with password authentication.
- Not yet exercised on a second, independently configured cluster.
