# Cluster jobs

A DeepSeek Harness bundle that registers one Host tool, `cluster_job`, for working
with remote HPC clusters over SSH: submit SLURM or PBS jobs, follow the queue,
read job output, cancel, and move files. One configuration can describe several
clusters, such as an on-prem Slurm cluster and NSCC ASPIRE 2A (PBS).

The Harness, the model, and the session stay on this machine. The plugin only
runs local `ssh`/`scp` processes, so nothing has to be installed on the cluster.

## Tool actions

| Action | Purpose |
|---|---|
| `check` | Verify connectivity, the login account, and the scheduler client tools. |
| `submit` | Upload an inline (`script`) or local (`scriptPath`) job script, run `sbatch --parsable` or `qsub`, record the job id and work directory. |
| `status` | One job from `squeue`/`sacct` (Slurm) or `qstat`/`qstat -x` (PBS). Omitting `jobId` lists your queue. |
| `queue` | All of your active jobs. |
| `logs` | Tail the job's stdout (`stream: "out"`) or stderr (`stream: "err"`), 20–2000 lines. |
| `cancel` | `scancel` (Slurm) or `qdel` (PBS) one job. |
| `exec` | Run one command on the login node through `bash -lc`. |
| `upload` / `download` | Copy files or directories between this machine and the cluster with `scp`. |

`submit` writes the script to `<remoteRoot>/<name>-<timestamp>/` and returns the
job id. SLURM writes `slurm-<id>.out`/`.err` there; PBS keeps its own
`Output_Path`/`Error_Path`, which the plugin reads from `qstat` right after
submission. `logs` resolves that path through the local registry at
`<stateDir>/jobs.json`; pass `path` to read any other file.

## Multiple clusters

```json
{
  "defaultCluster": "ccds",
  "clusters": {
    "ccds": {
      "scheduler": "slurm",
      "host": "slurm-cluster.example.edu",
      "user": "your-account",
      "authMode": "password",
      "passwordFile": "C:/Users/you/.dsh/cluster-password.txt",
      "remoteRoot": "~/dsh-jobs",
      "slurm": { "partition": "gpu-partition", "gpus": "1" }
    },
    "nscc": {
      "scheduler": "pbs",
      "host": "aspire2antu.nscc.sg",
      "user": "your-nscc-id",
      "authMode": "key",
      "identityFile": "C:/Users/you/.ssh/nscc_key",
      "sshPrefixArgs": ["-J", "you@ntu-jump.example.edu"],
      "scpPrefixArgs": ["-J", "you@ntu-jump.example.edu"],
      "remoteRoot": "~/dsh-jobs",
      "pbs": {
        "queue": "normal",
        "project": "11001234",
        "ngpus": "1",
        "ncpus": "14",
        "mem": "110gb",
        "walltime": "01:00:00"
      }
    }
  }
}
```

Pass `cluster` to choose one; it defaults to `defaultCluster`. A configuration
with no `clusters` key is still valid and behaves as a single cluster named
`default`.

The configuration file is read on every call, so a cluster entry can be added or
fixed without reloading the plugin.

## Configuration

The first existing path wins:

1. `configFile` in the plugin row configuration (Plugin Manager → this bundle → configuration).
2. `$DSH_CLUSTER_CONFIG`.
3. `./.dsh-cluster.json` relative to the Harness working directory.
4. `<DSH_HOME>/cluster.json` (normally `~/.dsh/cluster.json`).

See `cluster.config.example.json` for a complete multi-cluster file.

| Field | Default | Meaning |
|---|---|---|
| `host`, `user` | required | Login node and account. |
| `scheduler` | `slurm` | `slurm` (sbatch) or `pbs` (qsub). |
| `port` | `22` | SSH port. |
| `authMode` | `key` | `key` uses `identityFile` (or the agent); `password` reads `passwordFile`. |
| `identityFile` | client default | Private key. Required when the server does not accept an agent key. |
| `passwordFile` | empty | File whose first line is the account password; used by `authMode: "password"`. |
| `remoteRoot` | `~/dsh-jobs` | Parent directory for job directories. |
| `sshPrefixArgs` / `scpPrefixArgs` | `[]` | Extra client arguments, for example a jump host (`-J user@jump-host`). |
| `sshBinary` / `scpBinary` | `ssh` / `scp` | Local client executables, for a non-default install. |
| `knownHostsPolicy` | `accept-new` | `StrictHostKeyChecking` value. |
| `connectTimeoutSeconds` | `15` | TCP connect timeout. |
| `commandTimeoutMs` | `90000` | Deadline for one remote command. |
| `stateDir` | `<DSH_HOME>/cluster-jobs` | Local job registry and optional diagnostics. |
| `debug` | `false` | Append each remote command and outcome to `<stateDir>/commands.log`. |
| `slurm.*` / `pbs.*` | empty | Defaults merged under each `submit` call's `options`; `.extra` holds raw scheduler arguments. |

Per-call `options` override the scheduler defaults for one submission:

- SLURM: `partition`, `qos`, `account`, `time`, `gpus` (`--gres=gpu:N`), `gres`,
  `cpusPerTask`, `mem`, `nodes`, `dependency`, `output`, `error`, `exclusive`.
- PBS: `queue`, `project` (`-P`), `walltime`, `ngpus`, `ncpus`, `mem`, or a raw
  `select` such as `1:ngpus=2:ncpus=28`; `output` and `error` pass `-o`/`-e`.

PBS note: ASPIRE 2A requires a project code (`-P`) and an explicit `ngpus`; the
plugin composes `-l select=1:ngpus=…:ncpus=…:mem=…` from the options above.

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
  equivalent route. NSCC ASPIRE 2A for NTU users is reached through the NTU VPN
  and the NTU jump host, or through the NSCC VPN; put the jump host in
  `sshPrefixArgs` as `-J`.
- **Login node rules.** Only schedule and inspect work there; GPU work belongs in
  a submitted job.
- **One connection per action.** Each tool call opens and closes its own SSH
  connection, which keeps failures isolated but adds a little latency.
- **Bounded output.** stdout is capped at 120 KB and stderr at 24 KB per command;
  logs default to 200 lines. Truncation is reported in the result.
- **Secrets.** Keys and password files stay in their own files; the registry and
  logs contain only paths, job ids, and commands.

## Development

```
node tests/run.mjs
```

The test harness points `sshBinary`/`scpBinary` at mock clients under `tests/`
and exercises every action for both schedulers without a network: Slurm
(`sbatch`/`squeue`/`sacct`/`scancel`) and PBS (`qsub`/`qstat`/`qdel`), plus
multi-cluster selection. The mock logs every argv it receives.

`tests/bash-check.sh` replays the generated `check` and `submit` commands in a
real POSIX shell with a fake `sbatch`, verifying quoting, base64 round-tripping,
and per-option argv handling. It also confirms that CRLF script files become LF.

## Verification status

- `node tests/run.mjs`: 29 checks pass against mock clusters (Slurm, PBS, and
  multi-cluster dispatch).
- `bash tests/bash-check.sh` (WSL Ubuntu): generated commands run correctly and
  the uploaded script and sbatch arguments match.
- Verified live against a Slurm 24.11.5 GPU cluster (NVIDIA A40 nodes) over a
  VPN: `check`, `submit`, `status` (SACCT `COMPLETED 0:0`), `logs`, `queue`, and
  `upload`/`download` (SHA-256 match) with password authentication.
- PBS is covered by the mock suite only; a live ASPIRE 2A run still needs the
  NSCC network path, an NSCC account, and an authorized key.
