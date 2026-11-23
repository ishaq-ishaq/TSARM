# AWS Free plan: experiment E2 on an 8-node cluster

The AWS **Free plan** gives $100 of credit (or up to $200 if you complete the
console activities) for 6 months. On this plan:

- Only *free-tier-eligible* instance types can be launched. The largest is
  **`m7i-flex.large`** (2 vCPU, 8 GB).
- The account works in **one assigned region**. Ours is **Europe (Stockholm),
  `eu-north-1`**. Other regions show "Region unavailable" until the plan is
  upgraded.
- The vCPU quota *Running On-Demand Standard instances* (L-1216C47A) is
  **16** in `eu-north-1` (checked 2026-11-23). It is 5 in regions the plan
  cannot use.

Sixteen vCPUs give **8 × `m7i-flex.large`**: one node is the Spark master and
driver, and 7 are workers with 2 cores each. E2 therefore tests
**p = 1, 2, 4, 8, 14** executor cores. This is the largest E2 configuration
available to us, so AWS hosts the primary E2 run. The Wikidata series is
processed on Azure (`docs/azure.md`, Phase A).

| Item | Size | Estimated cost |
|---|---|---|
| 8 × `m7i-flex.large`, about 4-6 h | 8 × ~$0.10/h | ~$3-5 |
| 8 × 30 GB gp3 disks, public IPv4 addresses | | < $1 |
| **Total** | | **about $5 of the credit** |

> **Flex instances.** `m7i-flex` instances deliver their full CPU most of the
> time but are not guaranteed it at every moment. Report this with the E2
> results; the repeated runs and confidence intervals show the variation.

---

## How it runs

Everything is started from **AWS CloudShell** (the `>_` icon in the console's
top bar, with the console set to **Europe (Stockholm)**). Its home directory
persists between sessions, so the SSH key stays available.

`scripts/cluster/aws_e2_launch.sh launch` creates a key pair, a security group
(SSH from anywhere with the key only; all traffic between the nodes), the
master, then the 7 workers. Each node's user data clones this repository and
runs `scripts/cluster/aws_e2_node.sh`, which:

1. installs Java 17 and PySpark (`setup_node.sh`);
2. generates the deterministic synthetic datasets in `~/tsarm-data` (the
   12M-triple strong-scaling input of E1, and the 150k-entities-per-core
   weak-scaling inputs), identical on every node;
3. on a worker, registers with the master (2 cores, 5 GB) once its data exists;
4. on the master, waits for all 7 workers, then runs strong and weak scaling
   (`core_scaling.py --master-url ... --executor-memory 2g --tag aws`, driver
   4 GB), and packs `results/cluster_scaling_*` into `~/e2-results.tgz`.

If strong scaling at 12M triples fails (for example, out of memory at p = 1),
the master repeats it at 300,000 entities with the tag `aws_small`.

**Cost guards:** every node powers off after 24 hours whatever happens.
Workers power off and are terminated 10 minutes after the master finishes. The
master powers off 12 hours after finishing; a stopped master keeps its disk,
and therefore the results, until `cleanup`.

## Commands (in CloudShell)

```bash
curl -sLO https://raw.githubusercontent.com/ishaq-ishaq/TSARM/main/scripts/cluster/aws_e2_launch.sh
bash aws_e2_launch.sh launch      # about 1 minute; the nodes then work alone
bash aws_e2_launch.sh status      # node table + the master's last log lines
```

`status` shows `E2 DONE` when the run is finished (about 4-6 hours after
launch, including 20-30 minutes of setup and data generation). Then:

```bash
bash aws_e2_launch.sh fetch       # copies ~/tsarm-aws-results.tgz to CloudShell
bash aws_e2_launch.sh cleanup     # terminates every node
```

Download the archive with **Actions → Download file** →
`/home/cloudshell-user/tsarm-aws-results.tgz` and unpack it in the project
folder on the Mac. It contains `results/cluster_scaling_{strong,weak}_aws*`
and the master's log.

If the master was already powered off, start it first
(`aws ec2 start-instances --instance-ids <id from status>`), wait a minute,
then `fetch`.

Discard any run whose log line shows a **WARNING** about granted executors:
it used fewer executors than requested.

## Troubleshooting

| Symptom | Fix |
|---|---|
| `VcpuLimitExceeded` | Check the quota in **Service Quotas** for `eu-north-1`; with 8 vCPUs use `WORKERS=3 bash aws_e2_launch.sh launch` and report p = 1, 2, 4, 6 |
| `... not eligible for Free Tier` | `aws ec2 describe-instance-types --filters Name=free-tier-eligible,Values=true --query 'InstanceTypes[].[InstanceType,VCpuInfo.DefaultVCpus]'`, then `TYPE=<type> bash aws_e2_launch.sh launch` |
| "Region unavailable" in the console | Switch the console back to **Europe (Stockholm)** |
| `status` shows fewer than 7 workers alive after 90 min | The master runs with the workers it has; check a worker with `ssh -i ~/.ssh/tsarm.pem ubuntu@<ip> tail ~/e2-node.log` |
| `SETUP FAILED` or `GENERATE FAILED` in a log | `cleanup`, then `launch` again |
| Credit running low | **Billing → Bills** shows what is spending; `cleanup` |
