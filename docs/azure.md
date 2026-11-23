# Azure for Students: exact steps for TSARM

This guide uses an **Azure for Students** subscription ($100 of credit for 12
months, no credit card) for two jobs:

- **Phase A (data):** one VM downloads the Wikidata dumps at data-centre
  speed, filters them, and runs the Wikidata experiments. Only small result
  files come back to your Mac.
- **Phase B (cluster):** three or four small VMs run the multi-node Spark
  experiment (E2) with the same scripts as `docs/gcp.md` and
  `docs/oracle_cluster.md`.

Everything is done in **Azure Cloud Shell**, a terminal in the browser with
the `az` tool already installed. Nothing needs to be installed on your Mac.

| Item | Size | Estimated cost |
|---|---|---|
| Phase A VM `Standard_DS12_v2` (4 vCPU, 28 GB), 2–4 days | 48–96 h × ~$0.37/h | ~$18–36 |
| Phase A disk, 300 GB Standard SSD, about 4 days | | ~$3 |
| Phase B, 4 × `Standard_DS1_v2` (1 vCPU, 3.5 GB), about 10 h | 10 h × ~$0.30/h | ~$3 |
| Public IP addresses and Cloud Shell storage | | ~$1 |
| **Total** | | **about $25–45 of the $100** |

Prices are approximate East US pay-as-you-go rates. Other regions cost up to
about 20 % more. The **Cost Management** page shows the real figures.

> **What a student subscription actually allows (checked 23 Nov 2026).**
> - **Regions:** only `southafricanorth`, `francecentral`, `switzerlandnorth`,
>   `spaincentral` and `belgiumcentral` (policy *Allowed resource deployment
>   regions*; see `az policy assignment list`).
> - **vCPUs:** at most **6** at once per region, and at most **4** from any one
>   family. The current v5 families (`Dsv5`, `Esv5`) are set to **0**; older
>   families (`DSv2`, `DSv3`, `DSv4`, `ESv3`, ...) allow 4. Burstable B-series
>   allows 10, but its CPU is throttled, which spoils timings.
> - **Sizes that were accepted** in South Africa North: `Standard_DS12_v2`
>   (4 vCPU, 28 GB) for Phase A; `Standard_DS1_v2` (1 vCPU, 3.5 GB) is offered
>   for Phase B. `E4s_v3` and `E4as_v4` were refused.
> - New subscriptions must first register the resource providers (Step 1.3).
>
> **General rules.**
> - **Spending limit:** when the $100 is used up, Azure stops the resources.
>   Nothing is ever charged to you.
> - **Regions:** only some regions are allowed. Creating a resource elsewhere
>   fails with `RequestDisallowedByAzure`, and the error lists the regions you
>   may use.
> - **vCPU quota:** the number of vCPUs that can exist at once is capped per
>   region, and the cap may be lower than on a paid account. Step 1.4 checks
>   it, and the plan below adapts to it.

---

## Step 0: Before you start

**0.1 Merge the pull request.** The VMs download TSARM from GitHub `main`.

**0.2 Budget alert.** In the portal (<https://portal.azure.com>), search for
**Cost Management**, then open **Budgets** → **Add**. Set the scope to your
Azure for Students subscription, the amount to **$60**, and alerts at 50 %,
90 % and 100 %. Alerts warn you by email; the spending limit is the hard stop.

## Step 1: Open Cloud Shell and choose a region

1. In the portal, click the **Cloud Shell** icon (`>_`) in the top bar and
   choose **Bash**.
2. When it asks about storage, choose **Mount storage account** and let it
   **create one for you**. It costs a few cents a month. It keeps your SSH key
   between sessions, which you need to log back in to the VMs on later days.
   (Without storage, the key is lost when the session closes.)
3. Register the services once (a new subscription has none), then create a
   resource group in an allowed region:

   ```bash
   for ns in Microsoft.Compute Microsoft.Network Microsoft.Storage Microsoft.CloudShell; do az provider register --namespace $ns; done
   until [ "$(az provider show -n Microsoft.Compute --query registrationState -o tsv)" = Registered ]; do sleep 15; done
   ```


   ```bash
   REGION=southafricanorth
   az group create -n tsarm -l $REGION
   ```

   If this fails with `RequestDisallowedByAzure`, pick one of the regions
   listed in the error, set `REGION` to it, and run the command again.
   The allowed regions are listed by `az policy assignment list -o json`.
4. **Check the vCPU quota** in that region:

   ```bash
   az vm list-usage -l $REGION -o table | grep -E "Total Regional vCPUs|DSv2|DSv5|ESv5"
   ```

   The **Limit** column shows the caps. Choose the plan from this table:

   | Total Regional vCPUs limit | Phase A VM | Phase B cluster |
   |---|---|---|
   | 8 or more, v5 allowed | `Standard_E4s_v5` (4 vCPU, 32 GB) | 4 × `Standard_D2s_v5` (8 vCPUs) |
   | 6, v5 = 0 (typical student) | `Standard_DS12_v2` (4 vCPU, 28 GB) | 4 × `Standard_DS1_v2` (1 vCPU, 3.5 GB) |

   If the ESv5 family limit is 0, use `Standard_D4s_v5` (4 vCPU, 16 GB) for
   Phase A instead. **Phase A and Phase B never run at the same time**, so the
   quota only has to fit the larger of the two.

   **To raise a limit**, open the portal's **Quotas** page → **Compute**,
   select the region, and request a new limit for *Total Regional vCPUs*.
   Student subscriptions are sometimes refused, but asking costs nothing.
5. Create the private network that all the VMs share (Azure's default rules
   let VMs on one network reach each other, and nothing else is opened):

   ```bash
   az network vnet create -g tsarm -n tsarm-vnet --address-prefix 10.0.0.0/16 \
       --subnet-name default --subnet-prefix 10.0.0.0/24
   ```

---

## Phase A: Download, filter and mine Wikidata (one VM)

### A1. Create the VM

```bash
az vm create -g tsarm -n tsarm-data --image Ubuntu2404 --size Standard_DS12_v2 \
    --vnet-name tsarm-vnet --subnet default \
    --os-disk-size-gb 300 --storage-sku StandardSSD_LRS \
    --admin-username azureuser --generate-ssh-keys
```

300 GB is enough: the download script keeps only **one** raw dump on disk at a
time (the largest is 157 GB) and deletes it after filtering. The command
prints the VM's `publicIpAddress`. Note it, or look it up later with
`az vm list-ip-addresses -g tsarm -o table`.

If `Ubuntu2404` is not recognised, use
`--image Canonical:ubuntu-24_04-lts:server:latest`. If the size is not
available in the region, the error suggests alternatives; any size with
4 vCPUs and at least 16 GB will do.

### A2. Log in and set up

```bash
ssh azureuser@<publicIpAddress>
```

*On the VM:*

```bash
git clone https://github.com/ishaq-ishaq/TSARM.git
bash TSARM/scripts/cluster/setup_node.sh      # Java 17, Python, Spark, lbzip2, pigz (~5 min)
source ~/tsarm-env.sh
```

### A3. Download and filter (many hours; runs unattended)

*On the VM*, inside `tmux` so the job keeps running after you close the browser:

```bash
tmux new -s wikidata
cd ~/TSARM
bash scripts/download_wikidata.sh 2026-11     # the expiring dump FIRST
bash scripts/download_wikidata.sh             # then the other six
bash scripts/download_dbpedia.sh              # optional: DBpedia releases + ontology
```

**How long it takes.** On 200 random Wikidata entities (mean 20 KB of JSON
each), the extractor processed about 8,000 entities/s on one M5 core, 14,000
with 2 workers and 21,000 with 4. Decompression alone ran at about 74,000
entities/s, so it is not the bottleneck. A cloud vCPU is slower than an M5
core, so expect roughly 10,000 entities/s on 4 vCPUs. At that rate the 2026-11
dump (about 120 million entities) takes about 3–4 hours, and the whole series
about half a day of processing. The download from the Internet Archive
usually takes longer than the processing, so allow 1–2 days in total.

Detach with **Ctrl-b** then **d**. You can now close the browser. To check on
the job later, open Cloud Shell, `ssh azureuser@<ip>`, then
`tmux attach -t wikidata`. Filtered files appear in
`~/TSARM/data/processed/wikidata/` (a few GB each).

### A4. Run the Wikidata experiments on the VM

Once the 2026-11 snapshot and at least one older snapshot are filtered, run
these *on the VM*, in `tmux`:

```bash
cd ~/TSARM && source ~/tsarm-env.sh
export TSARM_DRIVER_MEMORY=22g      # DS12_v2 has 28 GB (24g on a 32 GB E4s_v5; 12g on 16 GB)
python3 scripts/run_wikidata_temporal.py --sample 0.01 --tag az_s001          # quick check
python3 scripts/run_wikidata_temporal.py --sample 0.10 --tag az_s010
python3 scripts/run_wikidata_temporal.py --tag az_full                         # h = 1, all data
python3 scripts/run_wikidata_temporal.py --sample 0.10 --superclass-depth 2 --tag az_s010_h2
python3 scripts/propose_events.py propose --data-dir data/processed/wikidata \
    --out results/events/wikidata_candidates.csv                               # E4 candidates
```

If the full run fails with an out-of-memory error on a 16 GB VM, keep the
10 % sample as the main result and run the full data on the Mac instead (A5).

### A5. Bring the results to your Mac

In **Cloud Shell** (not on the VM), copy the result files into Cloud Shell:

```bash
mkdir -p ~/tsarm-results
scp -r azureuser@<ip>:~/TSARM/results/wikidata_\* azureuser@<ip>:~/TSARM/results/events ~/tsarm-results/
tar czf ~/tsarm-results.tgz -C ~ tsarm-results
```

Then click **Manage files → Download** in the Cloud Shell toolbar and enter
`tsarm-results.tgz`. Unpack the archive into
`~/Documents/Projects/TSARM/results/` on your Mac.

The filtered Wikidata files (about 10–20 GB) are best kept on the VM's disk.
To also have them on the Mac, add your Mac's public key to the VM and run
`scp` from the Mac.

### A6. Stop paying for the VM

When Phase A is finished, **deallocate** the VM. Compute charges stop, but the
disk and all the data stay, at about $0.60 a day for the disk:

```bash
az vm deallocate -g tsarm -n tsarm-data
```

To use it again later, run `az vm start -g tsarm -n tsarm-data`; its public
IP address may change. If creating the Phase B VMs fails with a quota error,
delete the data VM instead:
`az vm delete -g tsarm -n tsarm-data --yes`. Bring the results home (A5)
before deleting it.

---

## Phase B: The Spark cluster (experiment E2)

Below, `N` is the number of nodes chosen in Step 1.4 (4, 3 or 2).

### B1. Create the VMs

In Cloud Shell:

```bash
N=4
for n in $(seq 1 $N); do
  az vm create -g tsarm -n tsarm-$n --image Ubuntu2404 --size Standard_DS1_v2 \
      --vnet-name tsarm-vnet --subnet default --storage-sku StandardSSD_LRS \
      --admin-username azureuser --generate-ssh-keys --no-wait
done
sleep 120; az vm list-ip-addresses -g tsarm -o table   # public and private IPs; tsarm-1 is the master
```

Note the **private** IP address of tsarm-1, which starts with `10.0.0.`.

If the subscription refuses another public IP address (a quota error naming
`PublicIPAddresses`), create tsarm-2 to tsarm-N with `--public-ip-address ""`.
Then reach them through tsarm-1 with
`ssh -J azureuser@<tsarm-1 public ip> azureuser@<private ip>`.

### B2. Set up every node

Run this from Cloud Shell, once for each node's public IP (about 5 minutes,
and the nodes can run in parallel):

```bash
for ip in <ip1> <ip2> <ip3> <ip4>; do
  ssh -o StrictHostKeyChecking=accept-new azureuser@$ip \
    "git clone -q https://github.com/ishaq-ishaq/TSARM.git; bash TSARM/scripts/cluster/setup_node.sh" &
done; wait
```

### B3. Start Spark

With `DS1_v2` nodes (1 vCPU, 3.5 GB) each worker offers **1 core and 2.5 GB**. With 2-vCPU, 8 GB
nodes on a subscription that allows them, use `WORKER_CORES=2 WORKER_MEMORY=5g` instead.

**Quota note:** `DS12_v2` (Phase A) and `DS1_v2` belong to the same family (DSv2, limit 4 vCPUs),
so **deallocate or delete `tsarm-data` before creating the cluster**.

```bash
ssh azureuser@<ip1> "bash ~/TSARM/scripts/cluster/start_master.sh"
# prints: Master starting: spark://10.0.0.X:7077   <- write this URL down
MASTER=spark://10.0.0.X:7077
for ip in <ip1> <ip2> <ip3> <ip4>; do
  ssh azureuser@$ip "WORKER_CORES=1 WORKER_MEMORY=2500m bash ~/TSARM/scripts/cluster/start_worker.sh $MASTER"
done
ssh azureuser@<ip1> "curl -s localhost:8080/json/ | grep -o 'aliveworkers.*'"   # expect N
```

### B4. Run experiment E2

There is no shared disk, so **every node needs the same input files**. The
generator is deterministic, so each node creates identical copies. On 3.5 GB
nodes use 2.4 million triples (`--entities 300000`); on 8 GB nodes the 12 million
triples of E1 (`--entities 1500000`, `--cores 1,2,4,8`):

```bash
for ip in <ip1> <ip2> <ip3> <ip4>; do
  ssh azureuser@$ip "source ~/tsarm-env.sh && cd ~/TSARM && \
    python3 scripts/core_scaling.py --generate-only --data-dir ~/tsarm-data --mode strong --cores 1,2,3,4 --entities 300000" &
done; wait
```

Then log in to tsarm-1 (`ssh azureuser@<ip1>`) and run, inside `tmux`:

```bash
tmux new -s e2
source ~/tsarm-env.sh && cd ~/TSARM
TSARM_DRIVER_MEMORY=1g python3 scripts/core_scaling.py \
    --master-url spark://10.0.0.X:7077 --data-dir ~/tsarm-data \
    --mode strong --cores 1,2,3,4 --entities 300000 --executor-memory 1500m --tag azure
```

- `p` is the number of 1-core executors, spread across the workers. On 4 × `DS1_v2`,
  p = 1–4 puts one executor on each of 1–4 machines. (On 4 × 2-vCPU nodes use
  `--cores 1,2,4,8`; on 3 nodes `--cores 1,2,3,6`.)
- Every run records `executors_granted`. A **WARNING** line means the run got
  fewer executors than requested; discard that run.
- On an out-of-memory error, use `--entities 300000` (2.4 million triples).
- (Optional) **Weak scaling:** generate and run again with `--mode weak
  --entities 150000`.

Bring `~/TSARM/results/cluster_scaling_*` home as in A5.

### B5. Delete the cluster

```bash
for n in $(seq 1 $N); do az vm delete -g tsarm -n tsarm-$n --yes --no-wait; done
```

When all the thesis experiments are finished, delete everything in one step.
This removes the data VM, disks, IP addresses and network, and stops all
charges:

```bash
az group delete -n tsarm --yes
```

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `RequestDisallowedByAzure` | The region is not allowed for students; use one listed in the error (Step 1.3) |
| `QuotaExceeded` / `OperationNotAllowed ... cores` | Too many vCPUs: deallocate or delete `tsarm-data`, use fewer nodes, or request more quota (Step 1.4) |
| `SkuNotAvailable` | That size is not offered in the region; the error lists alternatives with the same vCPUs |
| `ssh: Permission denied (publickey)` | Cloud Shell lost its key (no storage mounted); reset it: `az vm user update -g tsarm -n <vm> -u azureuser --ssh-key-value ~/.ssh/id_rsa.pub` |
| Download stopped | Re-run the same `download_wikidata.sh` command; it resumes |
| Workers missing (`aliveworkers` < N) | Check `~/spark-logs/worker.log` on that VM; restart it with `start_worker.sh` |
| `WARNING: requested p=… granted …` | A worker is down or short of memory; restart it, or lower `--executor-memory` |
| Cloud Shell disconnected | Normal after about 20 minutes idle; jobs inside `tmux` on the VMs keep running |
| Credit running low | **Cost Management → Cost analysis** shows what is spending; deallocate idle VMs |
