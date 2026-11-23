# Google Cloud free trial: exact steps for TSARM

This guide uses the Google Cloud **free trial** ($300 of credit for 90 days)
for two jobs:

- **Phase A (data):** one VM downloads the Wikidata dumps at data-centre
  speed, filters them, and runs the Wikidata experiments. Only small result
  files come back to your Mac, so your home connection hardly matters.
- **Phase B (cluster):** four small VMs run the multi-node Spark experiment
  (E2) with the same scripts as `docs/oracle_cluster.md`.

Everything is done in **Cloud Shell**, a terminal in your browser with the
`gcloud` tool already installed. Nothing needs to be installed on your Mac and
no SSH keys are needed.

| Item | Size | Estimated cost |
|---|---|---|
| Phase A VM `e2-standard-8` (8 vCPU, 32 GB), about 3 days | 72 h × ~$0.27/h | ~$20 |
| Phase A disk, 300 GB balanced, about 3 days | | ~$3 |
| Storage bucket for the extracted files (~10–20 GB) | per month | < $1 |
| Phase B, 4 × `e2-standard-2` (2 vCPU, 8 GB), about 10 h | 10 h × ~$0.27/h | ~$3 |
| **Total** | | **about $30 of the $300** |

Prices are for region `us-central1` (Iowa), the cheapest region and close to
the Internet Archive's servers.

> **Free-trial rules that matter here.** At most **8 vCPUs may run at the same
> time**, and GPUs are not allowed. So Phase A's VM (8 vCPUs) must be
> **deleted** before Phase B's four VMs (8 vCPUs in total) are created. The
> trial never charges your card: when the credit or the 90 days run out,
> resources stop unless you choose to upgrade to a paid account. Check these
> limits on the sign-up page, as Google can change them.

---

## Step 0: Before you start

**0.1 Merge the pull request.** The VMs download TSARM from GitHub `main`.

**0.2 Your card.** Sign-up verifies a Visa or Mastercard with a small
temporary charge. If your bank blocks international online payments, ask it
to allow them first (the same fix as for Oracle).

## Step 1: Create the account and a project

1. Go to <https://cloud.google.com/free> → **Get started for free**. Sign in
   with your Google account, choose **Nigeria**, account type **Individual**,
   and enter the card.
2. In the console (<https://console.cloud.google.com>), click the project
   picker at the top → **New project** → name it `tsarm` → **Create**. Note
   the **project ID** it shows (for example `tsarm-482915`).
3. **Budget alert:** ☰ menu → **Billing** → **Budgets & alerts** → **Create
   budget**. Name `tsarm`, amount **$50**, alerts at 50 %, 90 % and 100 %.
   Google then emails you before spending gets near $50. (Alerts warn; they do
   not stop resources. The trial credit itself is the hard limit.)
4. Open **Cloud Shell**: the `>_` icon at the top right of the console. A
   terminal opens at the bottom of the page. All commands below are typed
   there unless they say *on the VM*.

In Cloud Shell, once (replace the project ID with yours):

```bash
gcloud config set project tsarm-482915
gcloud config set compute/region us-central1
gcloud config set compute/zone us-central1-a
gcloud services enable compute.googleapis.com storage.googleapis.com
```

Create a storage bucket to keep the extracted Wikidata files after the VM is
deleted (the name must be globally unique, so add your project ID):

```bash
gcloud storage buckets create gs://tsarm-482915-data --location=us-central1
```

---

## Phase A: Download, filter and mine Wikidata (one VM)

### A1. Create the VM

```bash
gcloud compute instances create tsarm-data \
    --machine-type=e2-standard-8 \
    --image-family=ubuntu-2404-lts-amd64 --image-project=ubuntu-os-cloud \
    --boot-disk-size=300GB --boot-disk-type=pd-balanced \
    --scopes=cloud-platform
```

300 GB is enough: the script keeps only **one** raw dump on disk at a time
(the largest is 157 GB) and deletes it after filtering. `--scopes` lets the
VM write to your bucket.

If the image family is not found, list the current Ubuntu images with
`gcloud compute images list --filter="family~ubuntu-2404"` and use the
`amd64` family shown.

### A2. Log in and set up

```bash
gcloud compute ssh tsarm-data
```

(The first time, it creates a key for you; press Enter at the questions.)

*On the VM:*

```bash
git clone https://github.com/ishaq-ishaq/TSARM.git
SUBNET=10.128.0.0/9 bash TSARM/scripts/cluster/setup_node.sh   # Java 17, Python, Spark, lbzip2, pigz (~5 min)
source ~/tsarm-env.sh
```

### A3. Download and filter (many hours; runs unattended)

*On the VM*, inside `tmux` so it keeps running when you close the browser:

```bash
tmux new -s wikidata
cd ~/TSARM
bash scripts/download_wikidata.sh 2026-11     # the expiring dump FIRST
bash scripts/download_wikidata.sh             # then the other six
bash scripts/download_dbpedia.sh              # optional: DBpedia releases + ontology
```

Detach with **Ctrl-b** then **d**. You can close the browser tab. To check
later: open Cloud Shell, `gcloud compute ssh tsarm-data`, then
`tmux attach -t wikidata`.

When a snapshot finishes, its filtered file appears in
`~/TSARM/data/processed/wikidata/`. **Save each one to the bucket** as soon as
it exists (it is small; this protects it if anything goes wrong):

```bash
gcloud storage cp ~/TSARM/data/processed/wikidata/*.tsv.gz gs://tsarm-482915-data/wikidata/
```

### A4. Run the Wikidata experiments on the VM

Once at least the 2026-11 snapshot and one older snapshot are filtered, *on
the VM* (in `tmux`):

```bash
cd ~/TSARM && source ~/tsarm-env.sh
export TSARM_DRIVER_MEMORY=24g      # the VM has 32 GB
python3 scripts/run_wikidata_temporal.py --sample 0.01 --tag gcp_s001          # quick check
python3 scripts/run_wikidata_temporal.py --sample 0.10 --tag gcp_s010
python3 scripts/run_wikidata_temporal.py --tag gcp_full                         # h = 1, all data
python3 scripts/run_wikidata_temporal.py --sample 0.10 --superclass-depth 2 --tag gcp_s010_h2
```

The rehearsal (`results/wikidata_rehearsal.csv`) suggests depth 1 on the full
data and deeper closures only on samples. Each run writes
`results/wikidata_temporal_rules_<tag>.{json,csv}`, including the hardware it
ran on.

### A5. Bring the results to your Mac, then delete the VM

*On the VM*, copy the results to the bucket:

```bash
gcloud storage cp ~/TSARM/results/wikidata_* gs://tsarm-482915-data/results/
```

Then download them from the browser: console → ☰ → **Cloud Storage** →
**Buckets** → `tsarm-482915-data` → `results/` → tick the files →
**Download**, and put them in `~/Documents/Projects/TSARM/results/` on your
Mac. (If your connection is good, you can also download the filtered
Wikidata files from `wikidata/` the same way, to rerun experiments locally.)

Delete the VM. **This must be done before Phase B**, because of the 8-vCPU
limit. The bucket keeps the data. In Cloud Shell:

```bash
gcloud compute instances delete tsarm-data
```

---

## Phase B: The 4-node Spark cluster (experiment E2)

### B1. Create four VMs

In Cloud Shell:

```bash
gcloud compute instances create tsarm-1 tsarm-2 tsarm-3 tsarm-4 \
    --machine-type=e2-standard-2 \
    --image-family=ubuntu-2404-lts-amd64 --image-project=ubuntu-os-cloud \
    --boot-disk-size=30GB --boot-disk-type=pd-balanced
gcloud compute instances list      # note each INTERNAL_IP; tsarm-1 is the master
```

The VMs share Google's default network, whose built-in firewall rule
`default-allow-internal` already lets them talk to each other. Nothing is
opened to the internet.

### B2. Set up every node

Run the setup on all four at once from Cloud Shell (`--quiet` accepts
gcloud's first-connection questions automatically; about 5 minutes):

```bash
for n in 1 2 3 4; do
  gcloud compute ssh tsarm-$n --quiet --command \
    "git clone -q https://github.com/ishaq-ishaq/TSARM.git; SUBNET=10.128.0.0/9 bash TSARM/scripts/cluster/setup_node.sh" &
done; wait
```

### B3. Start Spark

Each VM has 2 vCPUs and 8 GB, so each worker offers **2 cores and 5 GB**.

```bash
gcloud compute ssh tsarm-1 --quiet --command "bash ~/TSARM/scripts/cluster/start_master.sh"
# prints: Master starting: spark://10.128.0.X:7077   <- write this URL down
MASTER=spark://10.128.0.X:7077
for n in 1 2 3 4; do
  gcloud compute ssh tsarm-$n --quiet --command "WORKER_CORES=2 WORKER_MEMORY=5g bash ~/TSARM/scripts/cluster/start_worker.sh $MASTER"
done
```

**Check that all 4 workers joined:**

```bash
gcloud compute ssh tsarm-1 --quiet --command "curl -s localhost:8080/json/ | grep -o 'aliveworkers.*'"
```

It should print `aliveworkers" : 4,` (the number of live workers is 4). If not, read
`~/spark-logs/worker.log` on the missing VM.

### B4. Run experiment E2

There is no shared disk, so **every node needs the same input files**. The
generator is deterministic, so each node creates identical copies. This uses
the same 12 million triples as the single-machine experiment E1, so the two
can be compared directly:

```bash
for n in 1 2 3 4; do
  gcloud compute ssh tsarm-$n --quiet --command "source ~/tsarm-env.sh && cd ~/TSARM && \
    python3 scripts/core_scaling.py --generate-only --data-dir ~/tsarm-data --mode strong --cores 1,2,4,8 --entities 1500000" &
done; wait
```

Then on **tsarm-1**, inside `tmux`:

```bash
gcloud compute ssh tsarm-1
```

*On tsarm-1:*

```bash
tmux new -s e2
source ~/tsarm-env.sh && cd ~/TSARM
TSARM_DRIVER_MEMORY=2g python3 scripts/core_scaling.py \
    --master-url spark://10.128.0.X:7077 --data-dir ~/tsarm-data \
    --mode strong --cores 1,2,4,8 --entities 1500000 --executor-memory 2g --tag gcp
```

- `p` is the number of 1-core executors. Spark spreads executors across
  workers, so p = 1, 2 and 4 use one core on 1, 2 and 4 machines, and p = 8
  uses both cores of all four machines.
- Every run records `executors_granted`. A **WARNING** line means a run got
  fewer executors than `p`; discard it.
- If a run fails with an out-of-memory error, use `--entities 300000`
  (2.4 million triples), as in the Oracle guide.

Copy the results to the bucket, then download them in the browser as in A5:

```bash
gcloud storage cp ~/TSARM/results/cluster_scaling_* gs://tsarm-482915-data/results/
```

(Optional) **Weak scaling:** repeat the generation and the run with
`--mode weak --entities 150000`.

### B5. Delete the cluster

Stopped VMs still pay for their disks, so delete them when E2 is done. In
Cloud Shell:

```bash
gcloud compute instances delete tsarm-1 tsarm-2 tsarm-3 tsarm-4
```

When the thesis experiments are finished, delete the bucket too:
`gcloud storage rm -r gs://tsarm-482915-data`.

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `Quota 'CPUS_ALL_REGIONS' exceeded` | More than 8 vCPUs are running: delete `tsarm-data` before creating Phase B's VMs |
| `The resource ... was not found` (image) | List images: `gcloud compute images list --filter="family~ubuntu-2404"` |
| `ZONE_RESOURCE_POOL_EXHAUSTED` | Use another zone: `gcloud config set compute/zone us-central1-b` and retry |
| `AccessDeniedException` writing to the bucket | The VM was created without `--scopes=cloud-platform`; recreate it, or copy with `gcloud compute scp` from Cloud Shell |
| Download stopped | Re-run the same `download_wikidata.sh` command; it resumes |
| Workers missing (`aliveworkers` < 4) | Check `~/spark-logs/worker.log` on that VM; restart it with `start_worker.sh` |
| `WARNING: requested p=… granted …` | A worker is down or short of memory; restart it, or lower `--executor-memory` |
| Cloud Shell disconnected | Normal after inactivity; jobs inside `tmux` on the VM keep running |
