# Oracle Cloud Always Free: exact steps for TSARM

This guide uses only Oracle Cloud Infrastructure's **Always Free** allowance:

| Resource | Free allowance | How TSARM uses it |
|---|---|---|
| Arm (Ampere A1) compute | **4 OCPUs + 24 GB RAM** in total | Phase A: 1 big VM. Phase B: 4 small VMs |
| Disk (boot + block volumes) | **200 GB in total** | Phase A: one 200 GB disk. Phase B: 4 × 47 GB |
| Outbound data | 10 TB / month | Copying results to your laptop |

The work runs in **two phases**, because the free disk cannot hold a 157 GB
Wikidata dump *and* four VM boot disks at the same time:

- **Phase A (data):** one VM with all 4 OCPUs, 24 GB and a 200 GB disk
  downloads and filters the Wikidata dumps. You copy the small results to
  your laptop, then delete the VM.
- **Phase B (cluster):** four VMs of 1 OCPU / 6 GB run the multi-node Spark
  experiment (E2).

> ⚠️ **Never exceed the allowance.** If a free account holds more A1 capacity
> than allowed, Oracle disables and later deletes the A1 instances. In each
> phase, the VMs together must total at most **4 OCPUs, 24 GB RAM and 200 GB of
> disk**. Delete Phase A's VM before creating Phase B's.

---

## Step 0: Before you start (on your Mac)

**0.1 Merge the code.** The VMs download TSARM from GitHub `main`, so the pull
request with the cluster scripts must be merged first.

**0.2 Create an SSH key** for logging in to the VMs. In Terminal:

```bash
ssh-keygen -t ed25519 -f ~/.ssh/oracle_tsarm -N ""
cat ~/.ssh/oracle_tsarm.pub          # you will paste this line into Oracle
```

## Step 1: Create the account

1. Go to <https://www.oracle.com/cloud/free/> → **Start for free**.
2. Fill in your details. A credit/debit card is required **for verification
   only**; Always Free resources are not charged. If one card is refused, try
   another; some Nigerian cards are declined by the verification step.
3. **Home region: choose carefully, it can never be changed.** Pick one near
   you with spare Arm capacity: **South Africa Central (Johannesburg)**,
   **UK South (London)** or **Germany Central (Frankfurt)**.
4. Wait for the "account is ready" email (minutes to a few hours), then sign
   in at <https://cloud.oracle.com>.

## Step 2: Create the network (once)

1. Open the **☰ menu** (top left) → **Networking** → **Virtual cloud
   networks**.
2. Click **Actions → Start VCN Wizard** → choose **Create VCN with Internet
   Connectivity** → **Start VCN Wizard**.
3. Name it `tsarm-vcn`. Keep the defaults: VCN CIDR `10.0.0.0/16`, public
   subnet `10.0.0.0/24`. Click **Next** → **Create**.
4. **Allow traffic between your VMs.** The default rules do *not* allow this,
   and Spark needs it:
   1. Open `tsarm-vcn` → **Security** (or **Security Lists**) → **Default
      Security List for tsarm-vcn**.
   2. **Add Ingress Rules** → *Source Type* **CIDR**, *Source CIDR*
      **`10.0.0.0/16`**, *IP Protocol* **All Protocols** → **Add Ingress
      Rules**.

   This only opens traffic *inside* your private network. Nothing new is
   exposed to the internet. SSH (port 22) is already allowed by default.

---

## Phase A: Download and filter Wikidata (one big VM)

### A1. Create the data VM

1. **☰ menu → Compute → Instances → Create instance**.
2. **Name:** `tsarm-data`.
3. **Image and shape → Edit**:
   - **Change image** → **Canonical Ubuntu** → **24.04** (it must say
     *aarch64*) → **Select image**.
   - **Change shape** → **Ampere** → tick **VM.Standard.A1.Flex** → set
     **OCPUs = 4**, **Memory = 24 GB** → **Select shape**. The card should show
     the *Always Free-eligible* label.
4. **Networking:** select `tsarm-vcn` and its **public subnet**; make sure
   **Assign a public IPv4 address** is on.
5. **Add SSH keys** → **Paste public keys** → paste the line from step 0.2.
6. **Boot volume** → tick **Specify a custom boot volume size** → **200 GB**.
7. Click **Create**. When the state turns **Running**, copy the **Public IP
   address**.

> **"Out of capacity for shape VM.Standard.A1.Flex"?** That's a free-tier
> capacity shortage, not your mistake. Change the *Availability domain* (in
> *Placement*) and retry, or try again later. It usually succeeds within a day.

### A2. Log in and set up

On your Mac:

```bash
ssh -i ~/.ssh/oracle_tsarm ubuntu@<PUBLIC-IP>
```

On the VM:

```bash
git clone https://github.com/ishaq-ishaq/TSARM.git
bash TSARM/scripts/cluster/setup_node.sh     # Java 17, Python, Spark, lbzip2, pigz (~5 min)
source ~/tsarm-env.sh
```

### A3. Download and filter (runs for many hours)

Run it inside `tmux`, so it keeps going if your Mac disconnects:

```bash
tmux new -s wikidata
cd ~/TSARM
bash scripts/download_wikidata.sh 2026-11     # the expiring dump FIRST
bash scripts/download_wikidata.sh             # then the other six
```

Detach with **Ctrl-b** then **d**, and log out. To check on it later:
`ssh -i ~/.ssh/oracle_tsarm ubuntu@<PUBLIC-IP>` then `tmux attach -t wikidata`.

### A4. Copy the results to your Mac, then delete the VM

On your Mac:

```bash
mkdir -p ~/Documents/Projects/TSARM/data/processed/wikidata
scp -i ~/.ssh/oracle_tsarm 'ubuntu@<PUBLIC-IP>:TSARM/data/processed/wikidata/*.tsv.gz' \
    ~/Documents/Projects/TSARM/data/processed/wikidata/
ls -lh ~/Documents/Projects/TSARM/data/processed/wikidata/   # expect 7 files
```

Then in the console: **Compute → Instances → `tsarm-data` → Actions →
Terminate**, and tick **Permanently delete the attached boot volume**. This
frees the 4 OCPUs and 200 GB for Phase B.

---

## Phase B: The 4-node Spark cluster (experiment E2)

### B1. Create four VMs

Repeat **A1** four times with these differences:

| Setting | Value |
|---|---|
| Names | `tsarm-1`, `tsarm-2`, `tsarm-3`, `tsarm-4` |
| Shape | VM.Standard.A1.Flex, **OCPUs = 1**, **Memory = 6 GB** |
| Boot volume | leave the **default** (about 47 GB; do *not* tick custom size) |

Note each VM's **public IP** and **private IP** (shown on the instance page;
it looks like `10.0.0.x`). **`tsarm-1` is the master.**

### B2. Set up every node

For **each** of the four VMs:

```bash
ssh -i ~/.ssh/oracle_tsarm ubuntu@<PUBLIC-IP>
git clone https://github.com/ishaq-ishaq/TSARM.git
bash TSARM/scripts/cluster/setup_node.sh
exit
```

### B3. Start Spark

On **tsarm-1**:

```bash
ssh -i ~/.ssh/oracle_tsarm ubuntu@<TSARM-1-PUBLIC-IP>
bash ~/TSARM/scripts/cluster/start_master.sh
# prints: Master starting: spark://10.0.0.X:7077   <- write this URL down
bash ~/TSARM/scripts/cluster/start_worker.sh spark://10.0.0.X:7077
```

On **tsarm-2, tsarm-3 and tsarm-4** (log in to each):

```bash
bash ~/TSARM/scripts/cluster/start_worker.sh spark://10.0.0.X:7077
```

**Check that all 4 workers joined.** On your Mac, open a tunnel:

```bash
ssh -i ~/.ssh/oracle_tsarm -L 8080:localhost:8080 ubuntu@<TSARM-1-PUBLIC-IP>
```

then browse to <http://localhost:8080>. It should list **4 Alive workers**,
each with 1 core. If one is missing, check that step 2.4 (the ingress rule)
was done, and read `~/spark-logs/worker.log` on that VM.

### B4. Run experiment E2

There is no shared disk, so **every node needs the same input files**. The
generator is deterministic, so each node creates identical copies. On **each
of the 4 VMs**:

```bash
source ~/tsarm-env.sh && cd ~/TSARM
python3 scripts/core_scaling.py --generate-only --data-dir ~/tsarm-data \
    --mode strong --cores 1,2,3,4 --entities 300000
```

Then on **tsarm-1 only**, inside `tmux`:

```bash
tmux new -s e2
source ~/tsarm-env.sh && cd ~/TSARM
TSARM_DRIVER_MEMORY=1g python3 scripts/core_scaling.py \
    --master-url spark://10.0.0.X:7077 --data-dir ~/tsarm-data \
    --mode strong --cores 1,2,3,4 --entities 300000 --executor-memory 3g
```

- `p` = number of 1-core executors = number of worker VMs used.
- Every run records `executors_granted`. A **WARNING** line means a run got
  fewer executors than `p`; discard such runs.

Results go to `results/cluster_scaling_strong_*`. Copy them to your Mac:

```bash
scp -i ~/.ssh/oracle_tsarm 'ubuntu@<TSARM-1-PUBLIC-IP>:TSARM/results/cluster_scaling_*' \
    ~/Documents/Projects/TSARM/results/
```

(Optional) **Weak scaling**: repeat B4 with `--mode weak` and
`--entities 150000` (per worker), generating the data first on every node
with the same flags.

### B5. Stop

On each VM: `bash ~/TSARM/scripts/cluster/stop_all.sh`. Stopped or running,
Always Free VMs within the allowance cost nothing. Keep them for re-runs, or
terminate them when the thesis experiments are finished.

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `Out of capacity for shape` | Try another availability domain, or retry later |
| `ssh: Connection timed out` | VM not *Running* yet, or wrong public IP |
| `Permission denied (publickey)` | Use `-i ~/.ssh/oracle_tsarm` and user **`ubuntu`** |
| Workers missing in the UI | Step 2.4 ingress rule missing; check `~/spark-logs/worker.log` |
| `WARNING: requested p=… granted …` | A worker is down or short of memory; restart it with `start_worker.sh` |
| Download stopped | Re-run the same `download_wikidata.sh` command; it resumes |
