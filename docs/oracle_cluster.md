# Running TSARM on an Oracle Cloud Always Free Spark cluster (experiment E2)

This guide builds a **4-node Apache Spark standalone cluster** entirely inside
Oracle Cloud Infrastructure's *Always Free* allowance (4 Ampere A1 OCPUs and
24 GB RAM in total), then runs TSARM's multi-node scalability experiment.

> **Stay inside the free allowance.** Oracle disables and later deletes A1
> instances if a free-tier account provisions more A1 capacity than allowed.
> The layout below uses **exactly** the allowance: 4 VMs × (1 OCPU, 6 GB).

## 1. Create the account (one time)

1. Sign up at <https://www.oracle.com/cloud/free/>. A card is needed for
   identity verification only; the Always Free tier is not charged.
2. **Choose the home region carefully: it cannot be changed later.** Free A1
   capacity is sometimes exhausted in popular regions. Johannesburg, London or
   Frankfurt are reasonable choices from Nigeria.

## 2. Network (one time)

In the console, go to *Networking → Virtual Cloud Networks → Start VCN Wizard →
"Create VCN with Internet Connectivity"*. The defaults are fine: a public
subnet `10.0.0.0/24` inside `10.0.0.0/16`.

The default security list already allows SSH (port 22) from the internet and
all traffic **within** the VCN, which is what Spark needs between nodes.
Do **not** open Spark ports (7077, 8080) to the internet. Use an SSH tunnel to
see the web UI.

## 3. Create the four VMs

Do this four times (`tsarm-1` … `tsarm-4`): *Compute → Instances → Create*.

| Setting | Value |
|---|---|
| Image | Canonical **Ubuntu 24.04** (aarch64) |
| Shape | **VM.Standard.A1.Flex**, **1 OCPU, 6 GB** memory |
| Network | the VCN/subnet from step 2, assign a public IPv4 |
| SSH key | upload your public key (`~/.ssh/id_ed25519.pub`) |
| Boot volume | default (≥ 47 GB); the free tier includes 200 GB in total |

Note each VM's **private IP** (10.0.0.x). `tsarm-1` is the master.

## 4. Set up every node

From your laptop, for each VM:

```bash
scp scripts/cluster/setup_node.sh ubuntu@<public-ip>:
ssh ubuntu@<public-ip> 'bash setup_node.sh'
```

The script installs Java 17, Python, Spark 4.0.3 (via pip), `lbzip2`, and the
TSARM code, and opens the firewall to the private subnet only. Repeat for all 4.

## 5. Start the cluster

```bash
# on tsarm-1 (master + one worker)
ssh ubuntu@<tsarm-1-public-ip>
bash ~/TSARM/scripts/cluster/start_master.sh        # prints spark://10.0.0.X:7077
bash ~/TSARM/scripts/cluster/start_worker.sh spark://10.0.0.X:7077

# on tsarm-2, tsarm-3, tsarm-4
bash ~/TSARM/scripts/cluster/start_worker.sh spark://10.0.0.X:7077
```

To check that all 4 workers registered, open the UI through a tunnel: run
`ssh -L 8080:localhost:8080 ubuntu@<tsarm-1-public-ip>`, then browse to
<http://localhost:8080>.

## 6. Run experiment E2

There is no shared filesystem, so **generate the identical dataset on every
node first**. The generator is deterministic for a given seed:

```bash
# on EVERY node
source ~/tsarm-env.sh && cd ~/TSARM
python3 scripts/core_scaling.py --generate-only --data-dir ~/tsarm-data \
    --mode strong --cores 1,2,3,4 --entities 300000
```

Then, on `tsarm-1` only:

```bash
source ~/tsarm-env.sh && cd ~/TSARM
TSARM_DRIVER_MEMORY=1g python3 scripts/core_scaling.py \
    --master-url spark://10.0.0.X:7077 --data-dir ~/tsarm-data \
    --mode strong --cores 1,2,3,4 --entities 300000 --executor-memory 3g
```

`p` = number of 1-core executors, i.e. worker nodes. Each run records
`executors_granted`, and the script prints a **WARNING** if fewer executors
than `p` were granted (e.g. `--executor-memory` larger than a worker's free
memory). Discard such runs. With 4 GB workers, 3 GB executors give exactly one
executor per worker. Results are written to
`results/cluster_scaling_strong_{runs,summary}.csv` and `.json`, with a
hardware fingerprint. Copy them back with
`scp ubuntu@<tsarm-1-public-ip>:TSARM/results/cluster_scaling_* results/`.

Memory budget on `tsarm-1`: driver 1 GB + worker 4 GB on a 6 GB VM. Keep
`--entities` moderate (≈ 300k per snapshot ≈ 2.4M triples) for the strong-scaling
run. Raise it gradually for weak scaling (`--mode weak`, entities per worker).

## 7. Stop / clean up

```bash
bash ~/TSARM/scripts/cluster/stop_all.sh   # on each node
```

Stopped Always Free VMs cost nothing. Do not resize them beyond 1 OCPU / 6 GB
each while all four exist.

## Using the cluster for Wikidata extraction

The same VMs can stream-filter the historical Wikidata dumps cloud-side (see
`docs/wikidata_data_plan.md`), so only the small filtered output is downloaded
to the laptop:

```bash
curl -sL https://archive.org/download/wikidatawiki-20170101/wikidatawiki-20170101-pages-articles.xml.bz2 \
  | lbzip2 -dc \
  | python3 scripts/extract_wikidata.py --input - --format xml \
      --out ~/wikidata/2017-01-01.tsv.gz --workers 1
```
