# AWS Free plan: experiment E2 on a 4-node cluster

The AWS **Free plan** gives $100 of credit (or up to $200 if you complete the
console activities) for 6 months. On this plan only *free-tier-eligible*
instance types can be launched. The largest is **`m7i-flex.large`** (2 vCPU,
8 GB). That is too small to mine the full Wikidata series, which runs on
Azure (`docs/azure.md`, Phase A). It is enough for the multi-node Spark
experiment E2, which this guide runs. Repeating E2 on a second provider shows
that the scaling result does not depend on one cloud.

Everything is done in **AWS CloudShell**, a terminal in the console with the
`aws` tool installed. Its home directory persists between sessions, so the
SSH key created below stays available.

| Item | Size | Estimated cost |
|---|---|---|
| 4 × `m7i-flex.large` (2 vCPU, 8 GB), about 10 h | 10 h × 4 × ~$0.10/h | ~$4 |
| 4 × 30 GB gp3 disks, public IPv4 addresses | | < $1 |
| **Total** | | **about $5 of the credit** |

Prices are for `us-east-1` (N. Virginia), the cheapest region.

> **Flex instances.** `m7i-flex` instances deliver their full CPU most of the
> time but are not guaranteed it at every moment. Report the AWS runs as a
> replication of E2, with this stated. The primary E2 numbers come from Azure's
> non-burstable `D2s_v5` nodes.

---

## Step 0: Checks (once)

1. **Budget:** console → **Billing and Cost Management** → **Budgets** →
   **Create budget** → *Monthly cost budget*, $50, with your email address.
2. **vCPU quota:** console → **Service Quotas** → **Amazon EC2** → search
   *Running On-Demand Standard (A, C, D, H, I, M, R, T, Z) instances*. The
   **Applied quota value** is in vCPUs. You need **8** for four nodes. With 6,
   use 3 nodes; with 4, use 2 nodes (`N` below). If it is lower, click
   **Request increase at account level**.
3. Switch the console to **US East (N. Virginia)** (region menu, top right),
   then open **CloudShell** (the `>_` icon in the top bar).

## Step 1: Key, firewall and image (in CloudShell)

```bash
export AWS_DEFAULT_REGION=us-east-1
mkdir -p ~/.ssh
aws ec2 create-key-pair --key-name tsarm --query KeyMaterial --output text > ~/.ssh/tsarm.pem
chmod 600 ~/.ssh/tsarm.pem

SG=$(aws ec2 create-security-group --group-name tsarm --description "TSARM Spark cluster" --query GroupId --output text)
aws ec2 authorize-security-group-ingress --group-id $SG --protocol tcp --port 22 --cidr 0.0.0.0/0      # SSH (key only)
aws ec2 authorize-security-group-ingress --group-id $SG --protocol -1 --source-group $SG               # nodes reach each other
echo "SG=$SG" > ~/tsarm-aws.env

AMI=$(aws ssm get-parameter --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id \
      --query Parameter.Value --output text)
echo "AMI=$AMI" >> ~/tsarm-aws.env
```

Only SSH is open to the internet, and it accepts only your key. Spark's ports
are open only between the four nodes.

## Step 2: Create the nodes

```bash
source ~/tsarm-aws.env
N=4
aws ec2 run-instances --image-id $AMI --instance-type m7i-flex.large --count $N \
    --key-name tsarm --security-group-ids $SG \
    --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":30,"VolumeType":"gp3"}}]' \
    --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=tsarm}]' --query 'Instances[].InstanceId' --output text
sleep 60
aws ec2 describe-instances --filters Name=tag:Name,Values=tsarm Name=instance-state-name,Values=running \
    --query 'Reservations[].Instances[].[PublicIpAddress,PrivateIpAddress]' --output table
```

Pick one node as the master and note both its addresses. Its **private**
address starts with `172.31.`.

If `run-instances` says the instance type is not eligible, list the types
your account may use and pick the largest:

```bash
aws ec2 describe-instance-types --filters Name=free-tier-eligible,Values=true \
    --query 'InstanceTypes[].[InstanceType,VCpuInfo.DefaultVCpus,MemoryInfo.SizeInMiB]' --output table
```

## Step 3: Set up every node

The default network's private range is `172.31.0.0/16`:

```bash
IPS="<ip1> <ip2> <ip3> <ip4>"        # the public addresses
for ip in $IPS; do
  ssh -i ~/.ssh/tsarm.pem -o StrictHostKeyChecking=accept-new ubuntu@$ip \
    "git clone -q https://github.com/ishaq-ishaq/TSARM.git; SUBNET=172.31.0.0/16 bash TSARM/scripts/cluster/setup_node.sh" &
done; wait
```

## Step 4: Start Spark

```bash
ssh -i ~/.ssh/tsarm.pem ubuntu@<ip1> "bash ~/TSARM/scripts/cluster/start_master.sh"
# prints: Master starting: spark://172.31.X.X:7077   <- write this URL down
MASTER=spark://172.31.X.X:7077
for ip in $IPS; do
  ssh -i ~/.ssh/tsarm.pem ubuntu@$ip "WORKER_CORES=2 WORKER_MEMORY=5g bash ~/TSARM/scripts/cluster/start_worker.sh $MASTER"
done
ssh -i ~/.ssh/tsarm.pem ubuntu@<ip1> "curl -s localhost:8080/json/ | grep -o 'aliveworkers.*'"   # expect N
```

## Step 5: Run E2

Every node generates the same input. The generator is deterministic, so the
copies are identical. The input is the same 12 million triples as E1:

```bash
for ip in $IPS; do
  ssh -i ~/.ssh/tsarm.pem ubuntu@$ip "source ~/tsarm-env.sh && cd ~/TSARM && \
    python3 scripts/core_scaling.py --generate-only --data-dir ~/tsarm-data --mode strong --cores 1,2,4,8 --entities 1500000" &
done; wait
ssh -i ~/.ssh/tsarm.pem ubuntu@<ip1>
```

*On the master*, inside `tmux`:

```bash
tmux new -s e2
source ~/tsarm-env.sh && cd ~/TSARM
TSARM_DRIVER_MEMORY=2g python3 scripts/core_scaling.py \
    --master-url spark://172.31.X.X:7077 --data-dir ~/tsarm-data \
    --mode strong --cores 1,2,4,8 --entities 1500000 --executor-memory 2g --tag aws
```

With 3 nodes, use `--cores 1,2,3,6`; with 2 nodes, `--cores 1,2,4`. Discard
any run that prints a **WARNING** about granted executors. On an out-of-memory
error, use `--entities 300000`.

## Step 6: Bring the results home and delete everything

In CloudShell:

```bash
scp -i ~/.ssh/tsarm.pem "ubuntu@<ip1>:~/TSARM/results/cluster_scaling_*" ~/
tar czf ~/tsarm-aws-results.tgz -C ~ $(cd ~ && ls cluster_scaling_*)
```

Download the archive with **Actions → Download file** →
`/home/cloudshell-user/tsarm-aws-results.tgz`, and unpack it into
`~/Documents/Projects/TSARM/results/` on your Mac.

Then terminate the nodes. Stopped instances still pay for their disks:

```bash
source ~/tsarm-aws.env
aws ec2 terminate-instances --instance-ids $(aws ec2 describe-instances \
    --filters Name=tag:Name,Values=tsarm Name=instance-state-name,Values=running,stopped \
    --query 'Reservations[].Instances[].InstanceId' --output text)
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| `VcpuLimitExceeded` | Fewer nodes (Step 0.2), or request a quota increase |
| `InvalidParameterCombination ... not eligible for Free Tier` | Use a type from the `free-tier-eligible` list (Step 2) |
| `Permission denied (publickey)` | Use `-i ~/.ssh/tsarm.pem` and user `ubuntu`; the key is only in CloudShell's home in `us-east-1` |
| Workers missing (`aliveworkers` < N) | Check `~/spark-logs/worker.log` on that node; is the security-group rule from Step 1 present? |
| Credit running low | **Billing → Bills** shows what is spending; terminate idle instances |
