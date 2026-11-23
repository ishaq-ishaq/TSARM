#!/usr/bin/env bash
# Launch, watch, collect and delete the AWS cluster for experiment E2
# (docs/aws.md). Run in AWS CloudShell:
#
#     bash aws_e2_launch.sh launch    # key, firewall, 1 master + 7 workers (16 vCPUs)
#     bash aws_e2_launch.sh status    # nodes and the master's progress log
#     bash aws_e2_launch.sh fetch     # copy ~/e2-results.tgz from the master
#     bash aws_e2_launch.sh cleanup   # terminate every node
#
# The nodes configure themselves (scripts/cluster/aws_e2_node.sh) and the
# master runs E2 without further input.
set -euo pipefail
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-eu-north-1}"   # the Free plan's assigned region
TYPE="${TYPE:-m7i-flex.large}"        # 2 vCPU, 8 GB, free-tier eligible
WORKERS="${WORKERS:-7}"
BRANCH="${BRANCH:-main}"
REPO="https://github.com/ishaq-ishaq/TSARM.git"
KEY="$HOME/.ssh/tsarm.pem"
ENV_FILE="$HOME/tsarm-aws.env"
SSH="ssh -i $KEY -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10"

ids() {  # ids <name-tag> <states>
    aws ec2 describe-instances --filters "Name=tag:Name,Values=$1" "Name=instance-state-name,Values=$2" \
        --query 'Reservations[].Instances[].InstanceId' --output text
}
master_public_ip() {
    aws ec2 describe-instances --filters Name=tag:Name,Values=tsarm-master \
        Name=instance-state-name,Values=running \
        --query 'Reservations[0].Instances[0].PublicIpAddress' --output text
}
user_data() {  # user_data <env assignments>
    cat <<EOF
#!/bin/bash
sudo -u ubuntu -H bash -c 'cd ~ && git clone -q -b $BRANCH $REPO && $1 bash TSARM/scripts/cluster/aws_e2_node.sh > ~/e2-node.log 2>&1'
EOF
}

case "${1:-}" in
launch)
    if [ -n "$(ids 'tsarm-*' pending,running)" ]; then
        echo "TSARM nodes are already running; use: status, fetch or cleanup"; exit 1
    fi
    echo "Region $AWS_DEFAULT_REGION, $((WORKERS + 1)) x $TYPE, branch $BRANCH"
    aws ec2 describe-instance-types --instance-types "$TYPE" \
        --query 'InstanceTypes[0].[InstanceType,FreeTierEligible]' --output text
    mkdir -p "$HOME/.ssh"
    if [ ! -s "$KEY" ]; then
        aws ec2 delete-key-pair --key-name tsarm >/dev/null 2>&1 || true
        aws ec2 create-key-pair --key-name tsarm --query KeyMaterial --output text > "$KEY"
        chmod 600 "$KEY"
    fi
    SG=$(aws ec2 describe-security-groups --filters Name=group-name,Values=tsarm \
        --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || true)
    if [ -z "$SG" ] || [ "$SG" = None ]; then
        SG=$(aws ec2 create-security-group --group-name tsarm --description "TSARM Spark cluster" \
            --query GroupId --output text)
        aws ec2 authorize-security-group-ingress --group-id "$SG" --protocol tcp --port 22 --cidr 0.0.0.0/0 >/dev/null
        aws ec2 authorize-security-group-ingress --group-id "$SG" --protocol -1 --source-group "$SG" >/dev/null
    fi
    AMI=$(aws ssm get-parameter \
        --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id \
        --query Parameter.Value --output text)
    printf 'SG=%s\nAMI=%s\n' "$SG" "$AMI" > "$ENV_FILE"
    DISK='[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":30,"VolumeType":"gp3","DeleteOnTermination":true}}]'

    user_data "ROLE=master WORKERS=$WORKERS" > /tmp/tsarm-master.sh
    M=$(aws ec2 run-instances --image-id "$AMI" --instance-type "$TYPE" --count 1 \
        --key-name tsarm --security-group-ids "$SG" --block-device-mappings "$DISK" \
        --user-data file:///tmp/tsarm-master.sh \
        --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=tsarm-master}]' \
        --query 'Instances[0].InstanceId' --output text)
    aws ec2 wait instance-running --instance-ids "$M"
    MIP=$(aws ec2 describe-instances --instance-ids "$M" \
        --query 'Reservations[0].Instances[0].PrivateIpAddress' --output text)
    echo "master $M at $MIP"

    user_data "ROLE=worker MASTER_IP=$MIP" > /tmp/tsarm-worker.sh
    aws ec2 run-instances --image-id "$AMI" --instance-type "$TYPE" --count "$WORKERS" \
        --key-name tsarm --security-group-ids "$SG" --block-device-mappings "$DISK" \
        --instance-initiated-shutdown-behavior terminate \
        --user-data file:///tmp/tsarm-worker.sh \
        --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=tsarm-worker}]' \
        --query 'Instances[].InstanceId' --output text
    echo "Launched. The nodes set themselves up (about 20-30 min), then E2 runs for a few hours."
    echo "Check with: bash $0 status"
    ;;
status)
    aws ec2 describe-instances --filters 'Name=tag:Name,Values=tsarm-*' \
        Name=instance-state-name,Values=pending,running,stopping,stopped \
        --query 'Reservations[].Instances[].[Tags[?Key==`Name`]|[0].Value,State.Name,PublicIpAddress,PrivateIpAddress]' \
        --output table
    IP=$(master_public_ip)
    if [ "$IP" != None ] && [ -n "$IP" ]; then
        $SSH "ubuntu@$IP" 'tail -n 15 ~/e2-node.log; echo; free -g | head -2; uptime' || true
    fi
    ;;
fetch)
    IP=$(master_public_ip)
    scp -i "$KEY" -o StrictHostKeyChecking=accept-new "ubuntu@$IP:e2-results.tgz" "$HOME/tsarm-aws-results.tgz"
    tar tzf "$HOME/tsarm-aws-results.tgz"
    echo "Saved $HOME/tsarm-aws-results.tgz (Actions -> Download file)"
    ;;
cleanup)
    ALL=$(ids 'tsarm-*' pending,running,stopping,stopped)
    if [ -n "$ALL" ]; then aws ec2 terminate-instances --instance-ids $ALL --query 'TerminatingInstances[].InstanceId' --output text; fi
    echo "Terminated. Disks are deleted with the instances."
    ;;
*)
    sed -n 2,11p "$0"; exit 1 ;;
esac
