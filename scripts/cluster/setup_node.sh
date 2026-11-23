#!/usr/bin/env bash
# One-time setup of a TSARM Spark node: an Oracle Cloud Always Free Ampere A1
# VM, or a Google Cloud, Azure or AWS VM (Ubuntu 22.04/24.04, arm64 or amd64). Run on EVERY node:
#     bash setup_node.sh
# Idempotent: safe to re-run.
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive   # no prompts (e.g. iptables-persistent) when run over ssh

REPO_URL="${REPO_URL:-https://github.com/ishaq-ishaq/TSARM.git}"
TSARM_DIR="${TSARM_DIR:-$HOME/TSARM}"
VENV="${VENV:-$HOME/tsarm-venv}"
SUBNET="${SUBNET:-10.0.0.0/16}"   # the VCN's private address range

echo "==> System packages (Java 17, Python, parallel bzip2, rsync)"
sudo apt-get update -y
sudo apt-get install -y openjdk-17-jre-headless python3-venv python3-pip \
    lbzip2 pigz rsync git

echo "==> Allow Spark traffic between nodes on the private subnet"
# Oracle's Ubuntu images ship restrictive iptables rules; open the private
# subnet only (nothing is exposed to the internet).
if ! sudo iptables -C INPUT -s "$SUBNET" -j ACCEPT 2>/dev/null; then
    sudo iptables -I INPUT -s "$SUBNET" -j ACCEPT
    sudo apt-get install -y iptables-persistent
    sudo netfilter-persistent save
fi

echo "==> Python environment"
python3 -m venv "$VENV"
"$VENV/bin/pip" install --upgrade pip
"$VENV/bin/pip" install "pyspark==4.0.3" "rdflib==7.6.0" "pandas>=2.2.0" "scipy>=1.10" "pytest>=8.0"

echo "==> TSARM code"
if [ -d "$TSARM_DIR/.git" ]; then git -C "$TSARM_DIR" pull --ff-only; else git clone "$REPO_URL" "$TSARM_DIR"; fi

JAVA_HOME_DIR="$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")"
cat > "$HOME/tsarm-env.sh" <<ENV
export JAVA_HOME="$JAVA_HOME_DIR"
export SPARK_HOME="\$($VENV/bin/python -c 'import pyspark,os;print(os.path.dirname(pyspark.__file__))')"
export PATH="$VENV/bin:\$SPARK_HOME/bin:\$PATH"
export TSARM_DIR="$TSARM_DIR"
ENV
echo "==> Done. Run:  source ~/tsarm-env.sh"
