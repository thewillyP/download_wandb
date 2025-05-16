#!/bin/bash
set -e
# Set WANDB environment variables
export WANDB_DIR=/wandb_data
export WANDB_CACHE_DIR=/wandb_data/.cache/wandb
export WANDB_CONFIG_DIR=/wandb_data/.config/wandb
export WANDB_DATA_DIR=/wandb_data/.cache/wandb-data/
export WANDB_ARTIFACT_DIR=/wandb_data/.artifacts

source ~/.secrets/env.sh

# Check inputs
if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Usage: $0 <project> <group_id>"
    exit 1
fi

PROJECT="$1"
GROUP_ID="$2"

python /workspace/download.py $PROJECT $GROUP_ID