#!/bin/bash
set -e

source ~/.secrets/env.sh

# Check inputs
if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Usage: $0 <project> <group_id>"
    exit 1
fi

PROJECT="$1"
GROUP_ID="$2"

python /workspace/download.py $PROJECT $GROUP_ID