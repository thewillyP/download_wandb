#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --mem=14G
#SBATCH --time=06:00:00
#SBATCH --job-name=oho_experiments
#SBATCH --gres=gpu:0
#SBATCH --cpus-per-task=8
#SBATCH --output="/vast/wlp9800/logs/%x-%A-%a.out"
#SBATCH --error="/vast/wlp9800/logs/%x-%A-%a.err"

set -e
if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Usage: $0 <project> <group_id>"
    exit 1
fi

PROJECT="$1"
GROUP_ID="$2"

source ~/.secrets/env.sh

singularity run --nv --containall --cleanenv --writable-tmpfs \
  --bind /home/${USER}/.secrets/env.sh \
  --bind /scratch/${USER}/wandb:/wandb_data \
  --bind /scratch/${USER}/space:/dump \
  /scratch/${USER}/images/wandb_download.sif $PROJECT $GROUP_ID
