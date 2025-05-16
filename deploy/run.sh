#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --mem=14G
#SBATCH --time=06:00:00
#SBATCH --job-name=download_wandb
#SBATCH --gres=gpu:0
#SBATCH --cpus-per-task=8
#SBATCH --output="/vast/wlp9800/logs/%x-%A-%a.out"
#SBATCH --error="/vast/wlp9800/logs/%x-%A-%a.err"

set -e

if [ -z "$PROJECT" ]; then
    echo "Error: WANDB_SWEEP_ID environment variable must be set!"
    exit 1
fi

if [ -z "$GROUP_ID" ]; then
    echo "Error: VARIANT environment variable must be set!"
    exit 1
fi

source ~/.secrets/env.sh

singularity run --containall --cleanenv --writable-tmpfs \
  --bind /home/${USER}/.secrets/env.sh \
  --bind /scratch/${USER}/wandb:/wandb_data \
  --bind /scratch/${USER}/space:/dump \
  /scratch/${USER}/images/download_wandb.sif $PROJECT $GROUP_ID

