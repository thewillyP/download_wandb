#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --mem=16G
#SBATCH --time=00:15:00
#SBATCH --job-name=build
#SBATCH --error=_build.err
#SBATCH --output=_build.log
#SBATCH --cpus-per-task=10
#SBATCH --mail-type=END
#SBATCH --mail-user=wlp9800@nyu.edu


IMAGE=wandb_download
DOCKER_URL="docker://thewillyp/${IMAGE}"

# Build the Singularity image
singularity build --force /scratch/${USER}/images/${IMAGE}.sif ${DOCKER_URL}
# Create the overlay
singularity overlay create --size 20480 /scratch/${USER}/images/${IMAGE}.sif

# 10 GiB
