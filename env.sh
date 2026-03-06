#!/bin/bash

# Define some basic environmental variables before launching the suite

# Load the analysis3 conda environment
module purge
module use /g/data/xp65/public/modules
module load conda/analysis3-26.02
source /home/565/mr4682/tobac_env/bin/activate

# Root directory for this repo
export ROOT=/home/565/${USER}/trop-wx-sys-characs	# Change this to match where you clone this repo

# Append modules to our python path
export MODULES=${ROOT}/budget
export PYTHONPATH=${MODULES}:${PYTHONPATH}

export MODULES=${ROOT}/filter_mode
export PYTHONPATH=${MODULES}:${PYTHONPATH}

export MODULES=${ROOT}/track_cell
export PYTHONPATH=${MODULES}:${PYTHONPATH}

export MODULES=${ROOT}/submit_job
export PYTHONPATH=${MODULES}:${PYTHONPATH}
