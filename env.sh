#!/bin/bash

# Define some basic environmental variables before launching the suite

# Load the analysis3 conda environment
module use /g/data/xp65/public/modules
module load conda/analysis3-25.02
source /gdata/gb02/mr4682/tobac_env/bin/activate

# Root directory for this repo
export ROOT=/home/565/${USER}/trop-wx-sys-characs	# Change this to match where you clone this repo
export MODULES=${ROOT}/filter_mode
export MODULES=${ROOT}/track_cell
export MODULES=${ROOT}/submit_job

# Append to our python path
export PYTHONPATH=${MODULES}:${PYTHONPATH}
