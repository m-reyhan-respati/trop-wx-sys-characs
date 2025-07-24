import numpy as np
from glob import glob

from config import *

import submit_job

script_dir = f"{ROOT_DIR}/scripts/"
script_filename = "calc_mask_track_isccp_olr.py"

year_start = 1985
year_end = 2016

years = np.arange(year_start, year_end + 1, 1, dtype=int)

env_vars = {}
env_vars["DIRI"] = f"{SCRATCH_TRACK_DIR}/"
env_vars["DIRO"] = f"{SCRATCH_TRACK_DIR}/mask/"

pbs_dir = f"{ROOT_DIR}/pbs_scripts/"
ncpus = 1
mem = 5
jobfsmem = 1
queue = "normal"
project = "k10"
walltime = "00:15:00"
storage = "gdata/xp65+scratch/k10"
command = f"""cd {ROOT_DIR}

source env.sh

python3 {script_dir}{script_filename}"""

mode_dict = {"Moisture Mode": "moisture.mode", "Mixed System": "mixed.system", "IG Wave": "ig.wave", "Eastward Moisture Mode": "eastward.moisture.mode", "Eastward Mixed System": "eastward.mixed.system", "Eastward IG Wave": "eastward.ig.wave", "Westward Moisture Mode": "westward.moisture.mode", "Westward Mixed System": "westward.mixed.system", "Westward IG Wave": "westward.ig.wave", "Tropical Cyclone": "tc", "Raw Anomaly": "anom"}

modes = ["Moisture Mode", "Mixed System", "IG Wave", "Eastward Moisture Mode", "Eastward Mixed System", "Eastward IG Wave", "Westward Moisture Mode", "Westward Mixed System", "Westward IG Wave", "Tropical Cyclone", "Raw Anomaly"]

for year in years:
    env_vars["YEAR"] = year
    
    jobs_id = []
    
    for mode in modes:
        env_vars["MODE"] = mode
        
        files = sorted(glob(f"{env_vars['DIRO']}mask.track.txuptp.{mode_dict[mode]}.{env_vars['YEAR']}.nc"))
        
        if len(files) != 0:
            continue
        
        name = f"{script_filename[:-3]}__{mode_dict[mode]}_{env_vars['YEAR']}"
        pbs_filename = f"{name}.sh"
        
        pbs_script = submit_job.submit.create_pbs_script(pbs_dir, pbs_filename, ncpus, mem, jobfsmem, queue, project, walltime, storage, name, command)
        
        submit_job.submit.submit_job(env_vars, pbs_script, jobs_id)
    
    if len(jobs_id) != 0:
        submit_job.check.check_job_status(jobs_id, time_delay=60)
