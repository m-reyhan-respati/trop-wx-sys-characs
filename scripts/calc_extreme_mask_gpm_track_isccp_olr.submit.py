import numpy as np
import time
from glob import glob

from config import *

import submit_job

script_dir = f"{ROOT_DIR}/scripts/"
script_filename = "calc_extreme_mask_gpm_track_isccp_olr.py"

year_start = 2000
year_end = 2016

years = np.arange(year_start, year_end + 1, 1, dtype=int)

env_vars = {}
env_vars["FILE_NAME"] = "precipitation"
env_vars["VAR_NAME"] = "precipitation"
env_vars["DIRI"] = f"{SCRATCH_GPM_DIR}/3hr_mean_{env_vars['FILE_NAME']}/"
env_vars["DIRO"] = f"{env_vars['DIRI']}extreme_mask/"

pbs_dir = f"{ROOT_DIR}/pbs_scripts/"
ncpus = 48
mem = 40
jobfsmem = 1
queue = "normal"
project = "fy29"
walltime = "01:00:00"
storage = "gdata/xp65+scratch/k10"
command = f"""cd {ROOT_DIR}

source env.sh

python3 {script_dir}{script_filename}"""

mode_dict = {"Moisture Mode": "moisture.mode", "Mixed System": "mixed.system", "IG Wave": "ig.wave", "Eastward Moisture Mode": "eastward.moisture.mode", "Eastward Mixed System": "eastward.mixed.system", "Eastward IG Wave": "eastward.ig.wave", "Westward Moisture Mode": "westward.moisture.mode", "Westward Mixed System": "westward.mixed.system", "Westward IG Wave": "westward.ig.wave", "Tropical Cyclone": "tc"}

modes = ["Moisture Mode", "Mixed System", "IG Wave", "Eastward Moisture Mode", "Eastward Mixed System", "Eastward IG Wave", "Westward Moisture Mode", "Westward Mixed System", "Westward IG Wave", "Tropical Cyclone"]

for year in years:
    jobs_id = []
    
    env_vars["YEAR"] = year
    
    if year == 2000:
        month_start = 6
    else:
        month_start = 1
    
    month_end = 12
    
    for month in np.arange(month_start, month_end + 1, 1, dtype=int):
        env_vars["MONTH"] = month
        
        for mode in modes[9:10]:
            env_vars["MODE"] = mode
            
            files = sorted(glob(f"{env_vars['DIRO']}extreme.mask.{env_vars['FILE_NAME']}.{mode_dict[mode]}.{env_vars['YEAR']:04d}{env_vars['MONTH']:02d}.nc"))
            name = f"{script_filename[:-3]}__{env_vars['FILE_NAME']}_{mode_dict[mode]}_{env_vars['YEAR']:04d}{env_vars['MONTH']:02d}"
            
            if len(files) != 0:
                continue
            
            pbs_filename = f"{name}.sh"
            
            pbs_script = submit_job.submit.create_pbs_script(pbs_dir, pbs_filename, ncpus, mem, jobfsmem, queue, project, walltime, storage, name, command)
            
            submit_job.submit.submit_job(env_vars, pbs_script, jobs_id)
        
    if len(jobs_id) != 0:
        submit_job.check.check_job_status(jobs_id)
