import numpy as np
from glob import glob

from config import *

import submit_job

script_dir = f"{ROOT_DIR}/scripts/"
script_filename = "calc_object_mean_precipitation.py"

year_start = 2000
year_end = 2016

years = np.arange(year_start, year_end + 1, 1, dtype=int)

env_vars = {}
env_vars["DIRO"] = "/scratch/k10/mr4682/data/track_isccp_olr/object_mean_precipitation/"

pbs_dir = f"{ROOT_DIR}/pbs_scripts/"
ncpus = 1
mem = 20
jobfsmem = 1
queue = "normal"
project = "k10"
walltime = "01:00:00"
storage = "gdata/xp65+scratch/k10"
command = f"""cd {ROOT_DIR}

source env.sh

python3 {script_dir}{script_filename}
"""

mode_dict = {"Moisture Mode": "moisture.mode", "Mixed System": "mixed.system", "IG Wave": "ig.wave", "Eastward Moisture Mode": "eastward.moisture.mode", "Eastward Mixed System": "eastward.mixed.system", "Eastward IG Wave": "eastward.ig.wave", "Westward Moisture Mode": "westward.moisture.mode", "Westward Mixed System": "westward.mixed.system", "Westward IG Wave": "westward.ig.wave", "Tropical Cyclone": "tc"}

modes = ["Moisture Mode", "Mixed System", "IG Wave", "Eastward Moisture Mode", "Eastward Mixed System", "Eastward IG Wave", "Westward Moisture Mode", "Westward Mixed System", "Westward IG Wave", "Tropical Cyclone"]

for mode in modes:
    env_vars["MODE"] = mode
    
    for year in years:
        jobs_id = []
        
        env_vars["YEAR"] = year
        
        if year == 2000:
            month_start = 6
        else:
            month_start = 1
        
        if year == 2016:
            month_end = 4
        else:
            month_end = 12
        
        for month in np.arange(month_start, month_end + 1, 1, dtype=int):
            env_vars["MONTH"] = month
            
            files = sorted(glob(f"{env_vars['DIRO']}object.mean.precipitation.{mode_dict[mode]}.{year:04d}{month:02d}.csv"))
            
            if len(files) != 0:
                continue
            
            name = f"{script_filename[:-3]}__{mode_dict[mode]}_{env_vars['YEAR']:04d}{env_vars['MONTH']:02d}"
            pbs_filename = f"{name}.sh"
            
            pbs_script = submit_job.submit.create_pbs_script(pbs_dir, pbs_filename, ncpus, mem, jobfsmem, queue, project, walltime, storage, name, command)
            
            submit_job.submit.submit_job(env_vars, pbs_script, jobs_id)
        
        if len(jobs_id) != 0:
            submit_job.check.check_job_status(jobs_id)
