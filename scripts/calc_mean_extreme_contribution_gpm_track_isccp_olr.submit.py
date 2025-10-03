import numpy as np
from glob import glob

from config import *

import submit_job

script_dir = f"{ROOT_DIR}/scripts/"
script_filename = "calc_mean_extreme_contribution_gpm_track_isccp_olr.py"

lat_min_list = np.arange(-30.5, 30.5, 2.0)
lat_max_list = lat_min_list + 2.0
lat_max_list[-1] = 30.5

env_vars = {}
env_vars["FILE_NAME"] = "mean_intensity"
env_vars["VAR_NAME"] = "mean_intensity"
env_vars["PERCENTILE"] = 99
env_vars["DIRI"] = f"{SCRATCH_GPM_DIR}/3hr_{env_vars['FILE_NAME']}/extreme_contribution/"
env_vars["DIRO"] = f"{SCRATCH_GPM_DIR}/3hr_{env_vars['FILE_NAME']}/mean_extreme_contribution/"

pbs_dir = f"{ROOT_DIR}/pbs_scripts/"
ncpus = 1
mem = 190
jobfsmem = 1
queue = "normal"
project = "fy29"
walltime = "02:30:00"
storage = "gdata/xp65+scratch/k10"
command = f"""cd {ROOT_DIR}

source env.sh

python3 {script_dir}{script_filename}"""

mode_dict = {"Moisture Mode": "moisture.mode", "Mixed System": "mixed.system", "IG Wave": "ig.wave", "Eastward Moisture Mode": "eastward.moisture.mode", "Eastward Mixed System": "eastward.mixed.system", "Eastward IG Wave": "eastward.ig.wave", "Westward Moisture Mode": "westward.moisture.mode", "Westward Mixed System": "westward.mixed.system", "Westward IG Wave": "westward.ig.wave", "Tropical Cyclone": "tc"}

modes = ["Moisture Mode", "Mixed System", "IG Wave", "Eastward Moisture Mode", "Eastward Mixed System", "Eastward IG Wave", "Westward Moisture Mode", "Westward Mixed System", "Westward IG Wave", "Tropical Cyclone"]

for mode in modes[0:3]:
    jobs_id = []
    
    env_vars["MODE"] = mode
    
    for i in range(0, len(lat_min_list)):
        env_vars["LAT_MIN"] = lat_min_list[i]
        env_vars["LAT_MAX"] = lat_max_list[i]
        
        if lat_min_list[i] < 0:
            lat_min_string = f"{np.abs(lat_min_list[i] + 0.05):.2f}S"
        else:
            lat_min_string = f"{lat_min_list[i] + 0.05:.2f}N"
        if lat_max_list[i] < 0:
            lat_max_string = f"{np.abs(lat_max_list[i] - 0.05):.2f}S"
        else:
            lat_max_string = f"{lat_max_list[i] - 0.05:.2f}N"
        
        files = sorted(glob(f"{env_vars['DIRO']}mean.extreme.contribution.{env_vars['PERCENTILE']:d}p.{env_vars['FILE_NAME']}.{mode_dict[mode]}.{lat_min_string}-{lat_max_string}.nc"))
        name = f"{script_filename[:-3]}__{env_vars['PERCENTILE']:d}p_{env_vars['FILE_NAME']}_{mode_dict[mode]}_{lat_min_string}-{lat_max_string}"
        
        if len(files) != 0:
            continue
        
        pbs_filename = f"{name}.sh"
        
        pbs_script = submit_job.submit.create_pbs_script(pbs_dir, pbs_filename, ncpus, mem, jobfsmem, queue, project, walltime, storage, name, command)
        
        submit_job.submit.submit_job(env_vars, pbs_script, jobs_id)
    
    if len(jobs_id) != 0:
        submit_job.check.check_job_status(jobs_id)
