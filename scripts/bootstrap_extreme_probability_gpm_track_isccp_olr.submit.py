import numpy as np
from glob import glob

from config import *

import submit_job

script_dir = f"{ROOT_DIR}/scripts/"
script_filename = "bootstrap_extreme_probability_gpm_track_isccp_olr.py"

lat_min_list = np.arange(-15.0, 15.0, 0.5)
lat_max_list = lat_min_list + 0.4
lat_max_list[-1] = 15.0

lagmax = 0

lags = np.arange(-lagmax, lagmax + 1.0, 1.0) * 3.0

env_vars = {}
env_vars["FILE_NAME"] = "mean_intensity"
env_vars["VAR_NAME"] = "mean_intensity"
env_vars["PERCENTILE"] = 99
env_vars["RAINING"] = 0
env_vars["DIRI"] = f"{SCRATCH_GPM_DIR}/3hr_{env_vars['FILE_NAME']}/"
env_vars["DIRO"] = f"{env_vars['DIRI']}extreme_probability/"

pbs_dir = f"{ROOT_DIR}/pbs_scripts/"
ncpus = 1
mem = 25
jobfsmem = 1
queue = "normal"
project = "fy29"
walltime = "18:00:00"
storage = "gdata/xp65+scratch/k10"
command = f"""cd {ROOT_DIR}

source env.sh

python3 {script_dir}{script_filename}"""

mode_dict = {"Moisture Mode": "moisture.mode", "Mixed System": "mixed.system", "IG Wave": "ig.wave", "Eastward Moisture Mode": "eastward.moisture.mode", "Eastward Mixed System": "eastward.mixed.system", "Eastward IG Wave": "eastward.ig.wave", "Westward Moisture Mode": "westward.moisture.mode", "Westward Mixed System": "westward.mixed.system", "Westward IG Wave": "westward.ig.wave", "Tropical Cyclone": "tc"}

modes = ["Moisture Mode", "Mixed System", "IG Wave", "Eastward Moisture Mode", "Eastward Mixed System", "Eastward IG Wave", "Westward Moisture Mode", "Westward Mixed System", "Westward IG Wave", "Tropical Cyclone"]

for mode in modes[2:3]:
    jobs_id = []
    
    env_vars["MODE"] = mode
    
    for lag in lags:
        env_vars["LAG"] = lag
        
        for i in range(0, len(lat_min_list)):
            env_vars["LAT_MIN"] = lat_min_list[i]
            env_vars["LAT_MAX"] = lat_max_list[i]
            
            if lat_min_list[i] < 0:
                lat_min_string = f"{np.abs(lat_min_list[i]):.2f}S"
            else:
                lat_min_string = f"{lat_min_list[i]:.2f}N"
            if lat_max_list[i] < 0:
                lat_max_string = f"{np.abs(lat_max_list[i]):.2f}S"
            else:
                lat_max_string = f"{lat_max_list[i]:.2f}N"
            
            if env_vars["RAINING"] == 1:
                files = sorted(glob(f"{env_vars['DIRO']}raining/bootstrapped.extreme.probability.{env_vars['PERCENTILE']:d}p.raining.{env_vars['FILE_NAME']}.{mode_dict[mode]}.lag.{env_vars['LAG']:.0f}.{lat_min_string}-{lat_max_string}.nc"))
                name = f"{script_filename[:-3]}__{env_vars['PERCENTILE']:d}p_raining_{env_vars['FILE_NAME']}_{mode_dict[mode]}_lag_{env_vars['LAG']:.0f}_{lat_min_string}-{lat_max_string}"
            else:
                files = sorted(glob(f"{env_vars['DIRO']}bootstrapped.extreme.probability.{env_vars['PERCENTILE']:d}p.{env_vars['FILE_NAME']}.{mode_dict[mode]}.lag.{env_vars['LAG']:.0f}.{lat_min_string}-{lat_max_string}.nc"))
                name = f"{script_filename[:-3]}__{env_vars['PERCENTILE']:d}p_{env_vars['FILE_NAME']}_{mode_dict[mode]}_lag_{env_vars['LAG']:.0f}_{lat_min_string}-{lat_max_string}"
            
            if len(files) != 0:
                continue
            
            pbs_filename = f"{name}.sh"
            
            pbs_script = submit_job.submit.create_pbs_script(pbs_dir, pbs_filename, ncpus, mem, jobfsmem, queue, project, walltime, storage, name, command)
            
            submit_job.submit.submit_job(env_vars, pbs_script, jobs_id)
    
    if len(jobs_id) != 0:
        submit_job.check.check_job_status(jobs_id)
