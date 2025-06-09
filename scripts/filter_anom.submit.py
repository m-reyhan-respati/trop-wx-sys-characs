import numpy as np
from glob import glob

from config import *

import submit_job

script_dir = f"{ROOT_DIR}/scripts/"
script_filename = "filter_anom.py"

lat_min_list = np.arange(-30.5, 30.5, 2.0)
lat_max_list = lat_min_list + 1.75
lat_max_list[-1] = 30.5

env_vars = {}
env_vars["FILE_NAME"] = "u"
env_vars["VAR_NAME"] = "u"
env_vars["YEAR_START"] = 1984
env_vars["YEAR_END"] = 2016
env_vars["SPD"] = 8
env_vars["DIRI"] = f"/scratch/k10/mr4682/data/ERA5/{env_vars['FILE_NAME']}/filtered/"
env_vars["DIRO"] = env_vars["DIRI"]

pbs_dir = f"{ROOT_DIR}/pbs_scripts/"
ncpus = 1
mem = 100
jobfsmem = 1
queue = "normal"
project = "k10"
walltime = "01:00:00"
storage = "gdata/xp65+scratch/k10"
command = f"""cd {ROOT_DIR}

source env.sh

python3 {script_dir}{script_filename}"""

mode_dict = {"Moisture Mode": "moisture.mode", "Mixed System": "mixed.system", "IG Wave": "ig.wave", "Eastward Moisture Mode": "eastward.moisture.mode", "Eastward Mixed System": "eastward.mixed.system", "Eastward IG Wave": "eastward.ig.wave", "Westward Moisture Mode": "westward.moisture.mode", "Westward Mixed System": "westward.mixed.system", "Westward IG Wave": "westward.ig.wave"}

modes = ["Moisture Mode", "Mixed System", "IG Wave", "Eastward Moisture Mode", "Eastward Mixed System", "Eastward IG Wave", "Westward Moisture Mode", "Westward Mixed System", "Westward IG Wave"]

# Following block of codes is for pressure-levels variables. Comment them if you want to use single-levels variables.

levels = [850]

for level in levels:
    jobs_id = []

    env_vars["FILE_NAME"] = f"{env_vars['FILE_NAME']}.{level:d}"
    
    for mode in modes:
        env_vars["MODE"] = mode
        
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
    
            files = sorted(glob(f"{env_vars['DIRO']}{env_vars['FILE_NAME']}.{mode_dict[mode]}.{env_vars['YEAR_START']}-{env_vars['YEAR_END']}.{lat_min_string}_{lat_max_string}*"))
    
            if len(files) != 0:
                continue
    
            name = f"{script_filename[:-3]}__{env_vars['FILE_NAME']}_{mode_dict[mode]}_{env_vars['YEAR_START']}-{env_vars['YEAR_END']}_{lat_min_string}_{lat_max_string}"
            pbs_filename = f"{name}.sh"
    
            pbs_script = submit_job.submit.create_pbs_script(pbs_dir, pbs_filename, ncpus, mem, jobfsmem, queue, project, walltime, storage, name, command)
    
            submit_job.submit.submit_job(env_vars, pbs_script, jobs_id)
    
    submit_job.check.check_job_status(jobs_id)

# Following block of codes is for single-levels variables. Comment them if you want to use pressure-levels variables.
'''
jobs_id = []

for mode in modes:
    env_vars["MODE"] = mode
    
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
    
        files = sorted(glob(f"{env_vars['DIRO']}{env_vars['FILE_NAME']}.{mode_dict[mode]}.{env_vars['YEAR_START']}-{env_vars['YEAR_END']}.{lat_min_string}_{lat_max_string}*"))
    
        if len(files) != 0:
            continue
    
        name = f"{script_filename[:-3]}__{env_vars['FILE_NAME']}_{mode_dict[mode]}_{env_vars['YEAR_START']}-{env_vars['YEAR_END']}_{lat_min_string}_{lat_max_string}"
        pbs_filename = f"{name}.sh"
    
        pbs_script = submit_job.submit.create_pbs_script(pbs_dir, pbs_filename, ncpus, mem, jobfsmem, queue, project, walltime, storage, name, command)
    
        submit_job.submit.submit_job(env_vars, pbs_script, jobs_id)
    
submit_job.check.check_job_status(jobs_id)
'''
