import numpy as np
from glob import glob

from config import *

import submit_job

script_dir = f"{ROOT_DIR}/scripts/"
script_filename = "calc_clim_and_anom_era5.py"

lat_min_list = np.arange(-30.5, 30.5, 5.0)
lat_max_list = lat_min_list + 4.0
lat_max_list[-1] = 30.5

env_vars = {}
env_vars["FILE_NAME"] = "vo"
env_vars["VAR_NAME"] = "vo"
env_vars["YEAR_START"] = 1984
env_vars["YEAR_END"] = 2016
env_vars["SPD"] = 8
env_vars["DIRI"] = f"{SCRATCH_ERA5_DIR}/{env_vars['FILE_NAME']}/tmp/"
env_vars["DIRO"] = env_vars["DIRI"]

pbs_dir = f"{ROOT_DIR}/pbs_scripts/"
ncpus = 1
mem = 50
jobfsmem = 1
queue = "normal"
project = "if69"
walltime = "00:15:00"
storage = "gdata/xp65+scratch/k10"
command = f"""cd {ROOT_DIR}

source env.sh

python3 {script_dir}{script_filename}"""

# Following block of codes is for pressure-levels variables. Comment them if you want to use single-levels variables.

levels = [100, 125, 150, 175, 200, 225, 250, 300, 350, 400, 450, 500, 550, 600, 650, 700, 750, 775, 800, 825, 850, 875, 900, 925, 950, 975, 1000]

for level in levels:
    jobs_id = []
    
    env_vars["LEVEL"] = level
    
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
        
        files = sorted(glob(f"{env_vars['DIRO']}{env_vars['FILE_NAME']}.{env_vars['LEVEL']}.clim.and.anom.{env_vars['YEAR_START']}-{env_vars['YEAR_END']}.{lat_min_string}_{lat_max_string}.nc"))
        
        if len(files) != 0:
            continue
        
        name = f"{script_filename[:-3]}__{env_vars['FILE_NAME']}.{env_vars['LEVEL']}_{env_vars['YEAR_START']}-{env_vars['YEAR_END']}_{lat_min_string}_{lat_max_string}"
        pbs_filename = f"{name}.sh"
        
        pbs_script = submit_job.submit.create_pbs_script(pbs_dir, pbs_filename, ncpus, mem, jobfsmem, queue, project, walltime, storage, name, command)
        
        submit_job.submit.submit_job(env_vars, pbs_script, jobs_id)
    
    if len(jobs_id) != 0:
        submit_job.check.check_job_status(jobs_id)

# Following block of codes is for single-levels variables. Comment them if you want to use pressure-levels variables.
'''
jobs_id = []

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
    
    files = sorted(glob(f"{env_vars['DIRO']}{env_vars['FILE_NAME']}.clim.and.anom.{env_vars['YEAR_START']}-{env_vars['YEAR_END']}.{lat_min_string}_{lat_max_string}.nc"))
    
    if len(files) != 0:
        continue
    
    name = f"{script_filename[:-3]}__{env_vars['FILE_NAME']}_{env_vars['YEAR_START']}-{env_vars['YEAR_END']}_{lat_min_string}_{lat_max_string}"
    pbs_filename = f"{name}.sh"
    
    pbs_script = submit_job.submit.create_pbs_script(pbs_dir, pbs_filename, ncpus, mem, jobfsmem, queue, project, walltime, storage, name, command)
    
    submit_job.submit.submit_job(env_vars, pbs_script, jobs_id)

if len(jobs_id) != 0:
    submit_job.check.check_job_status(jobs_id)
'''
