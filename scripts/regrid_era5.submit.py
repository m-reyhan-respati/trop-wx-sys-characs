import numpy as np
from glob import glob

from config import *

import submit_job

script_dir = f"{ROOT_DIR}/scripts/"
script_filename = "regrid_era5.py"

year_start = 1984
year_end = 2016

years = np.arange(year_start, year_end + 1, 1, dtype=int)

env_vars = {}
env_vars["FOLDER_NAME"] = "d"
env_vars["VAR_NAME"] = "d"
env_vars["LEVEL_TYPE"] = "pressure-levels"
env_vars["LAT_MIN"] = -30.5
env_vars["LAT_MAX"] = -env_vars["LAT_MIN"]
env_vars["LON_MIN"] = 0.5
env_vars["LON_MAX"] = 359.5
env_vars["SPD"] = 8
env_vars["RES_NEW"] = 1.0
env_vars["DIRO"] = f"{SCRATCH_ERA5_DIR}/{env_vars['FOLDER_NAME']}/tmp/"

pbs_dir = f"{ROOT_DIR}/pbs_scripts/"
ncpus = 48
mem = 75
jobfsmem = 1
queue = "normal"
project = "k10"
walltime = "01:00:00"
storage = "gdata/xp65+gdata/rt52+scratch/k10"
command = f"""cd {ROOT_DIR}

source env.sh

python3 {script_dir}{script_filename}"""

# Following block of codes is for pressure-levels variables. Comment them if you want to use single-levels variables.

levels = [100, 125, 150, 175, 200, 225, 250, 300, 350, 400, 450, 500, 550, 600, 650, 700, 750, 775, 800, 825, 850, 875, 900, 925, 950, 975, 1000]

for level in levels:
    jobs_id = []

    env_vars["LEVEL"] = level

    for year in years:
        env_vars["YEAR"] = year

        files = sorted(glob(f"{env_vars['DIRO']}{env_vars['FOLDER_NAME']}.{env_vars['LEVEL']}.{env_vars['YEAR']}.nc"))

        if len(files) != 0:
            continue

        name = f"{script_filename[:-3]}__{env_vars['FOLDER_NAME']}.{env_vars['LEVEL']}_{env_vars['YEAR']}"
        pbs_filename = f"{name}.sh"

        pbs_script = submit_job.submit.create_pbs_script(pbs_dir, pbs_filename, ncpus, mem, jobfsmem, queue, project, walltime, storage, name, command)

        submit_job.submit.submit_job(env_vars, pbs_script, jobs_id)

    if len(jobs_id) != 0:
        submit_job.check.check_job_status(jobs_id)

# Following block of codes is for single-levels variables. Comment them if you want to use pressure-levels variables.
'''
jobs_id = []

for year in years:
    env_vars["YEAR"] = year
    
    files = sorted(glob(f"{env_vars['DIRO']}{env_vars['FOLDER_NAME']}.{env_vars['YEAR']}.nc"))
    
    if len(files) != 0:
        continue
    
    name = f"{script_filename[:-3]}__{env_vars['FOLDER_NAME']}_{env_vars['YEAR']}"
    pbs_filename = f"{name}.sh"
    
    pbs_script = submit_job.submit.create_pbs_script(pbs_dir, pbs_filename, ncpus, mem, jobfsmem, queue, project, walltime, storage, name, command)
    
    submit_job.submit.submit_job(env_vars, pbs_script, jobs_id)

submit_job.check.check_job_status(jobs_id)
'''
