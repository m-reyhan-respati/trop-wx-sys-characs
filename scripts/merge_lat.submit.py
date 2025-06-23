from glob import glob

from config import *

import submit_job

script_dir = f"{ROOT_DIR}/scripts/"
script_filename = "merge_lat.py"

var = "precipitation"

env_vars = {}
env_vars["FILEI_TAIL"] = ".clim.and.anom.2000-2016"
env_vars["DIRI"] = f"{SCRATCH_GPM_DIR}/3hr_mean_{var}/clim.and.anom/tmp/"
env_vars["DIRO"] = f"{SCRATCH_GPM_DIR}/3hr_mean_{var}/clim.and.anom/"
env_vars["LAT_NAME"] = "lat"

pbs_dir = f"{ROOT_DIR}/pbs_scripts/"
ncpus = 1
mem = 900
jobfsmem = 1
queue = "hugemem"
project = "gb02"
walltime = "12:00:00"
storage = "gdata/xp65+scratch/k10"
command = f"""cd {ROOT_DIR}

source env.sh

python3 {script_dir}{script_filename}"""


'''
levels = [100, 125, 150, 175, 200, 225, 250, 300, 350, 400, 450, 500, 550, 600, 650, 700, 750, 775, 800, 825, 850, 875, 900, 925, 950, 975, 1000]

jobs_id = []

for level in levels:
    env_vars["FILEI"] = f"{var}.{level:d}"
    env_vars["FILEO"] = f"{env_vars['FILEI']}{env_vars['FILEI_TAIL']}.nc"
    
    files = sorted(glob(f"{env_vars['DIRO']}{env_vars['FILEO']}"))
    
    if len(files) != 0:
        continue
    
    name = f"{script_filename[:-3]}__{env_vars['FILEI']}{env_vars['FILEI_TAIL']}"
    
    pbs_filename = f"{name}.sh"
    
    pbs_script = submit_job.submit.create_pbs_script(pbs_dir, pbs_filename, ncpus, mem, jobfsmem, queue, project, walltime, storage, name, command)
    
    submit_job.submit.submit_job(env_vars, pbs_script, jobs_id)

if len(jobs_id) != 0:
    submit_job.check.check_job_status(jobs_id)
'''




jobs_id = []

env_vars["FILEI"] = var
env_vars["FILEO"] = f"{env_vars['FILEI']}{env_vars['FILEI_TAIL']}.nc"

files = sorted(glob(f"{env_vars['DIRO']}{env_vars['FILEO']}"))

if len(files) != 0:
    sys.exit()

name = f"{script_filename[:-3]}__{env_vars['FILEI']}{env_vars['FILEI_TAIL']}"

pbs_filename = f"{name}.sh"

pbs_script = submit_job.submit.create_pbs_script(pbs_dir, pbs_filename, ncpus, mem, jobfsmem, queue, project, walltime, storage, name, command)

submit_job.submit.submit_job(env_vars, pbs_script, jobs_id)

if len(jobs_id) != 0:
    submit_job.check.check_job_status(jobs_id)

