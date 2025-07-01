import numpy as np
from glob import glob

from config import *

import submit_job

script_dir = f"{ROOT_DIR}/scripts/"
script_filename = "calc_mean_precipitation_in_deep_tropics_associated_with_mode.py"

year_start = 2000
year_end = 2016

years = np.arange(year_start, year_end + 1, 1, dtype=int)

env_vars = {}
env_vars["DIRO"] = "/scratch/k10/mr4682/data/GPM/3hr_mean_precipitation_associated_with_mode/"

pbs_dir = f"{ROOT_DIR}/pbs_scripts/"
ncpus = 1
mem = 75
jobfsmem = 1
queue = "normal"
project = "k10"
walltime = "04:00:00"
storage = "gdata/xp65+scratch/k10"
command = f"""cd {ROOT_DIR}

source env.sh

python3 {script_dir}{script_filename}
"""

jobs_id = []

for year in years:
    env_vars["YEAR"] = year
    
    files = sorted(glob(f"{env_vars['DIRO']}precipitation.in.deep.tropics.a.w.mode.{year:04d}.nc"))
    
    if len(files) != 0:
        continue

    name = f"{script_filename[:-3]}__{env_vars['YEAR']}"
    pbs_filename = f"{name}.sh"

    pbs_script = submit_job.submit.create_pbs_script(pbs_dir, pbs_filename, ncpus, mem, jobfsmem, queue, project, walltime, storage, name, command)

    submit_job.submit.submit_job(env_vars, pbs_script, jobs_id)

if len(jobs_id) != 0:
    submit_job.check.check_job_status(jobs_id)
