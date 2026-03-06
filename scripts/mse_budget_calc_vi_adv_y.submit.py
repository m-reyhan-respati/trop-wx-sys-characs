import numpy as np
from glob import glob

from config import *

import submit_job

script_dir = f"{ROOT_DIR}/scripts/"
script_filename = "mse_budget_calc_vi_adv_y.py"

year_start = 1984
year_end = 2016

years = np.arange(year_start, year_end + 1, 1, dtype=int)

env_vars = {}
env_vars["DIRI"] = f"{SCRATCH_ERA5_DIR}/"
env_vars["DIRO"] = f"{SCRATCH_ERA5_DIR}/mse/budget/adv_y/tmp/"

pbs_dir = f"{ROOT_DIR}/pbs_scripts/"
ncpus = 1
mem = 110
jobfsmem = 1
queue = "normal"
project = "if69"
walltime = "01:00:00"
storage = "gdata/xp65+scratch/k10"
command = f"""cd {ROOT_DIR}

source env.sh

python3 {script_dir}{script_filename}"""

jobs_id = []

for year in years:
    env_vars["YEAR"] = year

    files = sorted(glob(f"{env_vars['DIRO']}vi_mse_adv_y.{env_vars['YEAR']}.nc"))

    if len(files) != 0:
        continue

    name = f"{script_filename[:-3]}__{env_vars['YEAR']}"
    pbs_filename = f"{name}.sh"

    pbs_script = submit_job.submit.create_pbs_script(pbs_dir, pbs_filename, ncpus, mem, jobfsmem, queue, project, walltime, storage, name, command)

    submit_job.submit.submit_job(env_vars, pbs_script, jobs_id)

if len(jobs_id) != 0:
    submit_job.check.check_job_status(jobs_id)
