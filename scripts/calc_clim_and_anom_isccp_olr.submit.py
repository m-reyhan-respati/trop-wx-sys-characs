import numpy as np
from glob import glob

from config import *

import submit_job

script_dir = f"{ROOT_DIR}/scripts/"
script_filename = "calc_clim_and_anom_isccp_olr.py"

lat_min_list = np.arange(-30.5, 30.5, 5.0)
lat_max_list = lat_min_list + 4.0
lat_max_list[-1] = 30.5

env_vars = {}
env_vars["YEAR_START"] = 1984
env_vars["YEAR_END"] = 2016
env_vars["DIRO"] = f"/scratch/k10/mr4682/data/ISCCP/txuptp/tmp/"

pbs_dir = f"{ROOT_DIR}/pbs_scripts/"
ncpus = 48
mem = 100
jobfsmem = 1
queue = "normal"
project = "k10"
walltime = "06:00:00"
storage = "gdata/xp65+gdata/k10+scratch/k10"
command = f"""cd {ROOT_DIR}

source env.sh

python3 {script_dir}{script_filename}"""

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

    name = f"{script_filename[:-3]}__{env_vars['YEAR_START']}-{env_vars['YEAR_END']}_{lat_min_string}_{lat_max_string}"
    pbs_filename = f"{name}.sh"
    
    files = sorted(glob(f"{env_vars['DIRO']}*.{env_vars['YEAR_START']}-{env_vars['YEAR_END']}.{lat_min_string}_{lat_max_string}.*"))
    
    if len(files) != 0:
        continue

    pbs_script = submit_job.submit.create_pbs_script(pbs_dir, pbs_filename, ncpus, mem, jobfsmem, queue, project, walltime, storage, name, command)

    submit_job.submit.submit_job(env_vars, pbs_script, jobs_id)

submit_job.check.check_job_status(jobs_id)
