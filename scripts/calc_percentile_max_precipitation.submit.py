import numpy as np
from glob import glob

from config import *

import submit_job

script_dir = f"{ROOT_DIR}/scripts/"
script_filename = "calc_percentile_max_precipitation.py"

lat_min_list = np.arange(-30.5, 30.5, 2.0)
lat_max_list = lat_min_list + 1.9
lat_max_list[-1] = 30.5

env_vars = {}
env_vars["PERCENTILE"] = 95
env_vars["DIRO"] = "/scratch/k10/mr4682/data/GPM/3hr_max_precipitation/percentiles/"

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

python3 {script_dir}{script_filename}
"""

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
    
    files = sorted(glob(f"{env_vars['DIRO']}max_precipitation.{env_vars['PERCENTILE']:d}p.{lat_min_string}_{lat_max_string}.nc"))
    
    if len(files) != 0:
        continue
    
    name = f"{script_filename[:-3]}__{env_vars['PERCENTILE']}p_{lat_min_string}_{lat_max_string}"
    pbs_filename = f"{name}.sh"
    
    pbs_script = submit_job.submit.create_pbs_script(pbs_dir, pbs_filename, ncpus, mem, jobfsmem, queue, project, walltime, storage, name, command)
    
    submit_job.submit.submit_job(env_vars, pbs_script, jobs_id)

if len(jobs_id) != 0:
    submit_job.check.check_job_status(jobs_id)
