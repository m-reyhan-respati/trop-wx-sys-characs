import numpy as np
from glob import glob

from config import *

import submit_job

script_dir = f"{ROOT_DIR}/scripts/"
script_filename = "calc_mean_precipitation_in_deep_tropics_associated_with_mode_box_mask.py"

year_start = 2000
year_end = 2016

years = np.arange(year_start, year_end + 1, 1, dtype=int)

env_vars = {}
env_vars["BOX_SIZE"] = 5.0
env_vars["DIRO"] = "/scratch/k10/mr4682/data/GPM/3hr_mean_precipitation_associated_with_mode/box_mask/"

pbs_dir = f"{ROOT_DIR}/pbs_scripts/"
ncpus = 1
mem = 50
jobfsmem = 1
queue = "normal"
project = "k10"
walltime = "00:30:00"
storage = "gdata/xp65+scratch/k10"
command = f"""cd {ROOT_DIR}

source env.sh

python3 {script_dir}{script_filename}
"""

for year in years:
    jobs_id = []
    
    env_vars["YEAR"] = year
    
    if year == 2000:
        month_start = 6
    else:
        month_start = 1
    
    month_end = 12
    
    for month in np.arange(month_start, month_end + 1, 1, dtype=int):
        env_vars["MONTH"] = month
        
        files = sorted(glob(f"{env_vars['DIRO']}precipitation.in.deep.tropics.a.w.mode.{year:04d}{month:02d}.box.mask.{env_vars['BOX_SIZE']:.1f}deg.nc"))
        
        if len(files) != 0:
            continue
        
        name = f"{script_filename[:-3]}__{env_vars['YEAR']:04d}{env_vars['MONTH']:02d}_{env_vars['BOX_SIZE']:.1f}deg"
        pbs_filename = f"{name}.sh"
        
        pbs_script = submit_job.submit.create_pbs_script(pbs_dir, pbs_filename, ncpus, mem, jobfsmem, queue, project, walltime, storage, name, command)
        
        submit_job.submit.submit_job(env_vars, pbs_script, jobs_id)
    
    if len(jobs_id) != 0:
        submit_job.check.check_job_status(jobs_id)
