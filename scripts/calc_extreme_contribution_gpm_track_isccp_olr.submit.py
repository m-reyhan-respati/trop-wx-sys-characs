import numpy as np
from glob import glob

from config import *

import submit_job

script_dir = f"{ROOT_DIR}/scripts/"
script_filename = "calc_extreme_contribution_gpm_track_isccp_olr.py"

year_start = 2001
year_end = 2015

years = np.arange(year_start, year_end + 1, 1, dtype=int)

env_vars = {}
env_vars["FILE_NAME"] = "mean_intensity"
env_vars["VAR_NAME"] = "mean_intensity"
env_vars["PERCENTILE"] = 99
env_vars["RAINING"] = 0
env_vars["DIRI"] = f"{SCRATCH_GPM_DIR}/3hr_{env_vars['FILE_NAME']}/"
env_vars["DIRO"] = f"{env_vars['DIRI']}extreme_contribution/"

pbs_dir = f"{ROOT_DIR}/pbs_scripts/"
ncpus = 1
mem = 100
jobfsmem = 1
queue = "normal"
project = "ng72"
walltime = "00:30:00"
storage = "gdata/xp65+scratch/k10"
command = f"""cd {ROOT_DIR}

source env.sh

python3 {script_dir}{script_filename}"""

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
        
        if env_vars["RAINING"] == 1:
            files = sorted(glob(f"{env_vars['DIRO']}raining/extreme.contribution.{env_vars['PERCENTILE']:d}p.{env_vars['FILE_NAME']}.{env_vars['YEAR']:04d}{env_vars['MONTH']:02d}.nc"))
            name = f"{script_filename[:-3]}__{env_vars['PERCENTILE']:d}p_raining_{env_vars['FILE_NAME']}_{env_vars['YEAR']:04d}{env_vars['MONTH']:02d}"
        else:
            files = sorted(glob(f"{env_vars['DIRO']}extreme.contribution.{env_vars['PERCENTILE']:d}p.{env_vars['FILE_NAME']}.{env_vars['YEAR']:04d}{env_vars['MONTH']:02d}.nc"))
            name = f"{script_filename[:-3]}__{env_vars['PERCENTILE']:d}p_{env_vars['FILE_NAME']}_{env_vars['YEAR']:04d}{env_vars['MONTH']:02d}"
        
        if len(files) != 0:
            continue
        
        pbs_filename = f"{name}.sh"
        
        pbs_script = submit_job.submit.create_pbs_script(pbs_dir, pbs_filename, ncpus, mem, jobfsmem, queue, project, walltime, storage, name, command)
        
        submit_job.submit.submit_job(env_vars, pbs_script, jobs_id)
    
    if len(jobs_id) != 0:
        submit_job.check.check_job_status(jobs_id)
