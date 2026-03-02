from glob import glob

from config import *

import submit_job

script_dir = f"{ROOT_DIR}/scripts/"
script_filename = "calc_composite_total_field_track_isccp_olr.py"

env_vars = {}
env_vars["FILE_NAME"] = "dse"
env_vars["VAR_NAME"] = "dse"
env_vars["YEAR_START"] = 1984
env_vars["YEAR_END"] = 2016
env_vars["DIRI"] = f"{SCRATCH_ERA5_DIR}/{env_vars['FILE_NAME']}/"
env_vars["DIRO"] = f"{env_vars['DIRI']}total_field_composites/"

pbs_dir = f"{ROOT_DIR}/pbs_scripts/"
ncpus = 1
mem = 150
jobfsmem = 1
queue = "normal"
project = "if69"
walltime = "02:30:00"
storage = "gdata/xp65+scratch/k10"
command = f"""cd {ROOT_DIR}

source env.sh

python3 {script_dir}{script_filename}"""

mode_dict = {"Moisture Mode": "moisture.mode", "Mixed System": "mixed.system", "IG Wave": "ig.wave", "Eastward Moisture Mode": "eastward.moisture.mode", "Eastward Mixed System": "eastward.mixed.system", "Eastward IG Wave": "eastward.ig.wave", "Westward Moisture Mode": "westward.moisture.mode", "Westward Mixed System": "westward.mixed.system", "Westward IG Wave": "westward.ig.wave", "Tropical Cyclone": "tc"}

#modes = ["Moisture Mode", "Mixed System", "IG Wave", "Eastward Moisture Mode", "Eastward Mixed System", "Eastward IG Wave", "Westward Moisture Mode", "Westward Mixed System", "Westward IG Wave", "Tropical Cyclone"]
modes = ["Moisture Mode", "Mixed System", "IG Wave", "Tropical Cyclone"]

# Following block of codes is for pressure-levels variables. Comment them if you want to use single-levels variables.

levels = [100, 125, 150, 175, 200, 225, 250, 300, 350, 400, 450, 500, 550, 600, 650, 700, 750, 775, 800, 825, 850, 875, 900, 925, 950, 975, 1000]

len_file_name_string = len(env_vars["FILE_NAME"])

for level in levels:
    jobs_id = []

    env_vars["FILE_NAME"] = f"{env_vars['FILE_NAME'][0:len_file_name_string]}.{level:d}"

    for mode in modes:
        env_vars["MODE"] = mode
        
        files = sorted(glob(f"{env_vars['DIRO']}composite.{env_vars['FILE_NAME']}.{mode_dict[mode]}.{env_vars['YEAR_START']}-{env_vars['YEAR_END']}.nc"))
        name = f"{script_filename[:-3]}__{env_vars['FILE_NAME']}_{mode_dict[mode]}_{env_vars['YEAR_START']}-{env_vars['YEAR_END']}"
        
        if len(files) != 0:
            continue
        
        pbs_filename = f"{name}.sh"
        
        pbs_script = submit_job.submit.create_pbs_script(pbs_dir, pbs_filename, ncpus, mem, jobfsmem, queue, project, walltime, storage, name, command)
        
        submit_job.submit.submit_job(env_vars, pbs_script, jobs_id)
    
    if len(jobs_id) != 0:
        submit_job.check.check_job_status(jobs_id)

# Following block of codes is for single-levels variables. Comment them if you want to use pressure-levels variables.
'''
jobs_id = []

for mode in modes:
    env_vars["MODE"] = mode
    
    files = sorted(glob(f"{env_vars['DIRO']}composite.{env_vars['FILE_NAME']}.{mode_dict[mode]}.{env_vars['YEAR_START']}-{env_vars['YEAR_END']}.nc"))
    name = f"{script_filename[:-3]}__{env_vars['FILE_NAME']}_{mode_dict[mode]}_{env_vars['YEAR_START']}-{env_vars['YEAR_END']}"
    
    if len(files) != 0:
        continue
    
    pbs_filename = f"{name}.sh"
    
    pbs_script = submit_job.submit.create_pbs_script(pbs_dir, pbs_filename, ncpus, mem, jobfsmem, queue, project, walltime, storage, name, command)
    
    submit_job.submit.submit_job(env_vars, pbs_script, jobs_id)

if len(jobs_id) != 0:
    submit_job.check.check_job_status(jobs_id)
'''
