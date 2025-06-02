import os
import subprocess

def create_pbs_script(pbs_dir, pbs_filename, ncpus, mem, jobfsmem, queue, project, walltime, storage, name, command):
    pbs_script_content = f"""#!/bin/bash
  
#PBS -l ncpus={ncpus:d}
#PBS -l mem={mem:d}GB
#PBS -l jobfs={jobfsmem:d}GB
#PBS -q {queue}
#PBS -P {project}
#PBS -l walltime={walltime}
#PBS -l storage={storage}
#PBS -o {pbs_dir}{name}.out
#PBS -e {pbs_dir}{name}.err

{command}"""
    with open(pbs_dir + pbs_filename, "w") as file:
        file.write(pbs_script_content)
    return pbs_dir+pbs_filename

def submit_job(env_vars, pbs_script, jobs_id):
    env_vars_str = ",".join([f"{key}={value}" for key, value in env_vars.items()])

    submit_command = ["qsub", "-v", env_vars_str, pbs_script]

    submission = subprocess.run(submit_command, check=True, stdout=subprocess.PIPE, text=True)
    submission_id = submission.stdout.strip()

    print(f"Submitted {pbs_script} as {submission_id}")

    jobs_id.append(submission_id)
