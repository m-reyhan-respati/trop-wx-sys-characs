import os
import subprocess
import time

def check_job_status(jobs_id, time_delay=120):
    print("\nWaiting for queue...\n")

    time.sleep(time_delay)

    print(f"Checking submission status at {time.ctime()}\n")

    for i in range(0, len(jobs_id)):
        try:
            check_command = ["qstat", "-swx", jobs_id[i]]

            checking = subprocess.run(check_command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            print(checking.stdout)
        except subprocess.CalledProcessError as e:
            print("Error:")
            print(e.stderr)
