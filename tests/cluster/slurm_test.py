import argparse
import os
import time

import pyiron_workflow as pwf
from pyiron_workflow.executors.wrapped_executorlib import CacheSlurmClusterExecutor

t_overhead = 2
t_sleep = 10


def state_check(wf, expected_wf, expected_n2, expected_result=pwf.api.NOT_DATA):
    wf_running, n_running, outputs = (
        wf.running,
        wf.n2.running,
        wf.outputs.to_value_dict(),
    )
    print(
        "wf.running, wf.n2.running, wf.outputs.to_value_dict()",
        wf_running,
        n_running,
        outputs,
    )
    assert (wf_running, n_running, outputs["n3__user_input"]) == (
        expected_wf,
        expected_n2,
        expected_result,
    )


def submission():
    submission_template = """\
#!/bin/bash
#SBATCH --output=time.out
#SBATCH --job-name={{job_name}}
#SBATCH --chdir={{working_directory}}
#SBATCH --get-user-env=L
#SBATCH --cpus-per-task={{cores}}

{{command}}
"""
    resource_dict = {"submission_template": submission_template}

    wf = pwf.Workflow("slurm_test")
    wf.n1 = pwf.std.UserInput(t_sleep)
    wf.n2 = pwf.std.Sleep(wf.n1)
    wf.n3 = pwf.std.UserInput(wf.n2)

    print("submitting")
    print(time.time())
    wf.n2.executor = (CacheSlurmClusterExecutor, (), {"resource_dict": resource_dict})
    wf.n2.use_cache = False
    out = wf.run_in_thread()
    print("run return", out)
    state_check(wf, True, True)
    print("sleeping", t_overhead + t_sleep / 4)
    time.sleep(t_overhead + t_sleep / 4)
    print("saving")
    state_check(wf, True, True)
    wf.save()
    print("sleeping", t_sleep / 4)
    time.sleep(t_sleep / 4)
    print("pre-exit state")
    state_check(wf, True, True)
    print("hard exit at time", time.time())
    os._exit(0)  # Hard exit so that we don't wait for the executor


def interruption():
    print("loading at time", time.time())
    wf = pwf.Workflow("slurm_test")
    state_check(wf, True, True)
    wf.executor = None  # https://github.com/pyiron/pyiron_workflow/issues/705
    wf.running = False  # https://github.com/pyiron/pyiron_workflow/issues/706
    print("re-running")
    out = wf.run_in_thread()
    print("run return", out)
    state_check(wf, True, True)
    print("sleeping", t_overhead + t_sleep)
    time.sleep(t_overhead + t_sleep)
    state_check(wf, False, False, t_sleep)
    wf.delete_storage()


def discovery():
    print("loading at time", time.time())
    wf = pwf.Workflow("slurm_test")
    state_check(wf, True, True)
    wf.executor = None  # https://github.com/pyiron/pyiron_workflow/issues/705
    wf.running = False  # https://github.com/pyiron/pyiron_workflow/issues/706
    print("sleeping", t_overhead + t_sleep)
    time.sleep(t_overhead + t_sleep)
    print("re-running")
    out = wf.run_in_thread()
    print("run return", out)
    state_check(wf, True, True)
    print("sleeping", t_sleep / 10)
    time.sleep(t_sleep / 10)
    print("finally")
    state_check(wf, False, False, t_sleep)
    wf.delete_storage()


def main():
    parser = argparse.ArgumentParser(description="Run workflow stages.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--submit", action="store_true", help="Run submission stage.")
    group.add_argument(
        "--interrupt", action="store_true", help="Run interruption stage."
    )
    group.add_argument("--discover", action="store_true", help="Run discovery stage.")
    args = parser.parse_args()

    if args.submit:
        submission()
    elif args.interrupt:
        interruption()
    elif args.discover:
        discovery()


if __name__ == "__main__":
    main()
