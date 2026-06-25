import argparse
import os
import pathlib
import subprocess
import tempfile
import time

from pip._internal.utils import datetime

import pyiron_workflow._wfms.api as pwf


@pwf.atomic
def identity(x):
    return x


@pwf.atomic
def sleepy(t):
    time.sleep(t)
    return t


@pwf.workflow
def three_step(t):
    n1 = identity(t)
    n2 = sleepy(n1)
    n3 = identity(n2)
    return n3


TEMPDIR = pathlib.Path(tempfile.mkdtemp())
T_SLEEP = 10


def kill_sleeper(
    run_dir: pathlib.Path,
    timestamp: datetime.datetime,
    lexical_path: str,
    status: pwf.schemas.RunStatus,
):
    print("Callback at", lexical_path, status)
    hard_coded_sleepy_path = "three_step.sleepy_0"
    if (
        lexical_path == hard_coded_sleepy_path
        and status == pwf.schemas.RunStatus.RUNNING
    ):
        print("Not killing the sleepy node")
        # print_queue("Queue at callback kill-time -- immediate")
        # time.sleep(0.25)  # give the job a second to process
        # print_queue("Queue at callback kill-time -- fast")
        # time.sleep(1)  # give the job a second to process
        # print_queue("Queue at callback kill-time -- slow")
        # os._exit(0)  # Then hard exit so that we don't even wait for the executor


def get_queue() -> list[dict[str, str]]:
    """Return current squeue entries as list of dicts."""
    cmd = ["squeue", "--noheader", "--format=%i|%j|%T|%u"]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    jobs = []
    for line in result.stdout.splitlines():
        job_id, name, state, job_user = line.split("|")
        jobs.append({"job_id": job_id, "name": name, "state": state, "user": job_user})
    return jobs


def print_queue(extra_message: str | None = None) -> None:
    if extra_message:
        print(extra_message)
    cmd = ["squeue"]
    subprocess.run(cmd, check=True)


def assert_queue_has_n_items(n: int) -> None:
    jobs = get_queue()
    assert len(jobs) == n, f"Expected {n} job(s) in queue, found {len(jobs)}: {jobs}"


def setup(callbacks=None):
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

    wf = three_step.pwf.node()

    wf.sleepy_0.executor = pwf.ExecutorInstructions(
        pwf.tools.NodeSlurmExecutor,
        (),
        {"resource_dict": resource_dict},
    )

    cfg = pwf.RunConfig(run_dir=TEMPDIR, progress_hooks=callbacks or [])
    return wf, cfg


def setup_node_executor(callbacks=None):
    wf = three_step.pwf.node()

    wf.sleepy_0.executor = pwf.ExecutorInstructions(
        pwf.tools.NodeSingleExecutor, (), {}
    )

    cfg = pwf.RunConfig(run_dir=TEMPDIR, progress_hooks=callbacks or [])
    return wf, cfg


def submission():
    return
    wf, cfg = setup([pwf.ProgressHook(kill_sleeper)])
    out = wf.run(cfg, t=T_SLEEP)
    print("FINISHED", out.outputs)


def interruption():
    wf, cfg = setup([pwf.ProgressHook(kill_sleeper)])
    out = wf.run(cfg, t=T_SLEEP)
    print("FINISHED", out.outputs)
    return
    print_queue("Queue at interruption time")
    assert_queue_has_n_items(1)
    wf, cfg = setup()
    t0 = time.time()
    out = wf.run(cfg, t=T_SLEEP)
    dt = time.time() - t0
    assert_queue_has_n_items(0)
    assert (
        dt > 0.5 * T_SLEEP
    ), f"Expected to need to wait for the job to finish, but got a small dt -- {dt}"
    assert (
        out.outputs["n3"].value == T_SLEEP
    ), f"Sanity check that the workflow returned T_SLEEP, but got {out.outputs['n3'].value}"


def discovery():
    wf, cfg = setup_node_executor([pwf.ProgressHook(kill_sleeper)])
    out = wf.run(cfg, t=T_SLEEP)
    print("FINISHED", out.outputs)
    return
    print_queue("Queue at discovery time")
    assert_queue_has_n_items(1)
    time.sleep(1.5 * T_SLEEP)  # Wait for it to finish
    assert_queue_has_n_items(0)
    wf, cfg = setup()
    t0 = time.time()
    out = wf.run(cfg, t=T_SLEEP)
    dt = time.time() - t0
    assert (
        dt < 0.1 * T_SLEEP
    ), f"Expected to get a quick cache hit, but got a large -- {dt}"
    assert (
        out.outputs["n3"].value == T_SLEEP
    ), f"Sanity check that the workflow returned T_SLEEP, but got {out.outputs['n3'].value}"


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
