import os
import shutil
import signal
import subprocess
import sys
import time
import unittest

import pyiron_workflow as pwf
from pyiron_workflow.mixin.run import ReadinessError

SCRIPT = """
import time
from concurrent import futures

import pyiron_workflow as pwf

wf = pwf.Workflow("passive_run")
wf.n1 = pwf.std.UserInput(3)
wf.n2 = pwf.std.Sleep(wf.n1)
wf.n3 = pwf.std.UserInput(wf.n2)

wf.n2.executor = (futures.ThreadPoolExecutor, (), {})

with futures.ThreadPoolExecutor() as exe:
    wf.executor = exe
    wf.run()
    time.sleep(1)
    wf.save()
"""


class TestRecovery(unittest.TestCase):
    def test_recovered_running_child_causes_readiness_error(self):
        proc = subprocess.Popen([sys.executable, "-c", SCRIPT])
        time.sleep(2)  # Let the process start and enter the critical section
        os.kill(proc.pid, signal.SIGKILL)
        proc.wait()

        wf = pwf.Workflow("passive_run")  # Auto-loads savefile

        wf.running = False  # Reset running status
        wf.use_cache = False  # Don't accidentally get a cached nothing result
        with self.assertRaises(ReadinessError, msg="Catch the bad readiness of n2"):
            wf.run()

        shutil.rmtree(wf.as_path())
