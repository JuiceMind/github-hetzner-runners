# Copyright 2023 Katteli Inc.
# TestFlows.com Open-Source Software Testing Framework (http://testflows.com)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import sys
import os
import signal
import threading
import subprocess

from .logger import logger


def shell(
    cmd: str,
    shell: bool = True,
    check: bool = True,
    use_logger=True,
    stacklevel=2,
    server_name=None,
    timeout=None,
):
    """Execute command."""
    p = subprocess.Popen(
        cmd,
        shell=shell,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
        universal_newlines=True,
        text=True,
        encoding="utf-8",
        start_new_session=timeout is not None,
    )

    expired = threading.Event()

    def expire():
        expired.set()
        try:
            os.killpg(p.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass

    timer = None
    if timeout is not None:
        timer = threading.Timer(max(0, timeout), expire)
        timer.daemon = True
        timer.start()
    try:
        for line in iter(p.stdout.readline, ""):
            if use_logger:
                logger.info(
                    f"   > {line.rstrip()}",
                    stacklevel=stacklevel,
                    extra={"server_name": server_name},
                )
            else:
                sys.stdout.write(line.rstrip())
                sys.stdout.write("\n")
                sys.stdout.flush()
        p.wait()
    finally:
        if timer is not None:
            timer.cancel()
            timer.join()
        if p.poll() is None:
            if timeout is not None:
                expire()
            else:
                p.kill()
            p.wait()
        p.stdout.close()

    if expired.is_set():
        raise TimeoutError(f"Command exceeded {timeout}s")
    if check and p.returncode != 0:
        raise RuntimeError(f"Command returned non-zero exit code {p.returncode}")
    return p.returncode
