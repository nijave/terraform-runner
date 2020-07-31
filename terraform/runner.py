import json
import logging
import os
import pathlib
import re
import threading
import time
import typing
from collections import OrderedDict
from concurrent.futures.thread import ThreadPoolExecutor

import non_blocking_process
from terraform.credentials import NoopCredHelper, CredentialHelper

status = {
    0: "Succeeded, diff is empty (no changes)",
    1: "Errored",
    2: "Succeeded, there is a diff",
}


class Runner:
    DEFAULT_CRED_HELPER = NoopCredHelper()
    TERRAFORM_ENTRYPOINT = ["bash", "--login", "-c"]

    def __init__(
        self,
        project: pathlib.Path,
        cred_helper: CredentialHelper = DEFAULT_CRED_HELPER,
        exec_method: typing.Callable = non_blocking_process.NonBlockingProcess,
    ):
        """
        :param project: path to a Terraform project :param cred_helper: terraform.credentials.CredentialHelper which
        can be called with the project path to supply credentials
        """
        self._logger = logging.getLogger(__name__)
        self.project = project.expanduser()
        self._cred_helper = lambda: cred_helper.env(self.project)
        self._exec_method = exec_method

    def _run_wrapper(
        self, cmd: typing.Iterable[str]
    ) -> non_blocking_process.NonBlockingProcess:
        aws_vars = {k: v for k, v in os.environ.items() if k.lower().startswith("aws_")}
        okta_vars = {
            k: v for k, v in os.environ.items() if k.lower().startswith("okta_")
        }
        artifactory_vars = {
            k: v for k, v in os.environ.items() if k.lower().startswith("artifactory_")
        }
        self._logger.debug("Running %s with %s", cmd, self._exec_method)
        process = self._exec_method(
            self.TERRAFORM_ENTRYPOINT + list(cmd),
            cwd=self.project,
            env={
                "HOME": os.environ.get("HOME", "/"),
                "TF_LOG": "TRACE",
                "TF_LOG_PATH": str(pathlib.Path(os.environ["HOME"]) / ".terraform.log"),
                "AWS_SDK_LOAD_CONFIG": "true",
                **aws_vars,
                **okta_vars,
                **artifactory_vars,
                **self._cred_helper(),
            },
        )
        self._logger.debug("Started %s with %s -> %s", cmd, self._exec_method, process)
        return process

    def init(self) -> non_blocking_process.NonBlockingProcess:
        self._logger.info("Initializing %s", self.project)
        return self._run_wrapper(["terraform init"])

    # requires init
    def plan(self) -> non_blocking_process.NonBlockingProcess:
        # When init not needed, stdout: Refreshing Terraform state in-memory prior to plan...
        # When init needed (exit 1), stdout: Backend reinitialization required
        self._logger.info("Running plan for %s", self.project)
        process = self._run_wrapper(
            ["terraform plan -detailed-exitcode -out=default.tfplan"]
        )
        for _ in range(120):
            self._logger.debug("Checking to see if plan %s started", self.project)
            out = process.readall(stream="stdout")
            err = process.readall(stream="stderr")
            if "Backend reinitialization required" in out or re.search(
                r'"terraform\s*init"', err
            ):
                self._logger.info("Project %s needs initialized first", self.project)
                process.wait()
                init_process = self.init()
                if init_process.result[0] != 0:
                    self._logger.warning("Failed to initialize %s", self.project)
                    return init_process
                self._logger.info(
                    "Restarting plan after initialization for %s", self.project
                )
                return self.plan()
            if "Refreshing Terraform state in-memory prior to plan..." in out:
                self._logger.debug("Plan %s started successfully", self.project)
                break
            # Wait for the first line
            if ("\n" not in out) and ("\n" not in err):
                self._logger.debug("Plan %s waiting for first line", self.project)
                time.sleep(0.5)
                continue
            elif process.returncode is not None and process.returncode not in (0, 2):
                break

        return process

    # requires saved plan
    def apply(self) -> non_blocking_process.NonBlockingProcess:
        assert (pathlib.Path(self.project) / "default.tfplan").is_file()
        return self._run_wrapper(["terraform apply default.tfplan"])

    # requires init
    def state(self) -> typing.Dict[str, typing.Any]:
        for _ in range(2):
            process = self._run_wrapper(["terraform state pull"])
            process.wait()
            if (
                process.returncode == 1
                and "Initialization required" in process.readall("stderr")
            ):
                init_process = self.init()
                if init_process.result[0] != 0:
                    raise RuntimeError(
                        f"Failed to initialize workspace for {self.project}"
                    )
            else:
                break
        return json.loads(process.readall("stdout"))


class RunnerPool:
    def __init__(
        self,
        projects: typing.Iterable[pathlib.Path],
        cred_helper: CredentialHelper = Runner.DEFAULT_CRED_HELPER,
        max_concurrency: int = 4,
    ):
        self._logger = logging.getLogger(__name__)
        self._logger.debug("Creating non_blocking_process pool of %i", max_concurrency)
        self._pool = non_blocking_process.pool.Pool(workers=max_concurrency)
        self._projects = list(projects)
        self._cred_helper = cred_helper
        self._max_concurrency = max_concurrency
        self._results = OrderedDict()

    def _run_wrapper(
        self, operation: str
    ) -> typing.Iterable[
        typing.Tuple[pathlib.Path, non_blocking_process.NonBlockingProcess]
    ]:
        self._logger.debug(
            "Creating ThreadPoolExecutor to bootstrap %s jobs", operation
        )
        pool = ThreadPoolExecutor(max_workers=32)
        for project in self._projects:
            self._logger.debug("Queueing job %s for %s", operation, project)
            project_runner = Runner(
                project=project,
                cred_helper=self._cred_helper,
                exec_method=self._pool.queue,
            )
            self._results[project] = pool.submit(getattr(project_runner, operation))

        pool.shutdown(wait=False)

        for project in self._results:
            self._logger.debug("Getting process for %s", project)
            proc = self._results[project].result()
            yield project, proc

    def init(self):
        yield from self._run_wrapper("init")

    def plan(self):
        yield from self._run_wrapper("plan")

    def apply(self):
        yield from self._run_wrapper("apply")

    def shutdown(self):
        self._pool.shutdown()
        self._logger.debug("Running threads %s", list(threading.enumerate()))

    def __del__(self):
        self.shutdown()
