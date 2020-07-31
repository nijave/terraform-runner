import json
import logging
import os
import pathlib
import re
import subprocess

import typing
from collections import defaultdict
from concurrent.futures.thread import ThreadPoolExecutor

PathValidator = typing.Callable[[pathlib.Path], bool]
PathMetadataParser = typing.Callable[[pathlib.Path], typing.Dict[str, typing.Any]]

TERRAFORM_CONFIG_INSPECTOR = str(
    pathlib.Path(os.environ.get("GOPATH", "~/go")).expanduser()
    / pathlib.Path(
        "src/github.com/hashicorp/terraform-config-inspect/terraform-config-inspect"
    )
)


def contains_s3_backend(file_path: pathlib.Path) -> bool:
    """
    Naively checks file for an S3 backend configuration
    :param file_path: file to check for backend
    :return: whether backend config was found
    """
    with open(str(file_path.resolve()), "r") as f:
        contents = f.read()
    return 'backend "s3" {' in contents


def project_environment_region_parser(file_path: pathlib.Path) -> typing.Dict[str, str]:
    """
    Splits a relative path like project/environment/region
    into metadata (project, environment, region)
    :param file_path:
    :return: extracted metadata
    """
    parts = file_path.parts

    # Handle project/sub_project type names
    if len(parts) == 4:
        parts = ["/".join(parts[:2])] + list(parts[2:])
    if len(parts) != 3:
        return {}
    return dict(zip(("project", "environment", "region"), parts))


def environment_sorter(
    path: pathlib.Path, metadata: typing.Dict[pathlib.Path, typing.Dict]
) -> int:
    """
    Compares two paths based on their environment by
    getting environment from project metadata
    :param path:
    :param metadata:
    :return:
    """
    environment_order = defaultdict(lambda: 4)
    environment_order.update(
        {
            "deploy_testing": 1,
            "staging": 2,
            "ci": 2,
            "partner_testing": 3,
            "production": 5,
        }
    )

    return environment_order[metadata[path].get("environment", "")]


class ProjectFinder:
    DEFAULT_FILE_NAME = "main.tf"
    DEFAULT_PROJECT_VALIDATOR = contains_s3_backend
    DEFAULT_PROJECT_SORTER = environment_sorter

    @staticmethod
    def find_projects(
        path: pathlib.Path, path_validator: PathValidator
    ) -> typing.List[pathlib.Path]:
        candidate_paths = path.rglob(ProjectFinder.DEFAULT_FILE_NAME)
        return [
            p.parent.resolve()
            for p in candidate_paths
            if not any(
                parent.is_dir() and parent.name == ".terraform" for parent in p.parents
            )
            and path_validator(p)
        ]

    def __init__(
        self,
        base_path: pathlib.Path,
        path_parser: PathMetadataParser = lambda _: {},
        path_validator: PathValidator = None,
    ):
        """
        :param base_path: Where to start looking for projects
        :param path_parser: A function that returns a dict of metadata given a path
        """
        self._logger = logging.getLogger(__name__)

        if base_path is None:
            base_path = pathlib.Path.cwd()

        if isinstance(base_path, str):
            base_path = pathlib.Path(base_path)

        if path_validator is None:
            path_validator = ProjectFinder.DEFAULT_PROJECT_VALIDATOR

        self.base_path = base_path.expanduser()
        self.path_parser = path_parser
        self.projects = self.find_projects(self.base_path, path_validator)
        self.project_metadata = {}

        with ThreadPoolExecutor(max_workers=4) as pool:
            results = [(p, pool.submit(self.inspect_project, p)) for p in self.projects]
            for result in results:
                self.project_metadata[result[0]] = {"config": result[1].result()}
                self.project_metadata[result[0]].update(
                    self.path_parser(result[0].relative_to(self.base_path))
                )

    def inspect_project(
        self, path: pathlib.Path
    ) -> typing.Dict[str, typing.Dict[str, typing.Dict]]:
        """
        Recurses through path grabbing project module and resource metadata
        :param path:
        :return: Dict[resources, local_modules, remote_modules]
        """
        terraform_modules = [("", path)]
        managed_resources = {}
        local_modules = {}
        remote_modules = {}

        while len(terraform_modules) > 0:
            prefix, module_path = terraform_modules.pop(0)
            self._logger.info("Checking %s", module_path)

            details = json.loads(
                subprocess.check_output(
                    [TERRAFORM_CONFIG_INSPECTOR, "--json", module_path,], text=True,
                )
            )

            managed_resources.update(
                {f"{prefix}{k}": v for k, v in details["managed_resources"].items()}
            )

            for name, metadata in details["module_calls"].items():
                module_name = f"{prefix}module.{metadata['name']}"
                if (
                    any(
                        metadata["source"].startswith(prefix)
                        for prefix in ("./", "../", "/")
                    )
                    and (
                        module_absolute_path := pathlib.Path(path) / metadata["source"]
                    ).is_dir()
                ):
                    self._logger.debug("Local module %s", metadata["source"])
                    # noinspection PyUnboundLocalVariable
                    terraform_modules.append(
                        (f"{prefix}module.{metadata['name']}.", module_absolute_path)
                    )
                    local_modules[module_name] = module_absolute_path.resolve()
                else:
                    self._logger.debug("Non-local module %s", metadata["source"])
                    remote_modules[module_name] = metadata["source"]

        return {
            "resources": managed_resources,
            "local_modules": local_modules,
            "remote_modules": remote_modules,
        }

    def sort_projects(
        self,
        comparer_function: typing.Callable[
            [pathlib.Path, typing.Dict[pathlib.Path, str]], int
        ] = None,
    ) -> None:
        if comparer_function is None:
            comparer_function = ProjectFinder.DEFAULT_PROJECT_SORTER
        self.projects.sort(key=lambda a: comparer_function(a, self.project_metadata))


class ChangeFinder:
    @staticmethod
    def is_parent(possible_parent: pathlib.Path, child: pathlib.Path) -> bool:
        try:
            child.resolve().relative_to(possible_parent.resolve())
            return True
        except ValueError:
            return False

    def __init__(self, project_finder: ProjectFinder):
        self._project_finder = project_finder

    def _changed_projects_from_changed_files(
        self, changed_files: typing.Iterable[pathlib.Path]
    ) -> typing.List[pathlib.Path]:
        """
        Finds Terraform projects with changes given a list of changed files
        in a Terraform project tree
        :param changed_files: iterable of changed file paths
        :return: list of changed project paths
        """

        changed_directories = [p if p.is_dir() else p.parent for p in changed_files]

        changes = set()
        for directory in changed_directories:
            if any(directory.samefile(p) for p in self._project_finder.projects):
                changes.add(directory)
            else:
                for project in self._project_finder.projects:
                    metadata = self._project_finder.project_metadata[project]
                    if any(
                        directory.samefile(module)
                        for module in metadata["config"]["local_modules"].values()
                    ):
                        changes.add(project)

        return list(changes)

    def git_diff(
        self,
        target_branch: str = "origin/master",
        git_terraform_directory: pathlib.Path = pathlib.Path("terraform/"),
    ) -> typing.List[pathlib.Path]:
        """
        Finds changed projects based on comparing a git branch
        to the current working tree
        :param target_branch: branch to compare to
        :param git_terraform_directory: Terraform base_path relative to git repository (i.e. "terraform" for repo/terraform)
        :return: list of changed projects
        """

        # All changes
        git_results = subprocess.check_output(
            ["git", "diff", "--name-only", "--right-only", target_branch],
            cwd=self._project_finder.base_path,
            text=True,
        )

        # All changed Terraform files
        changed_files = [
            pathlib.Path(
                self._project_finder.base_path
                / re.sub(
                    "^" + re.escape(str(git_terraform_directory).rstrip("/")) + "/?",
                    "",
                    d.strip(),
                )
            ).resolve()
            for d in git_results.splitlines()
        ]

        return self._changed_projects_from_changed_files(changed_files)

    def remote_module(
        self, module: str, fuzzy: bool = True
    ) -> typing.List[pathlib.Path]:
        if fuzzy:
            compare = lambda a, b: a in b
        else:
            compare = lambda a, b: a == b

        metadata = self._project_finder.project_metadata

        return [
            project
            for project in self._project_finder.projects
            if any(
                compare(module, project_module)
                for project_module in metadata[project]["config"][
                    "remote_modules"
                ].values()
            )
        ]


"""
import logging
logging.basicConfig(level=logging.INFO)
from terraform.project import *
tf_path = "~/Documents/workspace/root/root-infrastructure/terraform"
pf = ProjectFinder(tf_path, path_parser=project_environment_region_parser)
#pf.sort_projects(environment_sorter)

c = ChangeFinder(pf)
projects = c.git_diff()
print("\n".join(str(p.relative_to(pf.base_path)) for p in projects))
"""
