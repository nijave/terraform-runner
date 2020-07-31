import json
import pathlib
import subprocess

from pathlib import Path

import typing

TERRAFORM_CONFIG_INSPECTOR = str(
    Path(
        "~/Documents/workspace/go/src/github.com/hashicorp/terraform-config-inspect/terraform-config-inspect"
    ).expanduser()
)

tf_path = str(
    Path(
        "~/Documents/workspace/root/root-infrastructure/terraform/root_server/deploy_testing/us_east_1/"
    ).expanduser()
)


def inspect_project(
    path: pathlib.Path,
) -> typing.Dict[str, typing.Dict[str, typing.Dict]]:
    terraform_modules = [("", path)]
    managed_resources = {}
    local_modules = {}
    remote_modules = {}

    while len(terraform_modules) > 0:
        prefix, module_path = terraform_modules.pop(0)
        print(f"Checking {module_path}")

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
                    module_absolute_path := (Path(tf_path) / metadata["source"])
                ).is_dir()
            ):
                print(f"Local module {metadata['source']}")
                terraform_modules.append(
                    (f"{prefix}module.{metadata['name']}.", module_absolute_path)
                )
                local_modules[module_name] = module_absolute_path.resolve()
            else:
                print(f"Non-local module {metadata['source']}")
                remote_modules[module_name] = metadata["source"]

    return {
        "resources": managed_resources,
        "local_modules": local_modules,
        "remote_modules": remote_modules,
    }
