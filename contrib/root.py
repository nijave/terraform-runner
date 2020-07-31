import os
import pathlib
import re
import subprocess
from collections import defaultdict

import typing

import util

# ROOT_INFRASTRUCTURE_DIR = pathlib.Path(
#    "~/Documents/workspace/root/root-infrastructure"
# ).expanduser()
ROOT_INFRASTRUCTURE_DIR = pathlib.Path("~/code/root-infrastructure").expanduser()
os.environ["ROOT_INFRASTRUCTURE_DIR"] = str(ROOT_INFRASTRUCTURE_DIR)


def get_role_mapping(root_infrastructure_dir: pathlib.Path) -> typing.Dict[str, str]:
    """
    Sketchy ruby shenanigans to extract the account->profile
    mapping from ruby code
    :param root_infrastructure_dir:
    :return:
    """
    with open(root_infrastructure_dir / "lib/infrastructure/aws.rb", "r") as f:
        aws_account_info = f.read()

    account_module = (
        re.search(
            r" {4}module Accounts.*?^ {4}end",
            aws_account_info,
            flags=re.DOTALL | re.MULTILINE,
        )[0]
        .replace("Infrastructure::AWS::", "")
        .replace(".underscore", ".gsub('-', '_')")
    )
    ruby_code = f"""
    require 'yaml'
    ACCOUNTS_FILE_LOCATION = '{root_infrastructure_dir / "aws-accounts.yml"}'
    {account_module}
    puts ({{
        "terraform-reliability" => Accounts::RELIABILITY_ONLY,
        "terraform-non-production" => Accounts::NON_PROTECTED
    }}.to_yaml)
    """

    ruby_proc = subprocess.Popen(
        ["ruby"], stdin=subprocess.PIPE, stdout=subprocess.PIPE
    )
    ruby_output, _ = ruby_proc.communicate(input=ruby_code.encode("ascii"))

    role_mapping = defaultdict(lambda: "terraform-production")
    role_mapping.update(
        {acc: role for role, ids in util.yaml.loads(ruby_output).items() for acc in ids}
    )

    return role_mapping


ROLE_MAPPING = get_role_mapping(ROOT_INFRASTRUCTURE_DIR)
