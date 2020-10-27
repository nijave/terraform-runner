import pathlib
import re

import typing


class CredentialHelper:
    """
    Credential helper interface
    """

    def env(self, project: pathlib.Path) -> typing.Dict[str, str]:
        raise NotImplementedError


class NoopCredHelper(CredentialHelper):
    """
    Does nothing.
    """

    def env(self, _) -> typing.Dict:
        return {}


class RoleToProfileMapping(CredentialHelper):
    """
    Maps an AWS role ARN to an AWS profile.
    """

    @staticmethod
    def account_id_for_project(project: pathlib.Path) -> str:
        """
        Extracts IDs from role ARNs in
        a Terraform config file
        :param project: directory containing a main.tf with role_arn
        :return: AWS account_id
        """
        with open(project / "main.tf", "r") as f:
            contents = f.read()
        arns = re.findall(r'\s*role_arn\s+=\s+"(.*?)"', contents)
        # if len(set(arns)) != 1:
        #     raise ValueError(f"Expected to find exactly 1 role arn {project}")
        if len(arns) < 1:
            raise ValueError(f"Couldn't find role arn for project {project}")
        return arns[0].split(":")[4]

    def __init__(self, account_id_to_profile_map: typing.Dict[str, str]):
        """
        :param account_id_to_profile_map: A dictionary of {aws_account_id: Str -> aws_profile_name: Str}
        """
        self._account_to_profile = account_id_to_profile_map

    def env(self, project: pathlib.Path) -> typing.Dict[str, str]:
        account_id = self.account_id_for_project(project)
        return dict(
            AWS_PROFILE=self._account_to_profile[account_id],
            AWS_REGION="us-east-1",
            AWS_DEFAULT_REGION="us-east-1",
        )
