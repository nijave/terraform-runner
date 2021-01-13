#!/usr/bin/env python3
import logging
import pathlib
import subprocess
import time
import typing

import click
import munch

import contrib.root
import terraform

# logging.basicConfig(level=logging.DEBUG)
logging.basicConfig(level=logging.WARNING)
# logging.getLogger("non_blocking_process").setLevel(logging.WARNING)

REPO_PATH = pathlib.Path("~/code/root-infrastructure/terraform").expanduser()


def terrify_args(func: typing.Callable[[click.Context, str, str], None]):
    """
    Returns if the param value isn't set.
    If it is set, replace hyphens with underscores
    and call function
    :param func: to wrap
    :return: wrapped func
    """

    def wrapped_func(ctx: click.Context, param: str, value: str) -> None:
        if value is None:
            return
        value = value.replace("-", "_")
        return func(ctx, param, value)

    return wrapped_func


def git_changes(ctx, _, commit):
    if commit is None:
        return

    change_finder = terraform.project.ChangeFinder(ctx.obj.project_finder)
    changed_projects = change_finder.git_diff(commit)
    ctx.obj.projects = [
        project
        for project in ctx.obj.projects
        if any(project.samefile(changed) for changed in changed_projects)
    ]


@terrify_args
def environment_filter(ctx, _, environment):
    ctx.obj.projects = [
        project
        for project in ctx.obj.projects
        if ctx.obj.project_finder.project_metadata.get(project, {}).get(
            "environment", ""
        )
        == environment
    ]


@terrify_args
def region_filter(ctx, _, region):
    ctx.obj.projects = [
        project
        for project in ctx.obj.projects
        if ctx.obj.project_finder.project_metadata.get(project, {}).get("region", "")
        == region
    ]


@terrify_args
def project_filter(ctx, _, project):
    ctx.obj.projects = [
        proj
        for proj in ctx.obj.projects
        if str(proj.relative_to(ctx.obj.project_finder.base_path)).startswith(project)
    ]


def remote_modules(ctx, _, module):
    if module is None:
        return

    change_finder = terraform.project.ChangeFinder(ctx.obj.project_finder)
    projects_with_module = change_finder.remote_module(module)
    ctx.obj.projects = [
        candidate
        for candidate in ctx.obj.projects
        if any(candidate.samefile(project) for project in projects_with_module)
    ]


filter_git_changes = click.option(
    "--git-diff",
    "git_changes",
    default=None,
    help="Git diff current working tree to specified commit",
    callback=git_changes,
    expose_value=False,
)

default_git_branch = subprocess.check_output(
    ["git", "rev-parse", "--abbrev-ref", "origin/HEAD"],
    cwd=REPO_PATH,
    text=True,
).strip()
filter_git_changes_master = click.option(
    "-c",
    "git_changes",
    flag_value=default_git_branch,
    help=f"Git diff current working tree to {default_git_branch}",
    callback=git_changes,
    expose_value=False,
)

filter_remote_module = click.option(
    "--module",
    "-m",
    default=None,
    help="Remote module source to filter by (substring match)",
    callback=remote_modules,
    expose_value=False,
)

filter_project = click.option(
    "--project",
    "-p",
    default=None,
    help="Filter by Terraform project name prefix",
    callback=project_filter,
    expose_value=False,
)

filter_environment = click.option(
    "--environment",
    "-e",
    default=None,
    help="The environment for the Terraform project. Specify empty string if the project doesn't have environments",
    callback=environment_filter,
    expose_value=False,
)

filter_region = click.option(
    "--region",
    "-r",
    default=None,
    help="The region for the project. Specify empty string if the project doesn't have a region",
    callback=region_filter,
    expose_value=False,
)


# https://stackoverflow.com/a/40195800/2751619
def add_options(options):
    def _add_options(func):
        for option in reversed(options):
            func = option(func)
        return func

    return _add_options


filter_options = add_options(
    [
        filter_git_changes,
        filter_git_changes_master,
        filter_remote_module,
        filter_project,
        filter_environment,
        filter_region,
    ]
)


@click.group()
@click.pass_context
def cli(ctx):
    ctx.ensure_object(munch.Munch)
    finder = terraform.project.ProjectFinder(
        REPO_PATH,
        path_parser=terraform.project.project_environment_region_parser,
    )
    finder.sort_projects()
    ctx.obj.project_finder = finder
    ctx.obj.projects = finder.projects[:]


@cli.command()
@click.pass_context
def benchmark(ctx):
    pf = ctx.obj.project_finder

    c = terraform.project.ChangeFinder(pf)
    projects = c.git_diff()
    click.echo("\n".join(str(p.relative_to(pf.base_path)) for p in projects))
    # tf = terraform.runner.Runner(project, cred_helper=cred_helper)


@cli.command(name="list")
@filter_options
@click.pass_context
def lst(ctx):
    base_path = ctx.obj.project_finder.base_path

    for project in ctx.obj.projects:
        click.echo(project.relative_to(base_path))


def handle_project_interaction(project, process, cred_helper):
    click.echo(f"\n{project}")
    if process.returncode == 0:
        click.echo("No changes. Continuing")
        return

    if process.returncode == 2:
        while True:
            response = click.prompt(
                "Show (o=full output/e=errors/p=plan/s=summary/a=apply/n=next)"
            )
            if response == "o":
                click.echo(process.readall())
            elif response == "e":
                click.echo(process.readall(stream="stderr"))
            elif response == "p":
                click.echo(terraform.plan.PlanTextAnalyzer.changes(process.readall()))
            elif response == "s":
                click.echo(
                    terraform.plan.PlanTextAnalyzer.change_summary(process.readall())
                )
            elif response == "a":
                runner = terraform.runner.Runner(project, cred_helper=cred_helper)
                apply_process = runner.apply()
                while apply_process.returncode is None:
                    if not (err := apply_process.read()):
                        time.sleep(0.25)
                    else:
                        click.echo(err, nl=False)
                if apply_process.returncode != 0 and not click.confirm(
                    "Apply failed. Would you like to continue?"
                ):
                    raise RuntimeError("Terraform apply failed")
                return
            elif response == "n":
                return
    else:
        click.echo(f"Error running plan exit code {process.returncode}")
        if out := process.readall(stream="stdout"):
            click.echo(out)
        if err := process.readall(stream="stderr"):
            click.echo(err)


@cli.command()
@filter_options
@click.pass_context
def run(ctx):
    base_path = ctx.obj.project_finder.base_path
    projects = ctx.obj.projects

    if len(projects) == 0:
        click.echo("No projects found!")
        raise click.Abort

    click.echo("\nFound the following projects:")
    for project in projects:
        click.echo(f" - {project.relative_to(base_path)}")

    click.confirm(f"\nStart planning?", abort=True)

    cred_helper = terraform.credentials.RoleToProfileMapping(contrib.root.ROLE_MAPPING)
    runner = terraform.runner.RunnerPool(projects=projects, cred_helper=cred_helper)

    try:
        click.echo(f"Starting plans...")
        operation_results = runner.plan()
        for project, proc in operation_results:
            if proc.returncode is None:
                click.echo(f"Waiting for {project} plan to complete")
            while proc.returncode is None:
                if not (out := proc.read()):
                    time.sleep(0.25)
                else:
                    click.echo(out, nl=False)
            relative_project = project.relative_to(base_path)
            handle_project_interaction(relative_project, proc, cred_helper)
    finally:
        runner.shutdown()


if __name__ == "__main__":
    cli()
