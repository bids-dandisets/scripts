import pathlib
import subprocess

import dandi.dandiapi

BASE_DIRECTORY = pathlib.Path("E:/GitHub/bids-dandisets")


def _deploy_subprocess(
    *,
    command: str | list[str],
    cwd: str | pathlib.Path | None = None,
    environment_variables: dict[str, str] | None = None,
    error_message: str | None = None,
    ignore_errors: bool = False,
) -> str | None:
    error_message = error_message or "An error occurred while executing the command."

    result = subprocess.run(
        args=command,
        cwd=cwd,
        shell=True,
        env=environment_variables,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if result.returncode != 0 and ignore_errors is False:
        message = (
            f"\n\nError code {result.returncode}\n"
            f"{error_message}\n\n"
            f"stdout: {result.stdout}\n\n"
            f"stderr: {result.stderr}\n\n"
        )
        raise RuntimeError(message)
    if result.returncode != 0 and ignore_errors is True:
        return None

    return result.stdout


def reset_excess_branches() -> None:
    client = dandi.dandiapi.DandiAPIClient()
    dandisets = client.get_dandisets()

    for dandiset in dandisets:
        dandiset_id = dandiset.identifier
        repo_directory = BASE_DIRECTORY / dandiset_id

        print(f"Cleaning excess branches on Dandiset {dandiset_id}...")

        branch_list = _deploy_subprocess(command="git branch -r", cwd=repo_directory)
        branches = {line.strip() for line in branch_list.splitlines()}
        skip_branches = {"origin/HEAD -> origin/draft", "origin/draft", "origin/git-annex"}
        branches_to_delete = branches - skip_branches
        print("\tCleaning excess branches...")
        for branch in branches_to_delete:
            branch_name = branch.removeprefix("origin/")
            _deploy_subprocess(command=f"git push origin --delete {branch_name}", cwd=repo_directory)
            _deploy_subprocess(command=f"git branch -D {branch_name}", cwd=repo_directory, ignore_errors=True)
            _deploy_subprocess(command=f"git branch -D {branch_name}", cwd=repo_directory, ignore_errors=True)

        print("Cleaning complete!\n\n")


if __name__ == "__main__":
    reset_excess_branches()
