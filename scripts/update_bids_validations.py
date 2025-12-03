import collections
import concurrent.futures
import importlib
import importlib.metadata
import json
import os
import pathlib
import subprocess
import tempfile

import dandi.dandiapi
import requests

MAX_WORKERS = 1  # TODO: try None in GitHub actions when working to see how fast it is
DANDISET_LIMIT = 5
BRANCH_NAME = "draft"

GITHUB_TOKEN = os.environ.get("_GITHUB_API_KEY", None)
if GITHUB_TOKEN is None:
    message = "`_GITHUB_API_KEY` environment variable not set"
    raise ValueError(message)

if "site-packages" in importlib.util.find_spec("nwb2bids").origin:
    message = "nwb2bids is installed in site-packages - please install in editable mode"
    raise RuntimeError(message)

BASE_GITHUB_URL = f"https://{GITHUB_TOKEN}@github.com"
BASE_GITHUB_API_URL = "https://api.github.com/repos"
RAW_CONTENT_BASE_URL = "https://raw.githubusercontent.com/bids-dandisets"

WORKDIR = pathlib.Path(tempfile.mkdtemp())

AUTHENTICATION_HEADER = {"Authorization": f"token {GITHUB_TOKEN}"}

# Config is likely temporary to suppress the 'unknown version' because we run from BEP32 schema
THIS_FILE_PATH = pathlib.Path(__file__)
BIDS_VALIDATION_CONFIG_FILE_PATH = THIS_FILE_PATH.parent / "bids_validation_config.json"
if not BIDS_VALIDATION_CONFIG_FILE_PATH.exists():
    message = f"BIDS validation config file not found at {BIDS_VALIDATION_CONFIG_FILE_PATH}!"
    raise FileNotFoundError(message)


def run(limit: int | None = None) -> None:
    client = dandi.dandiapi.DandiAPIClient()
    dandisets = list(client.get_dandisets())
    dandisets.sort(key=lambda dandiset: int(dandiset.identifier))

    if MAX_WORKERS is None or MAX_WORKERS != 0:
        with concurrent.futures.ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []

            for counter, dandiset in enumerate(dandisets):
                if limit is not None and counter >= limit:
                    break

                dandiset_id = dandiset.identifier
                futures.append(executor.submit(_run_bids_validation, dandiset_id=dandiset_id))

            collections.deque(concurrent.futures.as_completed(futures), maxlen=0)
    elif MAX_WORKERS == 1:
        for counter, dandiset in enumerate(dandisets):
            if limit is not None and counter >= limit:
                break

            dandiset_id = dandiset.identifier
            _run_bids_validation(dandiset_id=dandiset_id, branch_name=BRANCH_NAME)


def _run_bids_validation(dandiset_id: str, branch_name: str = "draft") -> None:
    print(f"Running BIDS validation Dandiset {dandiset_id}...")

    repo_name = f"bids-dandisets/{dandiset_id}"
    repo_api_url = f"{BASE_GITHUB_API_URL}/{repo_name}/branches/{branch_name}"
    response = requests.get(url=repo_api_url, headers=AUTHENTICATION_HEADER)
    if response.status_code != 200:
        print(f"\tStatus code {response.status_code}: {response.json()["message"]}")

        if response.status_code == 403:  # TODO: Not sure how to handle this yet
            return

        print("\tError with repository - skipping...\n\n")
        return

    # Clone BIDS-Dandiset repository
    print(f"\tCloning GitHub repository for Dandiset {dandiset_id} on branch {branch_name}...")
    repo_url = f"{BASE_GITHUB_URL}/{repo_name}.git@{branch_name}"
    _deploy_subprocess(command=f"git clone {repo_url}", cwd=WORKDIR)

    # Run BIDS validation
    repo_directory = WORKDIR / repo_name
    derivatives_directory = repo_directory / "derivatives"
    validations_directory = derivatives_directory / "validations"
    bids_validation_file_path = validations_directory / "bids_validation.txt"
    bids_validation_json_file_path = validations_directory / "bids_validation.json"

    bids_validator_command = (
        f"bids-validator-deno --ignoreNiftiHeaders --outfile {bids_validation_file_path} "
        "--schema https://bids-specification--1705.org.readthedocs.build/en/1705/schema.json "
        f"--config {BIDS_VALIDATION_CONFIG_FILE_PATH} "
        f"{repo_directory}"
    )
    _deploy_subprocess(command=bids_validator_command, ignore_errors=True)  # Annoyingly always returns 1 on warnings

    bids_validator_json_command = (
        f"bids-validator-deno --ignoreNiftiHeaders --verbose --json --outfile {bids_validation_json_file_path} "
        "--schema https://bids-specification--1705.org.readthedocs.build/en/1705/schema.json "
        f"--config {BIDS_VALIDATION_CONFIG_FILE_PATH} "
        f"{repo_directory}"
    )
    _deploy_subprocess(command=bids_validator_json_command, ignore_errors=True)

    if bids_validation_json_file_path.exists():
        with bids_validation_json_file_path.open(mode="r") as file_stream:
            content = json.load(fp=file_stream)
        with bids_validation_json_file_path.open(mode="w") as file_stream:
            json.dump(obj=content, fp=file_stream, indent=2)

    # Push changes
    _configure_git_repo(repo_directory=repo_directory)
    _push_changes(repo_directory=repo_directory)

    print(f"Process complete for Dandiset {dandiset_id}!\n\n")


def _deploy_subprocess(
    *,
    command: str | list[str],
    cwd: str | pathlib.Path | None = None,
    environment_variables: dict[str, str] | None = None,
    error_message: str | None = None,
    ignore_errors: bool = False,
    return_combined_output: bool = False,
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
    if result.returncode != 0 and ignore_errors is True and return_combined_output is False:
        return None

    if return_combined_output is True:
        combined_out = f"stdout: {result.stdout}\nstderr: {result.stderr}"
        return combined_out
    else:
        return result.stdout


def _configure_git_repo(repo_directory: pathlib.Path) -> None:
    _deploy_subprocess(
        command='git config --local user.email "github-actions[bot]@users.noreply.github.com"', cwd=repo_directory
    )
    _deploy_subprocess(command='git config --local user.name "github-actions[bot]"', cwd=repo_directory)


def _push_changes(repo_directory: pathlib.Path) -> None:
    _deploy_subprocess(command="git add .", cwd=repo_directory)
    _deploy_subprocess(command='git commit --message "update BIDS validation"', cwd=repo_directory, ignore_errors=True)
    _deploy_subprocess(command="git push", cwd=repo_directory)


if __name__ == "__main__":
    run(limit=DANDISET_LIMIT)
