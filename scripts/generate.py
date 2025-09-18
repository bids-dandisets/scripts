import collections
import importlib
import json
import os
import pathlib
import shutil
import subprocess

import dandi.dandiapi
import requests

import nwb2bids

LIMIT_SESSIONS = 2
LIMIT_DANDISETS = None

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", None)
if GITHUB_TOKEN is None:
    message = "GITHUB_TOKEN environment variable not set"
    raise ValueError(message)

if "site-packages" in importlib.util.find_spec("nwb2bids").origin:
    message = "nwb2bids is installed in site-packages - please install in editable mode"
    raise RuntimeError(message)

BASE_GITHUB_URL = f"https://{GITHUB_TOKEN}@github.com"
BASE_GITHUB_API_URL = "https://api.github.com/repos"
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


def run(limit: int | None = None) -> None:
    commit_hash = _deploy_subprocess(command="git rev-parse HEAD", cwd=pathlib.Path(nwb2bids.__file__).parents[1])[:10]
    print(f"\nnwb2bids commit hash: {commit_hash}\n\n")

    client = dandi.dandiapi.DandiAPIClient()
    dandisets = client.get_dandisets()

    for counter, dandiset in enumerate(dandisets):
        if limit is not None and counter >= limit:
            break

        dandiset_id = dandiset.identifier
        repo_directory = BASE_DIRECTORY / dandiset_id

        print(f"Processing Dandiset {dandiset_id}...")
        repo_name = f"bids-dandisets/{dandiset_id}"
        repo_url = f"{BASE_GITHUB_URL}/{repo_name}"
        repo_api_url = f"{BASE_GITHUB_API_URL}/{repo_name}"
        response = requests.get(url=repo_api_url)
        if response.status_code != 200:
            print(f"Status code {response.status_code}: {response.content}\n\tCreating GitHub repository...")

            if response.status_code == 403:
                continue

            repo_creation_url = "https://api.github.com/orgs/bids-dandisets/repos"
            headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github+json"}
            data = {
                "name": dandiset_id,
                "private": False,
                "default_branch": "draft",
                "auto_init": True,
                "description": f"BIDS-formatted version of Dandiset {dandiset_id}.",
                "has_issues": False,
                "has_projects": False,
                "has_wiki": False,
            }
            response = requests.post(url=repo_creation_url, headers=headers, json=data)
            response.raise_for_status()

        if not repo_directory.exists():
            print(f"Cloning GitHub repository for Dandiset {dandiset_id}...")

            _deploy_subprocess(command=f"git clone {repo_url}", cwd=BASE_DIRECTORY)
        else:
            _deploy_subprocess(command="git fetch", cwd=repo_directory)
        messages_file_path = repo_directory / ".messages.json"

        _deploy_subprocess(
            command='git config --local user.email "github-actions[bot]@users.noreply.github.com"', cwd=repo_directory
        )
        _deploy_subprocess(command='git config --local user.name "github-actions[bot]"', cwd=repo_directory)
        _deploy_subprocess(command="git checkout draft", cwd=repo_directory)
        _deploy_subprocess(command="git pull", cwd=repo_directory)

        print(f"Converting {dandiset_id}...")
        print("Updating draft...")
        current_content = [path for path in repo_directory.iterdir() if not path.name.startswith(".")]
        messages_file_path.unlink(missing_ok=True)
        collections.deque((shutil.rmtree(path=path, ignore_errors=True) for path in current_content), maxlen=0)

        dataset_converter = nwb2bids.DatasetConverter.from_remote_dandiset(
            dandiset_id=dandiset_id, limit=LIMIT_SESSIONS
        )
        dataset_converter.extract_metadata()
        dataset_converter.convert_to_bids_dataset(bids_directory=repo_directory)

        message_dump = [message.model_dump() for message in dataset_converter.messages]
        if len(message_dump) > 0:
            messages_file_path.write_text(data=json.dumps(obj=message_dump, indent=2))

        _deploy_subprocess(command="git add .", cwd=repo_directory)
        _deploy_subprocess(command='git commit --message "update"', cwd=repo_directory, ignore_errors=True)
        _deploy_subprocess(command="git push", cwd=repo_directory)

        try:
            _deploy_subprocess(command=f"git checkout -b {commit_hash}", cwd=repo_directory)
        except RuntimeError:
            _deploy_subprocess(command=f"git checkout {commit_hash}", cwd=repo_directory)

            print("Updating commit branch...")
            current_content = [path for path in repo_directory.iterdir() if not path.name.startswith(".")]
            messages_file_path.unlink(missing_ok=True)
            collections.deque((shutil.rmtree(path=path, ignore_errors=True) for path in current_content), maxlen=0)
            dataset_converter.extract_metadata()
            dataset_converter.convert_to_bids_dataset(bids_directory=repo_directory)

            message_dump = [message.model_dump() for message in dataset_converter.messages]
            if len(message_dump) > 0:
                messages_file_path.write_text(data=json.dumps(obj=message_dump, indent=2))

        _deploy_subprocess(command="git add .", cwd=repo_directory)
        _deploy_subprocess(command='git commit --message "update"', cwd=repo_directory, ignore_errors=True)
        _deploy_subprocess(command=f"git push --set-upstream origin {commit_hash}", cwd=repo_directory)

        print(f"Process complete for Dandiset {dandiset_id}!\n\n")


if __name__ == "__main__":
    run(limit=LIMIT_DANDISETS)
