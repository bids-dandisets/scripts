import os
import pathlib
import importlib

import requests
import subprocess

import dandi.dandiapi
import nwb2bids

LIMIT_SESSIONS = 2
LIMIT_DANDISETS = 2

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", None)
if GITHUB_TOKEN is None:
    message = "GITHUB_TOKEN environment variable not set"
    raise ValueError(message)

if "site-packages" in importlib.util.find_spec("nwb2bids").origin:
    message = "nwb2bids is installed in site-packages - please install in editable mode"
    raise RuntimeError(message)

BASE_GITHUB_URL = "https://api.github.com/repos"
BASE_DIRECTORY = pathlib.Path("E:/GitHub/bids-dandisets")


def run(limit: int | None = None) -> None:
    commit_hash = (
        subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=pathlib.Path(nwb2bids.__file__).parents[1]
        )
        .strip()
        .decode()
    )
    print(f"\nnwb2bids commit hash: {commit_hash}\n\n")

    client = dandi.dandiapi.DandiAPIClient()
    dandisets = client.get_dandisets()

    for counter, dandiset in enumerate(dandisets):
        if limit is not None and counter >= limit:
            break

        dandiset_id = dandiset.identifier
        repo_directory = BASE_DIRECTORY / dandiset_id

        dataset_converter = nwb2bids.DatasetConverter.from_remote_dandiset(
            dandiset_id=dandiset_id
        )
        if len(dataset_converter.session_converters) == 0:
            print(f"No NWB files found in Dandiset {dandiset_id}, skipping...")

            continue

        repo_name = f"bids-dandisets/{dandiset_id}"
        repo_url = f"{BASE_GITHUB_URL}/{repo_name}"
        response = requests.get(repo_url)
        if response.status_code == 404:
            print(f"Creating GitHub repository for Dandiset {dandiset_id}...")

            headers = {"Authorization": f"token {GITHUB_TOKEN}"}
            data = {"name": repo_name}
            response = requests.post(repo_url, headers=headers, json=data)
            response.raise_for_status()
        else:
            subprocess.run(args=["git", "fetch"], cwd=repo_directory)

        if not repo_directory.exists():
            print(f"Cloning GitHub repository for Dandiset {dandiset_id}...")

            subprocess.run(args=["git", "clone", repo_url], cwd=BASE_DIRECTORY)

        print(f"Converting {dandiset_id}...")
        dataset_converter.extract_metadata()
        dataset_converter.convert_to_bids_dataset(
            bids_directory=repo_directory, limit=LIMIT_SESSIONS
        )

        print(f"Pushing updates to GitHub repository for Dandiset {dandiset_id}...")
        email_config_command = [
            "git",
            "config",
            "--local",
            "user.email",
            '"github-actions[bot]@users.noreply.github.com"',
        ]
        subprocess.run(args=email_config_command, cwd=repo_directory)
        subprocess.run(
            args=["git", "config", "--local", "user.name ", '"github-actions[bot]"'],
            cwd=repo_directory,
        )
        subprocess.run(
            args=["git", "checkout", "--branch", commit_hash], cwd=repo_directory
        )
        subprocess.run(args=["git", "add", "."], cwd=repo_directory)
        subprocess.run(
            args=["git", "commit", "--message", '"update"'], cwd=repo_directory
        )
        subprocess.run(
            args=["git", "push", "--set-upstream", "origin", commit_hash],
            cwd=repo_directory,
        )

        print(f"\nProcess complete for Dandiset {dandiset_id}!\n\n")


if __name__ == "__main__":
    run(limit=LIMIT_DANDISETS)
