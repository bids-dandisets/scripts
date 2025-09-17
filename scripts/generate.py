import os
import pathlib

import requests
import subprocess

import dandi.dandiapi
import nwb2bids

GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN', None)
if GITHUB_TOKEN is None:
    message = "GITHUB_TOKEN environment variable not set"
    raise ValueError(message)

BASE_GITHUB_URL = "https://api.github.com/repos/bids-dandisets"
BASE_DIRECTORY = pathlib.Path("E:/GitHub/bids-dandisets")

def create_github_repo(repo_name: str) -> dict:
    """Create a new GitHub repository."""
    url = f'https://api.github.com/user/repos'
    headers = {'Authorization': f'token {GITHUB_TOKEN}'}
    data = {'name': repo_name}
    response = requests.post(url, headers=headers, json=data)
    response.raise_for_status()
    return response.json()

def run(limit: int | None = None) -> None:
    client = dandi.dandiapi.DandiapiClient()
    dandisets = client.get_dandisets()

    for counter, dandiset in enumerate(dandisets):
        if limit is not None and counter >= limit:
            break

        dandiset_id = dandiset.id

        dataset_converter = nwb2bids.DatasetConverter.from_remote_dandiset(dandiset_id=dandiset_id)
        if len(dataset_converter.session_converters) == 0:
            print(f"No NWB files found in Dandiset {dandiset_id}, skipping...")

            continue

        repo_url = f"{BASE_GITHUB_URL}/{dandiset_id}"
        response = requests.get(repo_url)
        if response.status_code == 404:
            print(f"Creating GitHub repository for Dandiset {dandiset_id}...")

            headers = {'Authorization': f'token {GITHUB_TOKEN}'}
            data = {'name': repo_name}
            response = requests.post(repo_url, headers=headers, json=data)
            response.raise_for_status()

        repo_directory = BASE_DIRECTORY / dandiset_id
        if not repo_directory.exists():
            print(f"Cloning GitHub repository for Dandiset {dandiset_id}...")

            subprocess.run(args=['git', 'clone', repo_url], cwd=BASE_DIRECTORY)

        dataset_converter.extract_metadata()
        dataset_converter.convert_to_bids_dataset(bids_directory=repo_directory)

        subprocess.run(args=["git", "config", "--local", "user.email", '"github-actions[bot]@users.noreply.github.com"'], cwd=repo_directory)
        subprocess.run(args=["git", "config", "--local", "user.name ", '"github-actions[bot]"'], cwd=repo_directory)
        subprocess.run(args=['git', 'add', "."], cwd=repo_directory)
        subprocess.run(args=['git', 'commit', '--message', '"update"'], cwd=repo_directory)
        subprocess.run(args=['git', 'push'], cwd=repo_directory)


if __name__ == '__main__':
    run(limit=2)
