import importlib
import importlib.metadata
import json
import os
import pathlib
import shutil
import subprocess

import dandi.dandiapi
import nwb2bids
import requests

LIMIT_SESSIONS = 10
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
        repo_api_url = f"{BASE_GITHUB_API_URL}/{repo_name}"
        response = requests.get(url=repo_api_url, headers={"Authorization": f"token {GITHUB_TOKEN}"})
        if response.status_code != 200:
            print(f"Status code {response.status_code}: {response.json()["message"]}")

            if response.status_code == 403:  # TODO: Not sure how to handle this yet
                continue

            print("\tCreating GitHub repository...")
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
            if response.status_code == 403:
                print(f"\tStatus code {response.status_code}: {response.json()['message']}")
                continue

        if not repo_directory.exists():
            print(f"\tCloning GitHub repository for Dandiset {dandiset_id}...")

            repo_url = f"{BASE_GITHUB_URL}/{repo_name}"
            _deploy_subprocess(command=f"git clone {repo_url}", cwd=BASE_DIRECTORY)
        else:
            _deploy_subprocess(command="git fetch", cwd=repo_directory)
        _configure_git_repo(repo_directory=repo_directory)
        _update_draft(repo_directory=repo_directory)

        print(f"Converting {dandiset_id}...")
        dataset_converter = nwb2bids.DatasetConverter.from_remote_dandiset(
            dandiset_id=dandiset_id, limit=LIMIT_SESSIONS
        )
        dataset_converter.extract_metadata()

        print("Updating draft...")
        _write_bids_dandiset(dataset_converter=dataset_converter, repo_directory=repo_directory)

        _push_changes(repo_directory=repo_directory, branch_name="draft")

        try:
            _deploy_subprocess(command=f"git checkout -b {commit_hash}", cwd=repo_directory)
        except RuntimeError:
            _deploy_subprocess(command=f"git checkout {commit_hash}", cwd=repo_directory)

            print("\tUpdating commit branch...")
            _write_bids_dandiset(dataset_converter=dataset_converter, repo_directory=repo_directory)
        _push_changes(repo_directory=repo_directory, branch_name=commit_hash)

        print(f"Process complete for Dandiset {dandiset_id}!\n\n")


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


def _write_bids_dandiset(dataset_converter: nwb2bids.DatasetConverter, repo_directory: pathlib.Path) -> None:
    raw_directory = repo_directory / "raw"
    derivatives_directory = repo_directory / "derivatives"
    inspections_directory = derivatives_directory / "inspections"
    nwb2bids_inspection_file_path = inspections_directory / "nwb2bids_inspection.json"
    nwb_inspector_version = importlib.metadata.version(distribution_name="nwbinspector").replace(".", "-")
    nwb_inspection_file_path = inspections_directory / f"src-nwb-inspector_ver-{nwb_inspector_version}.txt"
    bids_validation_file_path = inspections_directory / "bids_validation.txt"
    bids_validation_json_file_path = inspections_directory / "bids_validation.json"

    if raw_directory.exists():
        shutil.rmtree(path=raw_directory)
    raw_directory.mkdir(exist_ok=True)
    derivatives_directory.mkdir(exist_ok=True)
    inspections_directory.mkdir(exist_ok=True)
    nwb2bids_inspection_file_path.unlink(missing_ok=True)
    bids_validation_file_path.unlink(missing_ok=True)
    bids_validation_json_file_path.unlink(missing_ok=True)
    # TODO: write dataset_description.json and README for inspections pipeline
    # TODO: write dataset_description.json and README for entire 'study'
    # QUESTION FOR YARIK: does this repo itself need to be nested under a 'study-<label>' directory?

    dataset_converter.convert_to_bids_dataset(bids_directory=raw_directory)

    message_dump = [message.model_dump() for message in dataset_converter.messages]
    if len(message_dump) > 0:
        nwb2bids_inspection_file_path.write_text(data=json.dumps(obj=message_dump, indent=2))

    if nwb_inspection_file_path.exists() is False:
        dandiset_id = repo_directory.name
        nwb_inspector_command = (
            f"nwbinspector --report-file-path {nwb_inspection_file_path} --overwrite --stream {dandiset_id} --n-jobs -1"
        )
        _deploy_subprocess(command=nwb_inspector_command, ignore_errors=True)

    bids_validator_command = (
        f"bids-validator-deno --ignoreNiftiHeaders --verbose --outfile {bids_validation_file_path} "
        "--schema https://raw.githubusercontent.com/bids-standard/bids-schema/enh-prs-and-beps/BEPs/32/schema.json "
        f"{raw_directory}"
    )
    _deploy_subprocess(command=bids_validator_command, ignore_errors=True)

    bids_validator_json_command = (
        f"bids-validator-deno --ignoreNiftiHeaders --verbose --json --outfile {bids_validation_json_file_path} "
        "--schema https://raw.githubusercontent.com/bids-standard/bids-schema/enh-prs-and-beps/BEPs/32/schema.json "
        f"{raw_directory}"
    )
    _deploy_subprocess(command=bids_validator_json_command, ignore_errors=True)
    with bids_validation_json_file_path.open(mode="r") as file_stream:
        content = json.load(fp=file_stream)
    with bids_validation_json_file_path.open(mode="w") as file_stream:
        json.dump(obj=content, fp=file_stream, indent=2)


def _configure_git_repo(repo_directory: pathlib.Path) -> None:
    _deploy_subprocess(
        command='git config --local user.email "github-actions[bot]@users.noreply.github.com"', cwd=repo_directory
    )
    _deploy_subprocess(command='git config --local user.name "github-actions[bot]"', cwd=repo_directory)


def _update_draft(repo_directory: pathlib.Path) -> None:
    _deploy_subprocess(command="git checkout draft", cwd=repo_directory)
    _deploy_subprocess(command="git pull", cwd=repo_directory)


def _push_changes(repo_directory: pathlib.Path, branch_name: str) -> None:
    _deploy_subprocess(command="git add .", cwd=repo_directory)
    _deploy_subprocess(command='git commit --message "update"', cwd=repo_directory, ignore_errors=True)

    push_command = "git push" if branch_name == "draft" else f"git push --set-upstream origin {branch_name}"
    _deploy_subprocess(command=push_command, cwd=repo_directory)


if __name__ == "__main__":
    run(limit=LIMIT_DANDISETS)
