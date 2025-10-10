import importlib
import importlib.metadata
import json
import os
import pathlib
import platform
import shutil
import subprocess
import time
import warnings

import dandi.dandiapi
import nwb2bids
import requests

pynwb_warnings_to_suppress = [
    ".*Series .+: Length of .+",
    ".+ which is not compliant with .+",
    ".+ The second dimension of data .+",
    "Loaded namespace .+",
]
for message in pynwb_warnings_to_suppress:
    warnings.filterwarnings(action="ignore", category=UserWarning, message=message)

MAX_WORKERS = None
LIMIT_SESSIONS = None
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
RAW_CONTENT_BASE_URL = "https://raw.githubusercontent.com/bids-dandisets"

SYSTEM = platform.system()
if SYSTEM == "Windows":
    # For Cody's local running
    BASE_DIRECTORY = pathlib.Path("E:/GitHub/bids-dandisets")
else:
    # For CI
    BASE_DIRECTORY = pathlib.Path.cwd() / "bids-dandisets"
    BASE_DIRECTORY.mkdir(exist_ok=True)


AUTHENTICATION_HEADER = {"Authorization": f"token {GITHUB_TOKEN}"}

# Config is likely temporary to suppress the 'unknown version' because we run from BEP32 schema
THIS_FILE_PATH = pathlib.Path(__file__)
BIDS_VALIDATION_CONFIG_FILE_PATH = THIS_FILE_PATH.parent.parent / "bids_validation_config.json"


def run(limit: int | None = None) -> None:
    script_repo_path = pathlib.Path(__file__).parents[1]

    version_tag_command = "git describe --tags --always"
    generation_script_version = _deploy_subprocess(command=version_tag_command, cwd=script_repo_path).strip()
    print(f"\nGeneration script version: {generation_script_version}")

    nwb2bids_repo_path = pathlib.Path(nwb2bids.__file__).parents[1]
    nwb2bids_version = _deploy_subprocess(command=version_tag_command, cwd=nwb2bids_repo_path).strip()
    print(f"nwb2bids version: {nwb2bids_version}\n\n")

    run_info = {
        "generation_script_version": generation_script_version,
        "nwb2bids_version": nwb2bids_version,
        "limit": LIMIT_SESSIONS,
        "sessions_converted": None,
        "total_sessions": None,
    }

    client = dandi.dandiapi.DandiAPIClient()
    dandisets = list(client.get_dandisets())
    dandisets.sort(key=lambda dandiset: int(dandiset.identifier))

    for counter, dandiset in enumerate(dandisets):
        if limit is not None and counter >= limit:
            break

        dandiset_id = dandiset.identifier
        repo_directory = BASE_DIRECTORY / dandiset_id

        _convert_dandiset(dandiset_id=dandiset_id, repo_directory=repo_directory, run_info=run_info)


def _convert_dandiset(dandiset_id: str, repo_directory: pathlib.Path, run_info: dict) -> None:
    print(f"Processing Dandiset {dandiset_id}...")

    repo_name = f"bids-dandisets/{dandiset_id}"
    repo_api_url = f"{BASE_GITHUB_API_URL}/{repo_name}"
    response = requests.get(url=repo_api_url, headers=AUTHENTICATION_HEADER)
    if response.status_code != 200:
        print(f"Status code {response.status_code}: {response.json()["message"]}")

        if response.status_code == 403:  # TODO: Not sure how to handle this yet
            return

        print(f"\tForking GitHub repository for {dandiset_id} ...")

        repo_fork_url = f"https://api.github.com/repos/dandisets/{dandiset_id}/forks"
        headers = {"Authorization": f"token {GITHUB_TOKEN}", "Accept": "application/vnd.github+json"}
        data = {"organization": "bids-dandisets"}
        response = requests.post(url=repo_fork_url, headers=headers, json=data)
        if response.status_code != 202:
            print(f"\tStatus code {response.status_code}: {response.json()['message']}")

            return
        time.sleep(30)  # Give it some time to complete

    # Decide whether to skip based on hidden details of generation runs
    run_info_url = f"{RAW_CONTENT_BASE_URL}/{dandiset_id}/draft/.nwb2bids/run_info.json"
    response = requests.get(url=run_info_url, headers=AUTHENTICATION_HEADER)
    if response.status_code == 200:
        previous_run_info = response.json()
        previous_generation_script_version = previous_run_info.get("generation_script_version", "")
        previous_nwb2bids_version = previous_run_info.get("nwb2bids_version", "")
        previous_session_limit = previous_run_info.get("limit", None) or 0
        if (
            previous_generation_script_version == run_info["generation_script_version"]
            and previous_nwb2bids_version == run_info["nwb2bids_version"]
            and (LIMIT_SESSIONS is None or LIMIT_SESSIONS <= previous_session_limit)
        ):
            print(f"Skipping {dandiset_id} - already up to date!\n\n")

            return
    elif response.status_code == 403:  # TODO: Not sure how to handle this yet
        return

    # Clone the repo or fetch the latest changes
    if not repo_directory.exists():
        print(f"\tCloning GitHub repository for Dandiset {dandiset_id}...")

        repo_url = f"{BASE_GITHUB_URL}/{repo_name}"
        _deploy_subprocess(command=f"git clone {repo_url}", cwd=BASE_DIRECTORY)
    else:
        _deploy_subprocess(command="git fetch", cwd=repo_directory)
    _configure_git_repo(repo_directory=repo_directory)
    _update_draft(repo_directory=repo_directory)

    print(f"Converting {dandiset_id}...")

    dataset_converter = nwb2bids.DatasetConverter.from_remote_dandiset(dandiset_id=dandiset_id, limit=LIMIT_SESSIONS)
    dataset_converter.extract_metadata()

    print(f"Updating draft of {dandiset_id} ...")

    _write_bids_dandiset(dataset_converter=dataset_converter, repo_directory=repo_directory, run_info=run_info)
    _push_changes(repo_directory=repo_directory, branch_name="draft")

    # TODO: only make other branches for config options like sanitization
    # try:
    #     _deploy_subprocess(command=f"git checkout -b {nwb2bids_version}", cwd=repo_directory)
    # except RuntimeError:
    #     _deploy_subprocess(command=f"git checkout {nwb2bids_version}", cwd=repo_directory)
    #
    #     print("\tUpdating commit branch...")
    #     _write_bids_dandiset(
    #         dataset_converter=dataset_converter, repo_directory=repo_directory, run_info=run_info
    #     )
    # _push_changes(repo_directory=repo_directory, branch_name=nwb2bids_version)

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


def _write_bids_dandiset(
    dataset_converter: nwb2bids.DatasetConverter, repo_directory: pathlib.Path, run_info: dict
) -> None:
    nwb2bids_info_directory = repo_directory / ".nwb2bids"
    nwb2bids_info_directory.mkdir(exist_ok=True)
    run_info_file_path = nwb2bids_info_directory / "run_info.json"

    bids_ignore_file_path = repo_directory / ".bidsignore"
    derivatives_directory = repo_directory / "derivatives"
    derivatives_dataset_description_file_path = derivatives_directory / "dataset_description.json"
    validations_directory = derivatives_directory / "validations"
    nwb2bids_notifications_file_path = validations_directory / "nwb2bids_notifications.json"
    # nwb_inspector_version = importlib.metadata.version(distribution_name="nwbinspector").replace(".", "-")
    # nwb_inspection_file_path = inspections_directory / f"src-nwb-inspector_ver-{nwb_inspector_version}.txt"
    bids_validation_file_path = validations_directory / "bids_validation.txt"
    bids_validation_json_file_path = validations_directory / "bids_validation.json"
    # dandi_validation_file_path = validations_directory / "dandi_validation.txt"

    # Cleanup existing content from original fork or previous runs
    current_content = [path for path in repo_directory.iterdir() if not path.name.startswith(".") and path.is_dir()]
    for path in current_content:
        shutil.rmtree(path=path)
    if derivatives_directory.exists():
        shutil.rmtree(path=derivatives_directory)
    derivatives_directory.mkdir(exist_ok=True)
    validations_directory.mkdir(exist_ok=True)

    dataset_converter.convert_to_bids_dataset(bids_directory=repo_directory)

    # Required for BIDs validation on Dandisets
    bids_ignore_file_path.write_text("dandiset.yaml\n")

    # Required for BIDs validation on inspection derivatives
    derivatives_dataset_description = {
        "BIDSVersion": "1.10.0",
        "DatasetType": "derivative",
        "Name": f"Inspections and Validations for BIDS-Dandiset {repo_directory.stem}",
        "SourceDatasets": [{"URL": "../"}],
    }
    derivatives_dataset_description_file_path.write_text(json.dumps(obj=derivatives_dataset_description))

    notifications_dump = [notification.model_dump(mode="json") for notification in dataset_converter.messages]
    if len(notifications_dump) > 0:
        nwb2bids_notifications_file_path.write_text(data=json.dumps(obj=notifications_dump, indent=2))

    # if nwb_inspection_file_path.exists() is False:
    #     dandiset_id = repo_directory.name
    #     nwb_inspector_command = (
    #         f"nwbinspector --report-file-path {nwb_inspection_file_path} --overwrite --stream {dandiset_id}"
    #     )
    #     _deploy_subprocess(command=nwb_inspector_command, ignore_errors=True)

    bids_validator_command = (
        f"bids-validator-deno --ignoreNiftiHeaders --verbose --outfile {bids_validation_file_path} "
        "--schema https://raw.githubusercontent.com/bids-standard/bids-schema/enh-prs-and-beps/BEPs/32/schema.json "
        f"--config {BIDS_VALIDATION_CONFIG_FILE_PATH} "
        f"{repo_directory}"
    )
    _deploy_subprocess(
        command=bids_validator_command
    )  # , ignore_errors=True)  # Annoyingly always returns 1 on warnings

    bids_validator_json_command = (
        f"bids-validator-deno --ignoreNiftiHeaders --verbose --json --outfile {bids_validation_json_file_path} "
        "--schema https://raw.githubusercontent.com/bids-standard/bids-schema/enh-prs-and-beps/BEPs/32/schema.json "
        f"--config {BIDS_VALIDATION_CONFIG_FILE_PATH} "
        f"{repo_directory}"
    )
    _deploy_subprocess(command=bids_validator_json_command)  # , ignore_errors=True)

    with bids_validation_json_file_path.open(mode="r") as file_stream:
        content = json.load(fp=file_stream)
    with bids_validation_json_file_path.open(mode="w") as file_stream:
        json.dump(obj=content, fp=file_stream, indent=2)

    # _deploy_subprocess(command=f"dandi validate {repo_directory} > {dandi_validation_file_path}", ignore_errors=True)

    # Write last as a sign of completion
    # Temporary
    old_file_path = repo_directory / ".run_info.json"
    old_file_path.unlink(missing_ok=True)

    dandiset_run_info = run_info.copy()
    session_subdirectories = {
        path
        for path in repo_directory.rglob(pattern="*ses-*")
        if path.is_dir() and len(set(path.rglob(pattern="*.nwb"))) > 0
    }
    dandiset_run_info["sessions_converted"] = len(session_subdirectories)
    if LIMIT_SESSIONS is None:
        dandiset_run_info["total_sessions"] = len(dataset_converter.session_converters)
    elif LIMIT_SESSIONS is not None and dandiset_run_info["sessions_converted"] < LIMIT_SESSIONS:
        dandiset_run_info["total_sessions"] = f"{LIMIT_SESSIONS}+"
    elif LIMIT_SESSIONS is not None and dandiset_run_info["sessions_converted"] == LIMIT_SESSIONS:
        dandiset_run_info["total_sessions"] = "???"

    with run_info_file_path.open(mode="w") as file_stream:
        json.dump(obj=dandiset_run_info, fp=file_stream, indent=2)


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
