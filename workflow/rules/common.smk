
import atexit
import os
import shutil
import sys
import tempfile

from pathlib import Path
from typing import Iterable, Union

from snakemake.utils import validate


#### Constants ####
Extensions = ('.mrxs', '.svs', '.ndpi')


#### Configuration ####
validate(config, schema="../schemas/config.schema.yml")  # also sets default values

envvars:
    "TMPDIR"

##### Helper functions #########

def log(*args) -> None:
    print(*args, file=sys.stderr)


def get_repository_path() -> Path:
    return Path(config['repository']['path']).resolve()


def delete_temp_directory(tmp_dir: Union[str, Path]) -> None:
    log("Deleting temporary directory", tmp_dir)
    shutil.rmtree(tmp_dir, ignore_errors=True)


def setup_tmp_dir() -> None:
    """
    Creates a temporary directory for the workflow run.
    """
    tmp_dir = tempfile.mkdtemp(prefix='img-convert-wf', dir=os.getcwd())
    log("Created temporary directory", tmp_dir)
    os.environ['TMPDIR'] = tmp_dir
    atexit.register(delete_temp_directory, tmp_dir)


def get_tmp_dir() -> str:
    """
    Get temporary directory created within working directory.
    """
    return os.environ['TMPDIR']


def glob_source_paths() -> Iterable[Path]:
    base_dir = str(get_repository_path())
    source_paths = [ Path(p) for p in config['sources']['items'] ]
    if any(p.is_absolute() for p in source_paths):
        raise ValueError("Source paths must be relative to repository.path (absolute paths found).")
    # glob any directories for files that end with any of the Extensions
    try:
        cwd = os.getcwd()
        os.chdir(base_dir)
        source_files = \
            [ Path(os.path.join(root, slide))
                for p in source_paths if p.is_dir()
                for root, _, files in os.walk(p)
                for slide in files if any(slide.endswith(ext) for ext in Extensions) ] + \
            [ Path(p) for p in source_paths if p.is_file() and any(p.suffix == ext for ext in Extensions) ]
    finally:
        os.chdir(cwd)
    return source_files


###### Input functions ######
def gen_rule_input_path(wildcard):
    """
    Given the wildcard, tries all Extensions until it
    finds one that matches a file name in the repository.
    """
    for suffix in Extensions:
        path = get_repository_path() / f"{wildcard.slide}{suffix}"
        if path.exists():
            return path
    return ''


###### Environment configuration functions ######
def configure_environment():
    setup_tmp_dir()

    shell.prefix("set -o pipefail; ")

    if workflow.use_singularity:
        # Bind mount the repository path into container.
        # Ideally we want to mount the repository in read-only mode.
        # To avoid making the working directory read-only should it be inside
        # or the same path as the working directory, we check for this case
        # and if true we mount read-write.
        repository = Path(config['repository']['path']).resolve()
        work_dir = Path.cwd()
        if repository == work_dir or repository in work_dir.parents:
            mount_options = 'rw'
        else:
            mount_options = 'ro'
        workflow.singularity_args += ' '.join([
            f" --bind {repository}:{repository}:{mount_options}",
            f" --env TMPDIR={get_tmp_dir()}"])
