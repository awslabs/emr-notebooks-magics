import os
from setuptools import setup, find_packages
from shutil import copy
from pathlib import Path

setup(
    name="emr-notebooks-magics",
    version="0.1",
    description="Jupyter Magics for EMR Notebooks.",
    packages=["emr_notebooks_magics", "emr_notebooks_magics.utils"],
    install_requires=[
          'boto3',
    ],
    author_email='emrnotebooks@amazon.com',
)

dst = os.path.expanduser("~/.ipython/profile_default/startup/")
Path(dst).mkdir(parents=True, exist_ok=True)
copy("./startup_script/001-setup-emr-notebook-magics.py", dst)

