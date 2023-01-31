import os
from setuptools import setup, find_packages
from shutil import copy
from pathlib import Path

setup(
    name="emr-notebooks-magics",
    version="0.2.3",
    description="Jupyter Magics for EMR Notebooks.",
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    packages=["emr_notebooks_magics", "emr_notebooks_magics.utils"],
    install_requires=[
          'boto3',
    ],
    author_email='emrnotebooks@amazon.com',
    scripts=['startup_script/001-setup-emr-notebook-magics.py'],
    classifiers=[
        "Development Status :: 4 - Beta",
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: Apache Software License',
    ],
)

dst = os.path.expanduser("~/.ipython/profile_default/startup/")
Path(dst).mkdir(parents=True, exist_ok=True)
copy("./startup_script/001-setup-emr-notebook-magics.py", dst)

