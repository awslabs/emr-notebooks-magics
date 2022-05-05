# EMR Notebooks iPython Magics

This repository contains iPython magics that can be used in Amazon EMR Notebooks.

## Table of Contents
1. [Installation](#Installation)
2. [Usage](#Usage)
3. [Security](#Security)
4. [License](#License)


## Installation

### Installing Dependencies
`%mount_workspace_dir` magic mounts the Workspace using [S3-FUSE](https://github.com/s3fs-fuse/s3fs-fuse) or [Goofys](https://github.com/kahing/goofys).

* Installing S3-FUSE

  Add the following lines to your cluster bootstrap action script.
  ```
  #!/bin/sh

  sudo amazon-linux-extras install epel -y
  sudo yum install s3fs-fuse -y
  ```

* Installing Goofys

  Add the following lines to your cluster bootstrap action script.
  ```
  #!/bin/sh

  sudo wget https://github.com/kahing/goofys/releases/latest/download/goofys -P /usr/bin/
  sudo chmod ugo+x /usr/bin/goofys
  ```

Installing iPython magics

* Using EMR Step.

  EMR step script
  ```
  #!/bin/sh
  sudo -u emr-notebook /mnt/notebook-env/bin/pip install emr-notebooks-magics
  ```

* From Jupyter Notebook
  ```
  %pip install emr-notebooks-magics
  ```
The magics are loaded using kernel startup script. If you install magics from Jupyter Notebook, you will need to restart the kernel before using the magic.

Note: EMR-notebook-magics cannot be installed through bootstrap actions as JEG and Notebook environments are installed after the bootstrap.

## Usage
* `%generate_s3_download_url` magic generates presigned url for S3 objects so that it can be downloaded from the Jupyter Notebook.
  Refer `%generate_s3_download_url?` for help.
    * Generate download url for a S3 object specifying full S3 path.
      ```
      %generate_s3_download_url s3://my_bucket/path/to/s3/object
      ```

    * Generate download url for a file in the Workspace specifying path relative to Workspace root.
      ```
      %generate_s3_download_url relative/path/to/workspace/file
      ```

* `%mount_workspace_dir` magic mounts Workspace files on the EMR cluster instance using FUSE based filesystem.
  Refer `%mount_workspace_dir?` for help.
    * Mount the entire Workspace onto EMR cluster instance.
      ```
      %mount_workspace_dir .
      ```

    * Mount a sub-directory `mydirectory` and add `use_cache` mount option of S3-FUSE
      ```
      %mount_workspace_dir mydirectory --params use_cache=/tmp/
      ```

    * Mount a sub-directory `mydirectory` and add `cheap`, `region` mount option for Goofys.
      ```
      %mount_workspace_dir mydirectory --use goofys --params cheap,region=us-east-1
      ```

| :exclamation:  Warnings                  |
|-----------------------------------------|
| When the write access is enabled, any changes made to the mount directory are applied to the S3 Workspace. These changes are irreversible, please enable S3 versioning to your S3 Workspace as a pre-caution. |
| Once the Workspace is mounted on the EMR cluster, it can be accessed from all EMR Notebooks in your account that can attach to that cluster. |
| When you install S3-FUSE or Goofys, its your responsibility to keep those package up to date for new patches. Since Goofys is not managed by any package managers, take necessary steps to upgrade Goofys binaries. |  |

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.

