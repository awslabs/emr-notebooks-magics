# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os
import json
import subprocess
import boto3
import shlex

from IPython.core import magic_arguments
from IPython.core.magic import (Magics, magics_class, line_magic)
from IPython.core.error import UsageError
from pathlib import Path
from shutil import which
from .utils.str_utils import remove_prefix


@magics_class
class MountWorkspaceDirMagics(Magics):
    """
    Magic class to mount EMR Workspace directory.
    """

    def __init__(self, shell):
        super(MountWorkspaceDirMagics, self).__init__(shell)
        self.s3_client = boto3.client('s3')

    @magic_arguments.magic_arguments()
    @magic_arguments.argument(
        'ws_path',
        help="""Relative path of the EMR workspace to be mounted"""
    )
    @magic_arguments.argument(
        '--fuse-type', default='s3-fuse',
        help="""use S3-FUSE/Goofys to mount the Workspace directory"""
    )
    @magic_arguments.argument(
        '--params', default=None,
        help="""S3-FUSE/Goofys mount params. 
            These comma separated params can be used to override the S3-FUSE/Goofys mount options.
            For S3-FUSE mount options: https://github.com/s3fs-fuse/s3fs-fuse/wiki/Fuse-Over-Amazon#options
            For Goofys mount options: run `goofys --help`
            """
    )
    @line_magic
    def mount_workspace_dir(self, line):
        """
        Mount EMR workspace directory on the remote instance.
        In EMR notebooks, the Jupyter kernels are launched on the remote EMR cluster using Jupyter Enterprise Gateway.
        In order to access Workspace files (for e.g. to import a python module) Workspace has to be mounted on the EMR cluster.

        By default, the Workspace directory is mounted to read-only directory.
        Caution: if you enable writes, any changes you make to the mount directory will overwrite the contents in the Workspace.
        Usage:
            mount_workspace_dir .
            mount_workspace_dir mydirectory
            mount_workspace_dir mydirectory --fuse-type s3-fuse --params use_cache=/tmp/
            mount_workspace_dir mydirectory --fuse-type goofys --params cheap,region=us-east-1
        """
        
        args = magic_arguments.parse_argstring(self.mount_workspace_dir, line)
        mount_dir = self._get_mount_directory()

        # Check if the mount_dir is already mounted.
        # Users want to repeatedly execute their Notebook so we do NOT want to throw an error when the mount dir is already mounted.
        if self._is_already_mounted(mount_dir):
            print("The mount directory is already mounted. Skipping mounting.")
            os.chdir(mount_dir)
            return

        s3_bucket = os.environ["KERNEL_WORKSPACE_DIR_S3_BUCKET"]
        s3_key = os.environ["KERNEL_WORKSPACE_DIR_S3_LOCATION"]

        # remove "." and "./" from source file path
        if args.ws_path.startswith("./"):
            args.ws_path = remove_prefix(args.ws_path, "./")
        elif args.ws_path.startswith("."):
            args.ws_path = remove_prefix(args.ws_path, ".")
        
        s3_key = s3_key + args.ws_path

        if not self._is_valid_workspace_directory(s3_bucket, s3_key):
            raise UsageError("{} is not a valid Workspace directory".format(args.ws_path))

        if args.fuse_type == "s3-fuse":
            ret_code, stdout, stderr = self.mount_using_s3fuse(s3_bucket, s3_key, mount_dir, args.params, True)
        elif args.fuse_type == "goofys":
            ret_code, stdout, stderr = self.mount_using_goofys(s3_bucket, s3_key, mount_dir, args.params, True)
        else:
            raise UsageError("Unknown mount option:{}".format(args.fuse_type))

        if ret_code != 0:
            raise UsageError("Unable to mount the Workspace. stdout={} stderr={}".format(stdout, stderr))

        # Change directory to the mount folder
        os.chdir(mount_dir)

        print("Successfully mounted EMR Workspace on the cluster")
        return

    @line_magic
    def umount_workspace_dir(self, line):
        """
        Unmount Workspace directory
        """
        mount_dir = self._get_mount_directory()

        # change current directory to home directory (so that currently opened files in the mounted dir is released by python)
        os.chdir(os.path.expanduser("~"))

        command = "fusermount -u {}".format(mount_dir)
        ret_code, stdout, stderr = self._execute_command(command)
        if ret_code == 0:
            print("Successfully unmounted the directory")
        else:
            raise UsageError("Unable to unmount the Workspace. stdout={} stderr={}".format(stdout, stderr))

    def mount_using_s3fuse(self, s3_bucket, s3_key, mount_dir, params, read_only):
        if which("s3fs") is None:
            raise UsageError("S3-fuse is not installed")
        Path(mount_dir).mkdir(parents=True, exist_ok=True)
       
        mount_params = ""
        if params is not None:
            params_list = params.split(",")
            params_list = ["-o {} ".format(shlex.quote(param)) for param in params_list]
            mount_params = "".join(params_list)

        # use default iam role
        if "iam_role" not in mount_params:
            mount_params = "-o iam_role=auto " + mount_params
     
        # readonly mount by default
        if read_only and "umask" not in mount_params:
            mount_params = "-o umask=277 " + mount_params

        # mount the directory as the current user
        if "uid" not in mount_params:
            mount_params = "-o uid={} ".format(os.getuid()) + mount_params

        if "gid" not in mount_params:
            mount_params = "-o gid={} ".format(os.getgid()) + mount_params

        command = "s3fs {}{}:/{} {}".format(mount_params, s3_bucket, s3_key, mount_dir)
        print("Executing command ", command)
        return self._execute_command(command)
    
    def mount_using_goofys(self, s3_bucket, s3_key, mount_dir, params, read_only):
        if which("goofys") is None:
            raise UsageError("Goofys is not installed. Please install goofys/add goofys to PATH")
        Path(mount_dir).mkdir(parents=True, exist_ok=True)

        mount_params = ""
        if params is not None:
            params_list = params.split(",")
            params_list = ["--{} ".format(shlex.quote(param)) for param in params_list]
            mount_params = "".join(params_list)

        if read_only and "file-mode" not in mount_params:
            mount_params = "--file-mode 0400 " + mount_params

        if read_only and "dir-mode" not in mount_params:
            mount_params = "--dir-mode 0500 " + mount_params

        command = "goofys {}{}:/{} {}".format(mount_params, s3_bucket, s3_key, mount_dir)
        print("Executing command ", command)
        return self._execute_command(command)
    
    def _execute_command(self, cmd):
        cmd_list = shlex.split(cmd)
        process = subprocess.run(cmd_list, capture_output=True, text=True, shell=False)
        return process.returncode, process.stdout, process.stderr

    def _is_already_mounted(self, mount_dir):
        cmd = "findmnt {} --json".format(mount_dir)
        ret_code, std_out, std_err = self._execute_command(cmd)
        if ret_code != 0:
            return False

        try:
            mounts = json.loads(std_out)
            fs_type = mounts['filesystems'][0]['fstype']
            return True if fs_type.startswith('fuse') else False
        except:
            return False

    def _is_valid_workspace_directory(self, s3_bucket, s3_prefix):
        if not s3_prefix.endswith("/"):
            s3_prefix = s3_prefix + "/"

        result = self.s3_client.list_objects_v2(Bucket=s3_bucket,
                                                Prefix=s3_prefix,
                                                MaxKeys=1)

        return True if "Contents" in result else False

    def _get_mount_directory(self):
        workspace_id = os.environ["KERNEL_WORKSPACE_ID"]
        home_dir = os.path.expanduser("~")
        return os.path.join(home_dir, workspace_id)
