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
def load_ipython_extension(ipython):
    try:
        from emr_notebooks_magics import MountWorkspaceDirMagics, S3DownloadMagics, ExecuteNotebookMagics
        mount_ws_dir_magics = MountWorkspaceDirMagics(ipython)
        download_S3_magics = S3DownloadMagics(ipython)
        execute_notebook_magics = ExecuteNotebookMagics(ipython)
        ipython.register_magics(mount_ws_dir_magics)
        ipython.register_magics(download_S3_magics)
        ipython.register_magics(execute_notebook_magics)
    except ImportError:
        pass

load_ipython_extension(get_ipython())
