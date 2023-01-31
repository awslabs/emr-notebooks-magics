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
import boto3
import time

from IPython.core import magic_arguments
from IPython.core.magic import (Magics, magics_class, line_magic)
from IPython.core.error import UsageError
from .utils.instance_metadata_service_utils import IMDSv2Util
from .utils.display_utils import display_html

EXECUTIONS_TERMINAL_STATUS = ["FINISHED", "FAILED"]
EXECUTIONS_STARTING_STATUS = ["STARTING", "START_PENDING"]


@magics_class
class ExecuteNotebookMagics(Magics):

    def __init__(self, shell):
        super(ExecuteNotebookMagics, self).__init__(shell)
        self.shell = shell
        self.imdsv2 = IMDSv2Util()
        self.region = self.imdsv2.get_region()
        self.ec2 = boto3.client('ec2', region_name=self.region)
        self.emr = boto3.client('emr', region_name=self.region)

    @magic_arguments.magic_arguments()
    @magic_arguments.argument(
        'file',
        help="""File to be executed"""
    )
    @magic_arguments.argument(
        '--cluster-id',
        help="""[Optional] EMR cluster to be used for executing the notebook. 
        If not specified, cluster attached to the Workspace will be used."""
    )
    @magic_arguments.argument(
        '--service-role',
        help="""[Optional] Service role to be used for executing the notebook. 
        Default value: EMR_Notebooks_DefaultRole"""
    )
    @magic_arguments.argument(
        '--timeout',
        default=3600,
        type=int,
        help="""Timeout for the execution to complete"""
    )
    @line_magic
    def execute_notebook(self, line):
        """
        Execute a EMR Studio Notebook non-interactively.
        Followings permissions are required on EMR-EC2 role to execute this magic
        `
           "elasticmapreduce:StartNotebookExecution",
           "elasticmapreduce:DescribeNotebookExecution",
           "ec2:DescribeInstances",
           "iam:PassRole"
        `
        """

        args = magic_arguments.parse_argstring(self.execute_notebook, line)
        notebook = args.file
        emr_cluster_id = args.cluster_id
        emr_notebooks_service_role = args.service_role
        workspace_id = os.environ["KERNEL_WORKSPACE_ID"]
        timeout = args.timeout

        if emr_cluster_id is None:
            emr_cluster_id = self.get_cluster_id()
        if emr_notebooks_service_role is None:
            emr_notebooks_service_role = "EMR_Notebooks_DefaultRole"

        display_html("Going to execute the Notebook {} using the service role {} and the cluster {}".format(notebook, emr_notebooks_service_role, emr_cluster_id))
        start_notebook_resp = self.emr.start_notebook_execution(
            EditorId=workspace_id,
            RelativePath=notebook,
            ExecutionEngine={'Id': emr_cluster_id},
            ServiceRole=emr_notebooks_service_role
        )

        notebook_execution_id = start_notebook_resp["NotebookExecutionId"]
        display_html("Started Notebook execution with id: {}. Waiting for execution to finish...".format(notebook_execution_id))

        # Polling for execution to finish
        start = time.time()

        is_nb_output_link_shown = False

        # Note: Timeout will NOT be precise as there is 10s sleep inside the loop.
        while (time.time() - start) < timeout:
            describe_response = self.emr.describe_notebook_execution(NotebookExecutionId=notebook_execution_id)
            execution_status = describe_response["NotebookExecution"]["Status"]
            if not is_nb_output_link_shown and "OutputNotebookURI" in describe_response["NotebookExecution"] and execution_status not in EXECUTIONS_STARTING_STATUS:
                workspace_relative_path = self.get_output_nb_workspace(describe_response["NotebookExecution"]["OutputNotebookURI"])
                if workspace_relative_path is not None:
                    text = """Output of the cells that have finished execution are captured in a new notebook file <a href="{}">{}</a>.""".format(
                        workspace_relative_path, workspace_relative_path)
                    display_html(text)
                    is_nb_output_link_shown = True

            if execution_status in EXECUTIONS_TERMINAL_STATUS:
                display_html("Execution completed. Status: {}".format(execution_status))
                break

            time.sleep(10)

    def get_output_nb_workspace(self, output_notebook_uri):
        workspace_s3_prefix = os.environ["KERNEL_WORKSPACE_DIR_S3_PREFIX"]
        if output_notebook_uri.startswith(workspace_s3_prefix):
            return output_notebook_uri[len(workspace_s3_prefix):]
        return None

    def get_cluster_id(self):
        ec2_instance_id = self.imdsv2.ec2_instance_id()
        desc_ec2_instance_resp = self.ec2.describe_instances(InstanceIds=[ec2_instance_id])
        for tag in desc_ec2_instance_resp['Reservations'][0]['Instances'][0]['Tags']:
            if tag['Key'] == 'aws:elasticmapreduce:job-flow-id':
                return tag['Value']

        raise UsageError("Unable to determine cluster id. Please use --cluster-id parameter")