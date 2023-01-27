import os
import boto3
import time

from IPython.core import magic_arguments
from IPython.core.magic import (Magics, magics_class, line_magic)
from IPython.core.error import UsageError
from .utils.instance_metadata_service_utils import IMDSv2Util


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

        print("Going to execute the Notebook {} using the service role {} and the cluster {}".format(notebook, emr_notebooks_service_role, emr_cluster_id))
        start_notebook_resp = self.emr.start_notebook_execution(
            EditorId=workspace_id,
            RelativePath=notebook,
            ExecutionEngine={'Id': emr_cluster_id},
            ServiceRole=emr_notebooks_service_role
        )

        notebook_execution_id = start_notebook_resp["NotebookExecutionId"]
        print("Started Notebook execution with id: {}".format(notebook_execution_id))
        print("Waiting for execution to finish..")

        # Polling for execution to finish
        start = time.time()

        # Note: Timeout will NOT be precise as there is 15s sleep inside the loop.
        while (time.time() - start) < timeout:
            describe_response = self.emr.describe_notebook_execution(NotebookExecutionId=notebook_execution_id)
            if describe_response["NotebookExecution"]["Status"] == "FINISHED" or describe_response["NotebookExecution"]["Status"] == "FAILED":
                print("Execution completed. Status: ", describe_response["NotebookExecution"]["Status"])
                break
            time.sleep(15)

    def get_cluster_id(self):
        ec2_instance_id = self.imdsv2.ec2_instance_id()
        desc_ec2_instance_resp = self.ec2.describe_instances(InstanceIds=[ec2_instance_id])
        for tag in desc_ec2_instance_resp['Reservations'][0]['Instances'][0]['Tags']:
            if tag['Key'] == 'aws:elasticmapreduce:job-flow-id':
                return tag['Value']

        raise UsageError("Unable to determine cluster id. Please use --cluster-id parameter")