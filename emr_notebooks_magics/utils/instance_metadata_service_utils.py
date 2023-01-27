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

import requests

IMDSv2_TOKEN_TTL_HEADER = "X-aws-ec2-metadata-token-ttl-seconds"
IMDSv2_TOKEN_HEADER = "X-aws-ec2-metadata-token"


class IMDSv2Util:
    def __get_imdsv2_token(self):
        return requests.put("http://169.254.169.254/latest/api/token",
                            headers={IMDSv2_TOKEN_TTL_HEADER: "21600"}).text

    def get_region(self):
        token = self.__get_imdsv2_token()
        return requests.get("http://169.254.169.254/latest/dynamic/instance-identity/document",
                            headers={IMDSv2_TOKEN_HEADER: token}).json()['region']

    def ec2_instance_id(self):
        token = self.__get_imdsv2_token()
        return requests.get("http://169.254.169.254/latest/meta-data/instance-id",
                            headers={IMDSv2_TOKEN_HEADER: token}).text
