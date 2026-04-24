"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import os
from asyncio import sleep
from threading import Lock

from dotenv import load_dotenv

from platform_components.EdgeLake_functions.blockchain_EL_functions import insert_policy, \
    check_policy_inserted, get_policies
from platform_components.base_fl_participant import BaseFLParticipant

load_dotenv()


class Aggregator(BaseFLParticipant):
    def __init__(self, ip, port, logger):
        agg_name = os.getenv("AGG_NAME")
        super().__init__(agg_name, logger)

        self.agg_name = agg_name
        self.server_ip = ip
        self.server_port = port

        self.logger.debug("Aggregator initializing")

        # ===== Aggregator-specific state
        self.node_urls = {}
        self.node_count = {}
        self.lock = Lock()
        self.minParams = {}
        self.end_round = {}
        # =====

    def initialize_index_on_blockchain(self, index, module_name, module_path, db_name):
        if self.get_index_data_in_blockchain(index):
            return {
                'status': 'error',
                'message': 'index already initialized on the blockchain'
            }

        try:
            data = f'''<my_policy = {{"{index}" : {{
                                        "policy_type": "init",
                                        "name": "{index}",
                                        "module_name": "{module_name}",
                                        "module_path": "{module_path}",
                                        "ip_port": "{self.edgelake_tcp_node_ip_port}",
                                        "rest_ip_port": "{self.edgelake_node_url}",
                                        "db_name": "{db_name}"
            }} }}>'''
            success = False
            while not success:
                response = insert_policy(self.edgelake_node_url, data)
                if response.status_code == 200:
                    success = True
                else:
                    sleep(3)

                    if check_policy_inserted(self.edgelake_node_url, data):
                        success = True

            if success:
                return {
                    'status': 'success',
                    'message': 'index initialized onto the blockchain'
                }
            else:
                return {
                    'status': 'error',
                    'message': f'Request failed with status code: {response.status_code}'
                }
        except Exception as e:
            return {
                'status': 'error',
                'message': str(e)
            }

    def store_most_recent_agg_params(self, initParams_link, index, round_number):
        try:
            data = f'''<my_policy = {{"{index}" : {{
                                                    "index" : "{index}",
                                                    "policy_type" : "RoundStart",
                                                    "node_type": "aggregator",
                                                    "round_number": {round_number},
                                                    "initParams": "{initParams_link}",
                                                    "node_id": "{self.name}",
                                                    "ip_port": "{self.edgelake_tcp_node_ip_port}",
                                                    "rest_ip_port": "{self.edgelake_node_url}"
                                          }} }}>'''
            insert_success = False
            while not insert_success:
                response = insert_policy(self.edgelake_node_url, data)
                if response.status_code == 200:
                    insert_success = True
                else:
                    sleep(3)

                    if check_policy_inserted(self.edgelake_node_url, data):
                        insert_success = True

            if insert_success:
                return {
                    'status': 'success',
                    'message': f'Successfully updated most recent aggregated model file at policy {index}-r'
                }
            else:
                return {
                    'status': 'error',
                    'message': f'Request failed with status code: {response.status_code}'
                }
        except Exception as e:
            return {
                'status': 'error',
                'message': str(e)
            }
