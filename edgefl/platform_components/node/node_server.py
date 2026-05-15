"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import argparse
import os
import threading
import time
import warnings

from dotenv import load_dotenv
from fastapi import HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from platform_components.base_fl_server import BaseFLServer
from platform_components.EdgeLake_functions.blockchain_EL_functions import (
    get_policies,
)
from platform_components.node.node import Node


class NodeServer(BaseFLServer):
    def __init__(self, port: int, **fastapi_kwargs):
        super().__init__(srv_port=port, logger_name="node_server", **fastapi_kwargs)
        self._node: Node | None = None

    def register_routes(self):

        class InitNodeRequest(BaseModel):
            replica_name: str
            replica_ip: str
            replica_port: str
            replica_index: str
            round_number: int
            is_aggregator: bool = False
            min_params: int = 1

        class DFLTrainingRequest(BaseModel):
            input: list
            index: str

        # @self.init_router.post("/node", response_class=JSONResponse)
        @self.post("/init-node", response_class=JSONResponse)
        def init_node(request: InitNodeRequest):
            try:
                index = request.replica_index
                min_params = max(request.min_params, self.env_min)

                if self._node is None:
                    self.participant = self._node = Node(
                        request.replica_name, self.ip, request.replica_port, self.logger
                    )

                node = self._node
                node.databases.setdefault(index, self.db_name)
                node.initialize_specific_node_on_index(
                    index, self.module_name, self.module_file
                )
                node.round_number[index] = request.round_number

                # Set DFL config
                self.is_aggregator = request.is_aggregator or (
                    self.env_mode == "decentralized"
                )
                node.is_aggregator[index] = self.is_aggregator
                node.minParams[index] = min_params
                mode = (
                    "decentralized (DFL)" if self.is_aggregator else "centralized (CFL)"
                )
                self.logger.info(
                    f"{request.replica_name} successfully initialized for ({index}) in {mode} mode"
                    + (f" with minParams={min_params}" if self.is_aggregator else "")
                )

                # Start event listener for start round
                thread = threading.Thread(
                    name=f"{request.replica_name}--{index}",
                    target=self._listen_for_start_round,
                    args=(index,),
                    daemon=True,
                )
                thread.start()

                return {
                    "status": "success",
                    "message": f"Node initialized in {mode} mode",
                }
            except ConnectionError as e:
                raise ConnectionError(f"Unable to access the database tables: {str(e)}")
            except Exception as e:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"/init/node - {str(e)}",
                )

        # @self.training_router.post("/dfl", response_class=JSONResponse)
        def start_dfl(request: DFLTrainingRequest):
            pass

    def _listen_for_start_round(self, index: str):
        if (node := self._node) is None:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Node not initialized",
            )

        current_round = node.round_number[index]
        is_dfl = node.is_aggregator.get(index, False)

        self.logger.info(
            f"[{index}][Round {current_round}] Listening for start round {current_round}"
            + (" (DFL mode)" if is_dfl else "")
        )

        while True:
            try:
                end_round = node.end_round.get(index)
                if end_round is not None and current_round > end_round:
                    self.logger.info(
                        f"[{index}] Reached end round {end_round}. Stopping."
                    )
                    return

                # DFL nodes listen for both aggregator and dfl_aggregator RoundStart policies
                if is_dfl:
                    cond = (
                        f"where round_number = {current_round} "
                        f"and policy_type = RoundStart"
                    )
                else:
                    cond = (
                        f"where round_number = {current_round} "
                        f"and node_type = aggregator"
                    )

                if response := get_policies(self.el_url, index=index, condition=cond):
                    if round_data := response[0]:
                        self.logger.debug(f"[{index}] Round Data: {round_data}")
                        params_link = round_data.get("initParams", "")
                        ip_port = round_data.get("ip_port", "")
                        rest_ip_port = round_data.get("rest_ip_port", "")
                        modelUpdate_metadata = node.train_model_params(
                            params_link, current_round, ip_port, rest_ip_port, index
                        )
                        node.add_node_params(current_round, modelUpdate_metadata, index)
                        self.logger.info(
                            f"[{index}][Round {current_round}] Step 3 Complete: Model parameters published"
                        )

                        # DFL: after training and publishing, aggregate from peers
                        if is_dfl:
                            self.logger.info(
                                f"[{index}][Round {current_round}] DFL: Starting peer aggregation"
                            )
                            self._dfl_thread(current_round, index)
                            # agg_thread = threading.Thread(
                            #    name=f"{node.replica_name}--{index}--dfl-agg-r{current_round}",
                            #    target=self._dfl_thread,
                            #    args=(index, current_round),
                            #    daemon=True,
                            # )
                            # agg_thread.start()

                        current_round += 1
                        self.logger.info(
                            f"[{index}][Round {current_round}] Listening for start round {current_round}"
                        )
                        continue

                time.sleep(5)

            except Exception as e:
                self.logger.error(f"[{index}] Error in listener thread: {str(e)}")
                time.sleep(2)

    def _dfl_thread(self, round_number: int, index: str):
        """
        DFL aggregation: poll for peer submodels, aggregate when minParams met,
        update local model, and publish RoundStart for the next round.
        """
        if (node := self._node) is None:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Node not initialized",
            )

        min_params = node.minParams.get(index, 1)
        self.logger.info(
            f"[{index}][Round {round_number}] DFL: Waiting for {min_params} peer submodels"
        )

        agg_path = self.poll_and_aggregate(
            index, round_number, min_params, starve_timer=60
        )

        # Publish RoundStart so peers pick up the next round
        next_round = round_number + 1
        end_round = node.end_round.get(index)
        if end_round is not None and next_round > end_round:
            self.logger.info(
                f"[{index}][Round {round_number}] DFL: Reached end round. Not publishing RoundStart."
            )
            return
        node.start_round(agg_path, next_round, index, node_type="dfl_aggregator")
        self.logger.info(
            f"[{index}][Round {round_number}] DFL: published RoundStart for round {next_round}"
        )


load_dotenv()
warnings.filterwarnings("ignore")
_port = int(os.getenv("SERVER_PORT", 8080))
app = NodeServer(port=_port)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Node Server.")
    parser.add_argument(
        "--port", type=int, default=None, help="Port to run the server on."
    )
    args = parser.parse_args()
    if not (
        _port := args.port
        if args.port is not None
        else int(os.getenv("SERVER_PORT", 0))
    ):
        raise ValueError("Missing environment variable SERVER_PORT or argument --port")
    app = NodeServer(port=_port)
    app.run()
