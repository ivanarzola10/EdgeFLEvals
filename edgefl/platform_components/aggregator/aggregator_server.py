"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import argparse
import asyncio
import logging
import os
import threading
import time
import warnings

import requests
from dotenv import load_dotenv
from fastapi import HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from platform_components.aggregator.aggregator import Aggregator
from platform_components.base_fl_server import BaseFLServer
from platform_components.EdgeLake_functions.blockchain_EL_functions import (
    get_latest_policy_value,
    get_policies,
)
from platform_components.lib.modules.exceptions import NodeInitializationError


class AggregatorServer(BaseFLServer):
    def __init__(self, port: int, **fastapi_kwargs):
        super().__init__(
            srv_port=port, logger_name="aggregator_server", **fastapi_kwargs
        )
        self.logger.setLevel(logging.INFO)  # Excludes WARNING, ERROR, CRITICAL
        self.participant = self.aggregator = Aggregator(self.ip, str(port), self.logger)

    def register_routes(self):

        class InitRequest(BaseModel):
            nodeUrls: list[str]
            index: str
            is_aggregator: bool = (
                False  # per-node DFL override (applies to all nodes in this request)
            )
            min_params: int = 1  # min submodels before DFL nodes aggregate

        class TrainingRequest(BaseModel):
            totalRounds: int
            minParams: int
            index: str

        class ContinueTrainingRequest(BaseModel):
            additionalRounds: int
            minParams: int
            index: str

        class UpdateMinParamsRequest(BaseModel):
            updatedMinParams: int
            index: str

        # @self.init_router.post("/aggregator", response_class=JSONResponse)
        @self.post("/init", response_class=JSONResponse)
        def init_agg(request: InitRequest):
            """Deploy the smart contract with predefined nodes."""
            try:
                # Initialize the nodes on specified index and send the contract address
                agg = self.aggregator
                index = request.index

                git_dir = os.getenv("GITHUB_DIR")
                training_dir = os.getenv("TRAINING_APPLICATION_DIR")
                module_name = os.getenv("MODULE_NAME")
                module_file = os.getenv("MODULE_FILE")
                db_name = os.getenv("LOGICAL_DATABASE", "")

                # Verify filepath exists
                if not all((git_dir, training_dir, module_name, module_file, db_name)):
                    self.logger.error("Server missing required environment variables")
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail="Server missing required env vars",
                    )

                # Verify module exists
                assert git_dir and training_dir and module_file
                module_path = os.path.join(git_dir, training_dir, module_file)
                if not os.path.exists(module_path):
                    raise FileNotFoundError(
                        f"Module '{module_file}' not found at '{module_path}'"
                    )

                # Set up index and specific data
                agg.indexes.add(index)
                agg.databases.setdefault(index, db_name)
                agg.round_number.setdefault(index, 1)

                self._initialize_nodes(
                    request.nodeUrls, index, request.is_aggregator, request.min_params
                )

                agg.set_module_at_index(index, module_name, module_file)
                agg.initialize_index_on_blockchain(
                    index, module_name, module_path, db_name
                )
                agg.initialize_training_app_on_index(index)
                agg.initialize_file_write_paths_on_index(index)

                initialized = [
                    url for url in request.nodeUrls if url in agg.node_urls[index]
                ]
                failed = [
                    url for url in request.nodeUrls if url not in agg.node_urls[index]
                ]

                self.logger.info(
                    f"Initialized nodes with index ({index}): {agg.node_urls[index]}"
                )

                return {
                    "status": "success",
                    "message": "Initialization request finished.",
                    "initialized nodes": f"{initialized}",
                    "failed nodes": f"{failed}",
                }
            except FileNotFoundError as e:
                self.logger.error(f"{str(e)}")
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND, detail={str(e)}
                )
            except Exception as e:
                self.logger.error(
                    f"Failed to initialize nodes with index ({request.index}): {str(e)}"
                )
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
                )

        @self.post("/start-training", response_class=JSONResponse)
        async def start_training(request: TrainingRequest):
            """Start the training process by setting the number of rounds."""

            agg = self.aggregator
            index = request.index
            self._require_index(index)

            # Prevents stalling when minParams > # of active nodes; warns user
            agg.minParams[index] = min(request.minParams, agg.node_count[index])
            if request.minParams > agg.node_count[index]:
                self.logger.info(
                    f"[{index}] minParams ({request.minParams}) is greater than number of active nodes ({agg.node_count[index]}). Using active nodes as minParams."
                )
            if request.totalRounds <= 0:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Number of rounds (totalRounds) must be positive",
                )

            # TODO: if a training process is in-progress, do not allow another call to /start-training
            # TODO: add a manual way to stop training (if needed)

            starting_round = 1
            initial_params = ""
            self.logger.info(
                f"[{index}] {request.totalRounds} {'round' if request.totalRounds == 1 else 'rounds'} of training started."
            )
            try:
                thread = threading.Thread(
                    name=f"agg/train--{index}",
                    target=self._training_loop,
                    args=(initial_params, starting_round, request.totalRounds, index),
                    daemon=True,
                )
                thread.start()
            except Exception as e:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"An error occurred during training: {str(e)}",
                )

            return {
                "status": "success",
                "message": f"Started training at index: {index}",
            }

        @self.post("/continue-training", response_class=JSONResponse)
        async def continue_training(request: ContinueTrainingRequest):
            agg = self.aggregator
            index = request.index
            self._require_index(index)
            try:
                # Prevents stalling when minParams > # of active nodes; warns user
                agg.minParams[index] = min(request.minParams, agg.node_count[index])
                if request.minParams > agg.node_count[index]:
                    self.logger.info(
                        f"[{index}] minParams ({request.minParams}) is greater than number of active nodes ({agg.node_count[index]}). Using active nodes as minParams."
                    )
                if request.additionalRounds <= 0:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="additionalRounds must be positive",
                    )

                # If mid training, we only have to update the end_round value
                if agg.round_number[index] < agg.end_round.get(index, 0):
                    agg.end_round[index] += request.additionalRounds
                    return {
                        "status": "success",
                        "message": f"Extended training at index {index} to round {agg.end_round[index]}: current round is {agg.round_number[index]}",
                    }

                # Get the last round number from the blockchain
                if (
                    last_round := get_latest_policy_value(
                        self.el_url,
                        index,
                        "where node_type = aggregator",
                        "round_number",
                    )
                ) is None:
                    self.logger.error(f"[{index}] Error fetching last round")
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"[{index}] No previous training found",
                    )
                # assert last_round
                last_round = int(last_round)

                if not (
                    initial_params := get_latest_policy_value(
                        self.el_url, index, "where node_type = aggregator", "initParams"
                    )
                ):
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail=f"[{index}] Failed to fetch aggregated parameters from round {last_round}",
                    )

                # TODO: if a training process is in-progress, do not allow another call to /continue-training
                # TODO: add a manual way to stop training (if needed)

                starting_round = last_round + 1
                end_round = last_round + request.additionalRounds
                self.logger.info(
                    f"[{index}] Continuing training from round {last_round}, adding {request.additionalRounds} more {'round' if request.additionalRounds == 1 else 'rounds'}."
                )
                thread = threading.Thread(
                    name=f"agg/continue-training--{index}",
                    target=self._training_loop,
                    args=(initial_params, starting_round, end_round, index),
                    daemon=True,
                )
                thread.start()
                return {
                    "status": "success",
                    "message": f"Continuing from round {starting_round} to {end_round}: {index}",
                }
            except Exception as e:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
                )

        @self.post("/update-minParams", response_class=JSONResponse)
        async def update_min_params(request: UpdateMinParamsRequest):
            agg = self.aggregator
            index = request.index
            self._require_index(index)
            # TODO: Rare bug, when training two different models and both are in-progress, one of them may stop when this endpoint is called...or if a node is added mid-way...not sure
            try:
                if not get_policies(
                    self.el_url, index=index, condition=f"where name = {index}"
                ):
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"Index {index} not found in the blockchain.",
                    )

                agg.minParams[index] = min(
                    request.updatedMinParams, agg.node_count[index]
                )
                if request.updatedMinParams > agg.node_count[index]:
                    self.logger.info(
                        f"[{index}] minParams ({request.updatedMinParams}) is greater than number of active nodes ({agg.node_count[index]}). Using active nodes as minParams."
                    )
                return {
                    "status": "success",
                    "message": f"minParams set to {agg.minParams[index]}",
                }
            except Exception as e:
                raise HTTPException(
                    status_code=(status.HTTP_500_INTERNAL_SERVER_ERROR),
                    detail=(f"Unable to set minParams at index {index}; {e}",),
                )

    def _initialize_nodes(
        self, node_urls: list[str], index: str, is_aggregator: bool, min_params: int
    ):
        """
        POST the deployed contract address to each node
        URL's /init/node endpoint in parallel threads.
        """
        agg = self.aggregator
        index = index
        agg.node_count.setdefault(index, 0)
        agg.node_urls.setdefault(index, set())

        def init_node(node_url: str):
            try:
                ip_port = node_url.split("/")[-1].split(":")
                self.logger.info(f"Initializing model at {node_url}")

                # Check that node is online
                # If it's not, then remove it from node_urls
                if not self._is_node_online(node_url):
                    with agg.lock:
                        if node_url in agg.node_urls[index]:
                            agg.node_urls[index].remove(node_url)
                            agg.node_count[index] -= 1
                    self.logger.warning(
                        f"Node {node_url} is offline; skipping initialization."
                    )
                    return

                with agg.lock:
                    if node_url in agg.node_urls[index]:
                        self.logger.info(
                            f"Model at {node_url} already exists for index {index}."
                        )
                        return  # Already initialized
                    # Reserve a replica number
                    replica_number = agg.node_count[index] + 1
                    replica_name = f"node{replica_number}"
                    agg.node_count[index] = replica_number

                resp = requests.post(
                    f"{node_url}/init/node",
                    json={
                        "replica_ip": ip_port[0],
                        "replica_port": ip_port[1],
                        "replica_name": replica_name,
                        "replica_index": index,
                        "round_number": agg.round_number[index],
                        "is_aggregator": is_aggregator,
                        "min_params": min_params,
                    },
                )

                with agg.lock:
                    if resp.status_code == 200:
                        agg.node_urls[index].add(node_url)
                        self.logger.info(
                            f"Node at {node_url} initialized successfully."
                        )
                    else:
                        # Rollback node count if request fails
                        agg.node_count[index] -= 1
                        raise NodeInitializationError(
                            status_code=resp.status_code,
                            detail=f"Failed to init node at {node_url}",
                        )
            except NodeInitializationError as e:
                self.logger.critical(str(e))
                raise e
            except Exception as e:
                with agg.lock:
                    agg.node_count[index] -= 1  # Rollback on exception
                self.logger.critical(str(e))
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
                )

        """
        HTTPException in a thread — FastAPI won't catch it since it's not in a request handler context.
        It'll just kill the thread silently. Log the error and move on instead, or collect failures and
        report them in the response (like the project file does with initialized / failed lists).
        """

        # TODO: if a node gets re-init'ed because the node server re-opened, shut down the corresponding thread and start again
        threads = []
        for url in node_urls:
            t = threading.Thread(
                name=f"agg/init--{url}", target=init_node, args=(url,), daemon=True
            )
            t.start()
            threads.append(t)
            time.sleep(0.1)

        for i, t in enumerate(threads):
            t.join(timeout=180)
            if t.is_alive():
                self.logger.warning(
                    f"Node {i} thread timed out. Failed to initialize a node."
                )

    def _is_node_online(self, node_url: str) -> bool:
        try:
            requests.get(node_url, timeout=2)
            return True
        except requests.exceptions.RequestException:
            return False

    def _training_loop(
        self, initial_params: str, starting_round: int, end_round: int, index: str
    ):
        agg = self.aggregator
        agg.end_round[index] = end_round

        r = starting_round
        while r <= agg.end_round[index]:
            agg.round_number[index] = r
            self.logger.info(f"[{index}] Starting training round {r}")
            agg.start_round(initial_params, r, index)
            self.logger.debug(f"[{index}] Sent initial parameters to nodes")

            initial_params = self.poll_and_aggregate(
                index, r, agg.minParams[index], starve_timer=0
            )
            self.logger.info(
                f"[{index}][Round {r}] Step 4 Complete: model parameters aggregated"
            )
            r += 1

        self.logger.info(f"[{index}] Training completed successfully")
        return {"status": "success", "message": "Training completed successfully"}

    def _get_fallback_params(self, index: str) -> str | None:
        """
        Overrides BaseFLServer._get_fallback_params().
        Aggregator can fall back to the last known agg params on the blockchain.
        """
        try:
            return get_latest_policy_value(self.el_url, index, "", "initParams")
        except Exception:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to fetch aggregated params",
            )


load_dotenv()
warnings.filterwarnings("ignore")

# Track the training process of each index so that they can join once they're done
# training_processes = {}

_port = int(os.getenv("SERVER_PORT", 8080))
app = AggregatorServer(port=_port)

if __name__ == "__main__":
    # Add argument parsing to make the port configurable
    parser = argparse.ArgumentParser(description="Run the Aggregator Server.")
    parser.add_argument(
        "--port", type=int, default=8080, help="Port to run the server on."
    )
    args = parser.parse_args()
    if not (
        _port := args.port
        if args.port is not None
        else int(os.getenv("SERVER_PORT", 0))
    ):
        raise ValueError("Missing environment variable SERVER_PORT or argument --port")
    app = AggregatorServer(port=_port)
    app.run()
