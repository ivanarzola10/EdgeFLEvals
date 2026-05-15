"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""

import os
import pickle
import time
from abc import abstractmethod
from contextlib import asynccontextmanager

import uvicorn
from fastapi import APIRouter, FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from platform_components.base_fl_participant import BaseFLParticipant
from platform_components.EdgeLake_functions.blockchain_EL_functions import (
    get_local_ip,
    get_policies,
)
from platform_components.lib.logger.error_handling import get_logger
from platform_components.lib.logger.logger_config import configure_logging


class BaseFLServer(FastAPI):
    def __init__(self, srv_port: int, logger_name: str = "", **fastapi_kwargs):
        super().__init__(lifespan=self._lifespan, **fastapi_kwargs)
        self.participant: BaseFLParticipant
        self.is_aggregator: bool
        self.ip = get_local_ip()
        if srv_port:
            self.srv_port = srv_port
        else:
            self.srv_port = int(os.getenv("SERVER_PORT", "8080"))
        self.env_mode = os.getenv("AGGREGATION_MODE", "centralized").lower()
        self.env_min = int(os.getenv("MIN_PARAMS", "1"))
        try:
            self.el_url = f"http://{os.getenv('EXTERNAL_IP')}"
            # self.el_tcp_ip_port = os.getenv("EXTERNAL_TCP_IP_PORT", "")
            self.module_name = os.getenv("MODULE_NAME")
            self.module_file = os.getenv("MODULE_FILE")
            self.db_name = os.getenv("LOGICAL_DATABASE")
        except ValueError as e:
            raise ValueError(f"Missing environment variable.\t{str(e)}")
        configure_logging(f"{logger_name}_{self.el_url.split(':')[2]}")
        self.logger = get_logger(__name__)
        self._setup_middleware()

        # e.g. http://.../init/aggregator; http://.../init/node
        # self.init_router = APIRouter(prefix="/init")
        # e.g. http://.../start-training; http://.../start-training/{node.replica_name}
        # self.training_router = APIRouter(prefix="/start-training")

        self._register_common_routes()
        self.register_routes()
        # self.include_router(self.init_router)
        # self.include_router(self.training_router)

    @asynccontextmanager
    async def _lifespan(self, _):
        yield

    def _setup_middleware(self):
        self.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],  # or specify your frontend origin
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    def _register_common_routes(self):
        """
        Register shared URL endpoints/ prefixes
        """

        @self.post("/inference/{index}", response_class=JSONResponse)
        async def inference(index: str):
            self._require_participant()
            self._require_index(index)
            try:
                self.logger.info(f"[{index}] received inference request")
                results = self.participant.inference(index)
                return {
                    "index": index,
                    "status": "success",
                    "message": "Inference completed successfully",
                    "model_accuracy": str(results),
                }
            except Exception as e:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=str(e),
                )

        class InferenceRequest(BaseModel):
            input: list  # each element in here is one data value to test
            labels: list | None = None  # check element type within direct_inference
            index: str | None = None  # test run index

        @self.post("/infer", response_class=JSONResponse)
        @self.post("/direct-inference/{index}", response_class=JSONResponse)
        async def direct_inference(index: str, request: InferenceRequest):
            if not (resolved_index := request.index or index):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Index required (path param or request body)",
                )
            self._require_participant()
            self._require_index(resolved_index)
            try:
                results = self.participant.direct_inference(
                    resolved_index,
                    request.input,
                    request.labels,
                )
                return {
                    "index": index,
                    "status": "success",
                    "prediction": str(results),
                }
            except ValueError as e:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)
                )
            except Exception as e:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Inference failed: {str(e)}",
                )

    @abstractmethod
    def register_routes(self): ...

    def poll_and_aggregate(
        self,
        index: str,
        round_number: int,
        min_params: int,
        *,
        starve_timer: float = 60.0,
        poll_interval: float = 2.0,
    ) -> str:
        """
        Block until >= min_params submodel policies arrive for the given
        (index, round_number), aggregate them, update the local model,
        and return the file path of the aggregated weights.
        -----
        If starve_timer <= 0, wait forever until min_params number of
        submodels can be aggregated. (Can lead to node starvation!)
        """

        self._require_participant()
        self._require_index(index)
        decoded_params: dict = {}
        last_progress_time = time.time()
        # self.logger.info(f"[{index}][Round {round_number}] Polling for {min_params} submodel(s)...")

        while True:
            try:
                prev_count = len(decoded_params)

                # fetch policies of type "submodel" (for FL) from the blockchain
                result = get_policies(
                    self.el_url,
                    index=index,
                    condition=f"where policy_type=submodel and node_type=training and round_number={round_number}",
                )
                if result:
                    links = [item["trained_params_local_path"] for item in result]
                    ip_ports = [item["ip_port"] for item in result]
                    rest_ip_ports = [item["rest_ip_port"] for item in result]

                    # TODO: participant.fetch_decoded_params() silences submodel download failures,
                    # causing repeated get_policies() calls with the expected >= min_params
                    # number of submodel policies in each callw.

                    self.participant.fetch_decoded_params(
                        decoded_params_dict=decoded_params,
                        node_param_download_links=links,
                        ip_ports=ip_ports,
                        rest_ip_ports=rest_ip_ports,
                        index=index,
                    )

                if len(decoded_params) > prev_count:
                    last_progress_time = time.time()
                # self.logger.info(f"[{index}] Found submodel(s): {decoded_params}")
                # self.logger.info(f"[{index}] Found {len(decoded_params)} submodel(s)")

                if len(decoded_params) >= min_params:
                    return self._do_aggregation(decoded_params, index, round_number)

                # Wait forever until min_params number of submodels can be aggregated
                if starve_timer <= 0:
                    time.sleep(poll_interval)
                    continue

                elapsed = time.time() - last_progress_time
                if decoded_params and starve_timer > 0 and elapsed >= starve_timer:
                    self.logger.warning(
                        f"[{index}][Round {round_number}] Starvation timer expired; "
                        f"({starve_timer}s since last new model). "
                        f"Aggregating {len(decoded_params)}/{min_params} submodel(s)"
                    )
                    return self._do_aggregation(decoded_params, index, round_number)

                if not decoded_params and starve_timer > 0 and elapsed >= starve_timer:
                    fallback = self._get_fallback_params(index)
                    if fallback:
                        return fallback
                    last_progress_time = time.time()

            except Exception as e:
                self.logger.error(
                    f"[{index}][Round {round_number}] aggregation error: {e}"
                )
            time.sleep(poll_interval)

    def _do_aggregation(self, decoded_params, index, round_number):
        aggregated_params_link = self.participant.aggregate_model_params(
            decoded_params=list(decoded_params.values()),
            round_number=round_number,
            index=index,
        )
        self.logger.info(
            f"[{index}][Round {round_number}] Aggregated {len(decoded_params)} submodel(s)"
        )

        # Update local model with aggregated weights
        local_path = (
            f"{self.participant.file_write_destination}/{index}/"
            f"{round_number}-{self.participant.name}_update.json"
        )
        with open(local_path, "rb") as f:
            data = pickle.load(f)

        if data and "newUpdates" in data:
            weights = self.participant.decode_params(data["newUpdates"])
        else:
            self.logger.error(f"[{index}] Invalid aggregated data")
            raise ValueError(
                f"[{index}] Invalid or missing 'newUpdates' in aggregated file: {local_path}"
            )

        self.participant.data_handlers[index].update_model(weights)
        return aggregated_params_link

    def _get_fallback_params(self, index: str) -> str | None:
        """
        Override in the aggregator server to get the last aggregated min_params
        """
        return None

    def _require_participant(self):
        if self.participant is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Participant (aggregator/ node) has not been initialized on the server.",
            )
        assert self.participant

    def _require_index(self, index: str):
        self._require_participant()
        if index not in self.participant.indexes:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Index {index} not found (not yet initialized).",
            )
        assert index

    def run(self, host: str = "0.0.0.0", **srv_kwargs):
        uvicorn.run(self, host=host, port=self.srv_port, reload=False, **srv_kwargs)
