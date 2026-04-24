"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""
from fastapi.responses import JSONResponse
from fastapi.responses import PlainTextResponse

from platform_components.EdgeLake_functions.blockchain_EL_functions import get_local_ip, \
    connect_to_db, get_all_databases
from platform_components.node.node import Node
import asyncio
import logging
import pickle
import threading
import time
from dotenv import load_dotenv
import os
import argparse
import requests
import warnings

from uvicorn import run
from fastapi import FastAPI, HTTPException, status

from fastapi.middleware.cors import CORSMiddleware

from contextlib import asynccontextmanager
from pydantic import BaseModel

from platform_components.lib.logger.logger_config import configure_logging


warnings.filterwarnings("ignore")

load_dotenv()

edgelake_node_url = f'http://{os.getenv("EXTERNAL_IP")}'
edgelake_node_port = edgelake_node_url.split(":")[2]

configure_logging(f"node_server_{edgelake_node_port}")

logger = logging.getLogger(__name__)

# Initialize the Node instance
node_instance = None
listener_thread = None
stop_listening_thread = False

@asynccontextmanager
async def lifespan(app: FastAPI):
    yield

app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class InitNodeRequest(BaseModel):
    replica_name: str
    replica_ip: str
    replica_port: str
    replica_index: str
    round_number: int
    is_aggregator: bool = False
    min_params: int = 1


@app.post('/init-node')
def init_node(request: InitNodeRequest):
    global node_instance, listener_thread, stop_listening_thread
    try:
        ip = get_local_ip()
        most_recent_round = request.round_number

        port = request.replica_port
        replica_name = request.replica_name
        index = request.replica_index

        module_name = os.getenv("MODULE_NAME")
        module_file = os.getenv("MODULE_FILE")

        db_name = os.getenv("LOGICAL_DATABASE")

        # Read aggregation mode from env, allow override from init request
        env_aggregation_mode = os.getenv("AGGREGATION_MODE", "centralized").lower()
        is_aggregator = request.is_aggregator or (env_aggregation_mode == "decentralized")

        # Read min_params from env as default, allow override from init request
        env_min_params = int(os.getenv("MIN_PARAMS", "1"))
        min_params = request.min_params if request.min_params > 1 else env_min_params

        # Instantiate the Node class
        logger.info(f"{replica_name} before initialized")
        if not node_instance:
            node_instance = Node(replica_name, ip, port, logger)

        if index not in node_instance.databases:
            node_instance.databases[index] = db_name

        node_instance.initialize_specific_node_on_index(index, module_name, module_file)
        node_instance.round_number[index] = most_recent_round

        # Set DFL config
        node_instance.is_aggregator[index] = is_aggregator
        node_instance.minParams[index] = min_params

        mode = "decentralized (DFL)" if is_aggregator else "centralized (CFL)"
        logger.info(f"{replica_name} successfully initialized for ({index}) in {mode} mode"
                     + (f" with minParams={min_params}" if is_aggregator else ""))

        # Start event listener for start round
        listener_thread = threading.Thread(
            name=f"{replica_name}--{index}",
            target=listen_for_start_round,
            args=(node_instance, index, lambda: stop_listening_thread)
        )
        listener_thread.daemon = True
        listener_thread.start()

        return {
            'status': 'success',
            'message': f'Node initialized successfully in {mode} mode'
        }
    except ValueError as e:
        raise ValueError(
            f"No data found in the database: {os.getenv('LOGICAL_DATABASE')}"
        )
    except HTTPException as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"/init-node - {str(e)}"
        )
    except ConnectionError as e:
        raise ConnectionError(
            f"Unable to access the database tables: {str(e)}"
        )


def listen_for_start_round(nodeInstance, index, stop_event):
    current_round = nodeInstance.round_number[index]
    is_dfl = nodeInstance.is_aggregator.get(index, False)

    logger.info(f"[{index}][Round {current_round}] Listening for start round {current_round}"
                + (" (DFL mode)" if is_dfl else ""))
    while True:
        try:
            # DFL nodes listen for both aggregator and dfl_aggregator RoundStart policies
            if is_dfl:
                headers = {
                    'User-Agent': 'AnyLog/1.23',
                    'command': f'blockchain get {index} where round_number = {current_round} and policy_type = RoundStart'
                }
            else:
                headers = {
                    'User-Agent': 'AnyLog/1.23',
                    'command': f'blockchain get {index} where round_number = {current_round} and node_type = aggregator'
                }
            response = requests.get(edgelake_node_url, headers=headers)

            if response.status_code == 200:
                data = response.json()
                if not data:
                    time.sleep(2)
                    continue
                round_data = data[0].get(index)

                if round_data:
                    logger.debug(f"[{index}] Round Data: {round_data}")
                    paramsLink = round_data.get('initParams', '')
                    ip_port = round_data.get('ip_port', '')
                    rest_ip_port = round_data.get('rest_ip_port', '')
                    modelUpdate_metadata = nodeInstance.train_model_params(paramsLink, current_round, ip_port, rest_ip_port, index)
                    nodeInstance.add_node_params(current_round, modelUpdate_metadata, index)
                    logger.info(f"[{index}][Round {current_round}] Step 3 Complete: Model parameters published")

                    # DFL: after training and publishing, aggregate from peers
                    if is_dfl:
                        logger.info(f"[{index}][Round {current_round}] DFL: Starting peer aggregation")
                        agg_thread = threading.Thread(
                            name=f"{nodeInstance.replica_name}--{index}--dfl-agg-r{current_round}",
                            target=dfl_aggregate_round,
                            args=(nodeInstance, current_round, index)
                        )
                        agg_thread.daemon = True
                        agg_thread.start()

                    current_round += 1
                    logger.info(f"[{index}][Round {current_round}] Listening for start round {current_round}")

            time.sleep(5)
        except Exception as e:
            logger.error(f"[{index}] Error in listener thread: {str(e)}")
            time.sleep(2)


def dfl_aggregate_round(nodeInstance, round_number, index):
    """
    DFL aggregation: poll for peer submodels, aggregate when minParams met,
    update local model, and publish RoundStart for the next round.
    """
    min_params = nodeInstance.minParams.get(index, 1)
    decoded_params = {}
    check_chances = 5

    logger.info(f"[{index}][Round {round_number}] DFL: Waiting for {min_params} peer submodels")

    while True:
        try:
            headers = {
                'User-Agent': 'AnyLog/1.23',
                'command': f'blockchain get {index} where round_number={round_number} and node_type=training'
            }
            response = requests.get(edgelake_node_url, headers=headers)
            response.raise_for_status()

            result = response.json()
            if result:
                node_params_links = [
                    item.get(index).get('trained_params_local_path')
                    for item in result
                    if index in item
                ]
                ip_ports = [
                    item.get(index).get('ip_port')
                    for item in result
                    if index in item
                ]
                rest_ip_ports = [
                    item.get(index).get('rest_ip_port')
                    for item in result
                    if index in item
                ]

                nodeInstance.fetch_decoded_params(
                    decoded_params_dict=decoded_params,
                    node_param_download_links=node_params_links,
                    ip_ports=ip_ports,
                    rest_ip_ports=rest_ip_ports,
                    index=index
                )

            if len(decoded_params) >= min_params or (decoded_params and not check_chances):
                # Aggregate
                aggregated_params_link = nodeInstance.aggregate_model_params(
                    decoded_params=list(decoded_params.values()),
                    round_number=round_number,
                    index=index
                )
                logger.info(f"[{index}][Round {round_number}] DFL: Aggregated {len(decoded_params)} submodels")

                # Update local model with aggregated weights
                local_path = f"{nodeInstance.file_write_destination}/{index}/{round_number}-{nodeInstance.name}_update.json"
                with open(local_path, "rb") as f:
                    data = pickle.load(f)

                if data and 'newUpdates' in data:
                    weights = nodeInstance.decode_params(data['newUpdates'])
                else:
                    logger.error(f"[{index}] Invalid aggregated data")
                    return

                nodeInstance.data_handlers[index].update_model(weights)

                # Publish RoundStart for next round so peers can pick it up
                next_round = round_number + 1
                nodeInstance.start_round(aggregated_params_link, next_round, index, node_type="dfl_aggregator")
                logger.info(f"[{index}][Round {round_number}] DFL: Published RoundStart for round {next_round}")
                return

            if decoded_params and check_chances:
                check_chances -= 1

            if not decoded_params and not check_chances:
                check_chances = 5

        except Exception as e:
            logger.error(f"[{index}][Round {round_number}] DFL aggregation error: {str(e)}")

        time.sleep(2)


# Extracts initParams from the policy at the specified index
def get_most_recent_agg_params(index):
    policy_name = f"{index}-r"
    agg_params = None

    try:
        headers = {
            'User-Agent': 'AnyLog/1.23',
            'command': f'blockchain get {index}'
        }
        response = requests.get(edgelake_node_url, headers=headers)

        if response.status_code == 200:
            data = response.json()

            if data:
                policy = data[0]
                policy_data = policy[policy_name]
                agg_params = policy_data["initParams"]

        return agg_params
    except Exception as e:
        logger.error(f"[{index}] Error in extracting round number: {str(e)}")


@app.post('/inference/{index}', response_class=PlainTextResponse)
def inference(index):
    """Inference on current model w/ data passed in."""
    try:
        logger.info(f"[{index}] received inference request")
        if not index:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Index must be specified."
            )
        results = node_instance.inference(index)
        response = {
                    'index': f'{index}',
                    'status': 'success',
                    'message': 'Inference completed successfully',
                    'model_accuracy': f'{str(results)}'
                    }
        return JSONResponse(content=response)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

class InferenceRequest(BaseModel):
    input: list
    index: str

@app.post('/infer')
def direct_inference(request: InferenceRequest):
    """Inference on current model w/ data passed in."""
    try:
        float_list = request.input
        index = request.index
        results = node_instance.direct_inference(index, float_list)
        response = {
            'prediction': str(results),
        }
        return response
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Error executing inference on model. Check inference function in data handler"
        )

if __name__ == '__main__':
    global port
    parser = argparse.ArgumentParser(description="Run the Node Server.")
    parser.add_argument('--port', type=int, default=8080, help="Port to run the server on.")
    args = parser.parse_args()

    run(
    "node_server:app",
        host="0.0.0.0",
        port=args.port,
        reload=False
    )
