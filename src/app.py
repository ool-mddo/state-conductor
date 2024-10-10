import logging
from os import getenv
from flask import Flask, jsonify, request
from flask.logging import create_logger
from datetime import datetime, timezone
from pathlib import Path
from math import floor
from promclient import PrometheusClient
from collections import defaultdict
import requests
import re
import json

app = Flask(__name__)
app_logger = create_logger(app)
logging.basicConfig(level=logging.DEBUG)
# logging.basicConfig(level=logging.WARNING)

DATA_DIR = Path(__file__).parent
TIMESTAMP_DIR = DATA_DIR.joinpath("timestamp")
STATE_DIR = DATA_DIR.joinpath("state")
PROMETHEUS_URL = getenv("PROMETHEUS_URL", "http://prometheus:9090")
API_PROXY_HOST = "api-proxy"

def _get_timestamp_filepath(network: str, snapshot: str, action: str) -> Path:
    return TIMESTAMP_DIR.joinpath(f"{network}-{snapshot}-{action}.txt")


def _exist_timestamp_file(network: str, snapshot: str, action: str) -> bool:
    path = _get_timestamp_filepath(network, snapshot, action)
    return path.exists()


def _exist_ongoing_sampling(network: str, snapshot: str) -> bool:
    if not _exist_timestamp_file(network, snapshot, "begin"):
        return False

    if _exist_timestamp_file(network, snapshot, "end"):
        begin = _get_timestamp(network, snapshot, "begin")
        end = _get_timestamp(network, snapshot, "end")
        if begin > end:
            return True
    else:
        return True

    return False

def _get_state_stats_filepath(network, snapshot: str) -> Path:
    return STATE_DIR.joinpath(f"{network}-{snapshot}-stats.json")

def _save_state_stats(network: str, snapshot: str, state_stats: dict) -> None:
    stats_path = _get_state_stats_filepath(network, snapshot)

    if not stats_path.parent.exists():
        stats_path.parent.mkdir(exist_ok=True, parents=True)

    if stats_path.exists():
        app_logger.info(f"{stats_path} already exists. overwriting...")

    with open(stats_path, "w") as f:
        app_logger.info(f"saving stats to {stats_path}")
        json.dump(state_stats, f)

def _load_state_stats(network: str, snapshot: str) -> dict|None:
    stats_path = _get_state_stats_filepath(network, snapshot)

    if not stats_path.exists():
        app_logger.error(f"{stats_path} does not exist.")
        return None

    app_logger.info(f"loading state stats from {stats_path}")
    with open(stats_path, "r") as f:
        state_stats = json.load(f)
    
    return state_stats

def _save_timestamp(network: str, snapshot: str, action: str) -> None:
    path = _get_timestamp_filepath(network, snapshot, action)
    if not path.parent.exists():
        path.parent.mkdir(exist_ok=True)
    with open(path, "w") as f:
        # save unix timestamp (epoch)
        f.write(str(floor(datetime.now(timezone.utc).timestamp())))


def _get_timestamp(network: str, snapshot: str, action: str) -> int:
    path = _get_timestamp_filepath(network, snapshot, action)
    with open(path, "r") as f:
        timestamp = f.read()
    return int(floor(float(timestamp)))


def _error_message(response: dict, msg: str) -> dict:
    app_logger.error(msg)
    response["error"] = msg
    return response


@app.route(
    "/state-conductor/environment/<network>/<snapshot>/sampling", methods=["POST"]
)
def post_sampling_action(network: str, snapshot: str):
    response = {"network": network, "snapshot": snapshot}

    if not request.is_json:
        response["error"] = "request is not json"
        return jsonify(response), 400

    action = request.json["action"]
    app_logger.info(f"Sampling action=#{action}, for environment {network}/{snapshot}")

    # error check
    if not action in ["begin", "end"]:
        msg = f"action `{action}` is not defined"
        return jsonify(_error_message(response, msg)), 400
    if action == "begin" and _exist_ongoing_sampling(network, snapshot):
        msg = "sampling has already began. post action=end to complete the running sampling before begin."
        return jsonify(_error_message(response, msg)), 400
    if action == "end" and not _exist_ongoing_sampling(network, snapshot):
        msg = "sampling does not begin. begin at first to post action=begin"
        return jsonify(_error_message(response, msg)), 404

    # move into action
    _save_timestamp(network, snapshot, action)

    if action == "end":
        app_logger.info("fetching state stats...")
        # save state stats
        state_stats = _fetch_sampled_state_stats(network, snapshot)
        _save_state_stats(network, snapshot, state_stats)

    # response
    response["action"] = action
    response["timestamp"] = _get_timestamp(network, snapshot, action)
    return jsonify(response)

def _fetch_usecase_params(usecase: str, network: str) -> dict:

    try:
        response = requests.get(f"http://{API_PROXY_HOST}/usecases/{usecase}/{network}/params")
    except:
        app_logger.error(f"failed to fetch usecase params.")
        raise

    if response.status_code != 200:
        app_logger.error(f"failed to fetch usecase params. status_code={response.status_code}")
    
    app_logger.debug(f"response for usecase: {usecase}, network: {network}: {response.text}")
    
    return response.json()

def _fetch_ns_convert_table(network: str) -> list | dict:
    try:
        response = requests.get(f"http://{API_PROXY_HOST}/topologies/{network}/ns_convert_table")
        response.raise_for_status()
        app_logger.debug(response.text)
        return response.json().get('tp_name_table')
    except:
        app_logger.error(f"Failed to get ns_convert_table for {network}")
        raise

@app.route("/state-conductor/environment/<network>/<snapshot>/state", methods=["GET"])
def get_sampled_state_stats(network: str, snapshot: str):

    state_stats = _load_state_stats(network, snapshot)

    if not state_stats:
        return jsonify({"error": f"state stats for {network}/{snapshot} is not found"}), 404

    response = {
        "network": network,
        "snapshot": snapshot,
        "state": state_stats,
    }

    return jsonify(response)

@app.route("/state-conductor/<usecase>/<network>/snapshot_diff/<source_snapshot>/<destination_snapshot>", methods=["GET"])
def get_state_stats_diff(usecase: str, network: str, source_snapshot: str, destination_snapshot: str):
    source_stats = _load_state_stats(network, source_snapshot)
    destination_stats = _load_state_stats(network, destination_snapshot)

    if not source_stats:
        return jsonify({"error": f"state stats for source_snapshot ({network}/{source_snapshot}) is not found"}), 404

    if not destination_stats:
        return jsonify({"error": f"state stats for destination_snapshot({network}/{destination_snapshot}) is not found"}), 404

    diff = dict() 

    target_node = request.args.get("node")
    target_interface = request.args.get("interface")

    usecase_params = _fetch_usecase_params(usecase, network)
    try:
        traffic_scale = float(usecase_params["expected_traffic"]["emulated_traffic"]["scale"])
        app_logger.debug(f"traffic_scale: {traffic_scale=}")
    except KeyError:
        app_logger.error(f"failed to fetch usecase params. expected_traffic.emulated_traffic.scale is not found.")
        return jsonify({"error": f"failed to fetch usecase params. expected_traffic.emulated_traffic.scale is not found."}), 500

    app_logger.info(f"target_node: {target_node}, target_interface: {target_interface}, usecase_params: {usecase_params}")

    for dest_device, dest_if_stats in destination_stats.items():
        if target_node and dest_device != target_node:
            app_logger.debug(f"device `{dest_device}` is not target. skipped")
            continue

        if dest_device not in source_stats:
            app_logger.info(f"state stats for device `{dest_device}` is not found in source_snapshot ({network}/{source_snapshot}). skipped")
            continue

        for dest_interface, dest_stats in dest_if_stats.items():
            if target_interface and dest_interface != target_interface:
                app_logger.debug(f"interface `{dest_interface}` is not target. skipped")
                continue

            if dest_interface not in source_stats[dest_device]:
                app_logger.info(f"state stats for interface `{dest_interface}` is not found in destination_snapshot ({network}/{destination_snapshot}). skipped")
                continue

            if dest_device not in diff:
                diff[dest_device] = dict()

            diff[dest_device][dest_interface] = dict()

            for metric_name, dest_state_value in dest_stats.items():
                diff[dest_device][dest_interface][metric_name] = dict()
                src_state_value = source_stats[dest_device][dest_interface].get(metric_name)

                if src_state_value == None:
                    app_logger.info(f"metric {metric_name} not found in source_snapshot ({network}/{source_snapshot}). skipped")
                    diff[dest_device][dest_interface][metric_name] = None
                    continue

                diff[dest_device][dest_interface][metric_name]["counter"] = (dest_state_value - src_state_value) / traffic_scale

                if src_state_value == 0.0:
                    app_logger.info(f"{metric_name} in source_snapshot ({network}/{source_snapshot}) is 0. src/dst ration could not be calculated.")
                    diff[dest_device][dest_interface][metric_name]["ratio"] = None
                else:
                    diff[dest_device][dest_interface][metric_name]["ratio"] = dest_state_value / src_state_value

    if diff == {}:
        app_logger.info(f"no snapshot diff generated between {network}/{source_snapshot} and {network}/{destination_snapshot}")

    result = {
        "network": network,
        "source_snapshot": source_snapshot,
        "destination_snapshot": destination_snapshot,
        "diff": diff,
    }

    return jsonify(result), 200

def _fetch_sampled_state_stats(network: str, snapshot: str) -> dict:

    begin = _get_timestamp(network, snapshot, "begin")
    end = _get_timestamp(network, snapshot, "end")
    duration = end - begin

    queries = {
        "RX_BPS_AVG": f'avg_over_time(irate(container_network_receive_bytes_total{{instance="namespace-relabeler:5000",name=~"clab-.*"}}[10s])[{duration}s:])*8',
        "RX_BPS_MAX": f'max_over_time(irate(container_network_receive_bytes_total{{instance="namespace-relabeler:5000",name=~"clab-.*"}}[10s])[{duration}s:])*8',
        "RX_BPS_MIN": f'min_over_time(irate(container_network_receive_bytes_total{{instance="namespace-relabeler:5000",name=~"clab-.*"}}[10s])[{duration}s:])*8',
        "TX_BPS_AVG": f'avg_over_time(irate(container_network_transmit_bytes_total{{instance="namespace-relabeler:5000",name=~"clab-.*"}}[10s])[{duration}s:])*8',
        "TX_BPS_MAX": f'max_over_time(irate(container_network_transmit_bytes_total{{instance="namespace-relabeler:5000",name=~"clab-.*"}}[10s])[{duration}s:])*8',
        "TX_BPS_MIN": f'min_over_time(irate(container_network_transmit_bytes_total{{instance="namespace-relabeler:5000",name=~"clab-.*"}}[10s])[{duration}s:])*8',
    }

    required_keys_map = {
        "device": "container_label_clab_node_name",
        "interface": "interface",
    }

    ignored_ifname_patterns = [
        r"^erspan\d+",
        r"^gre\d+",
        r"^gretap\d+",
        r"^ip6tnl\d+",
        r"^lsi",
        r"^sit\d+",
        r"^tunl\d+",
        r"^irb",
        r"^eth0",
    ]

    client = PrometheusClient(PROMETHEUS_URL)
    metrics = defaultdict(lambda: defaultdict(dict))
    for metric_type, query in queries.items():
        raw_metrics = client.query_instant_metrics(query, end)
        for raw_metric in raw_metrics:
            interface = raw_metric["metric"].get(required_keys_map["interface"])
            if not interface:
                app_logger.debug(f"interface is not found. skipping")
                continue
            if any(re.match(pattern, interface) for pattern in ignored_ifname_patterns):
                app_logger.debug(f"{interface} is not needed. skipping")
                continue
            device = raw_metric["metric"].get(required_keys_map["device"])
            if not device:
                app_logger.debug(f"device is not found. skipping")
                continue
            value = float(raw_metric["value"][1])  # 1個目がタイムスタンプ、2個目が値
            metrics[device][interface][metric_type] = value

    return metrics

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
