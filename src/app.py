import logging
from os import getenv
from flask import Flask, jsonify, request
from flask.logging import create_logger
from datetime import datetime, timezone
from pathlib import Path
from math import floor
from promclient import PrometheusClient
from collections import defaultdict
from typing import Dict
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


def _error_message(response: Dict, msg: str) -> Dict:
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


def _get_original_asis_state(network: str) -> dict:
    return _load_state_stats(network, "original_asis")

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

@app.route("/state-conductor/environment/<network>/<snapshot>/diff", methods=["GET"])
def get_state_stats_diff(network: str, snapshot: str):
    original_asis_state = _get_original_asis_state(network)
    sampled_stats = _load_state_stats(network, snapshot)

    if not original_asis_state:
        return jsonify({"error": f"original as-is state for {network} is not found"}), 404

    if not sampled_stats:
        return jsonify({"error": f"sampled state for {network}/{snapshot} is not found"}), 404

    diff = dict() 

    for device, if_stats in sampled_stats.items():

        if device not in original_asis_state:
            app_logger.info(f"device {device} is not found in {network}/original_asis. skipped")
            continue

        if device not in diff:
            diff[device] = dict()

        for interface, stats in if_stats.items():
            if interface not in original_asis_state[device]:
                app_logger.info(f"interface {interface} not found in {network}/original_asis. skipped")
                continue

            diff[device][interface] = dict()

            for metric_name, value in stats.items():
                diff[device][interface][metric_name] = dict()
                original_asis_value = original_asis_state[device][interface].get(metric_name)

                if original_asis_value == None:
                    app_logger.info(f"metric {metric_name} not found in {network}/original_asis. skipped")
                    diff[device][interface][metric_name] = None
                    continue

                app_logger.info(f"{value=}")
                original_asis_value = original_asis_state[device][interface].get(metric_name)
                diff[device][interface][metric_name]["counter"] = value - original_asis_value

                if original_asis_value == 0.0:
                    app_logger.info(f"{metric_name} of {network}/original_asis_value is 0. diff could not be calculated.")
                    diff[device][interface][metric_name]["ratio"] = None
                else:
                    diff[device][interface][metric_name]["ratio"] = value / original_asis_value

    return jsonify(diff), 200

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
