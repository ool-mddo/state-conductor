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

app = Flask(__name__)
app_logger = create_logger(app)
logging.basicConfig(level=logging.DEBUG)
# logging.basicConfig(level=logging.WARNING)

DATA_DIR = Path(__file__).parent
TIMESTAMP_DIR = DATA_DIR.joinpath("timestamp")
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

    # response
    response["action"] = action
    response["timestamp"] = _get_timestamp(network, snapshot, action)
    return jsonify(response)


@app.route("/state-conductor/environment/<network>/<snapshot>/state", methods=["GET"])
def get_sampled_state_stats(network: str, snapshot: str):

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
            value = raw_metric["value"][1]  # 1個目がタイムスタンプ、2個目が値
            metrics[device][interface][metric_type] = value

    state_data = {
        "network": network,
        "snapshot": snapshot,
        "state": metrics,
    }

    # response
    return jsonify(state_data)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
