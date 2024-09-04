import logging
from flask import Flask, jsonify, request
from flask.logging import create_logger
from datetime import datetime, timezone
from pathlib import Path
from math import floor
from promclient import PrometheusClient
from collections import defaultdict
import re

app = Flask(__name__)
app_logger = create_logger(app)
logging.basicConfig(level=logging.DEBUG)
# logging.basicConfig(level=logging.WARNING)

DATA_DIR = Path(__file__).parent
TIMESTAMP_DIR = DATA_DIR.joinpath("timestamp")
PROMETHEUS_URL = "http://prometheus:9090"

step_seconds=10
def get_timestamp_filepath(network: str, snapshot: str, action: str) -> Path:
    return TIMESTAMP_DIR.joinpath(f"{network}-{snapshot}-{action}.txt")

def save_timestamp(network: str, snapshot: str, action: str) -> None:
    path = get_timestamp_filepath(network, snapshot, action)
    if not path.parent.exists():
        path.parent.mkdir(exist_ok=True)
    with open(path, 'w') as f:
        f.write(str(floor(datetime.now(timezone.utc).timestamp())))

def get_timestamp(network: str, snapshot: str, action: str) -> int:
    path = get_timestamp_filepath(network, snapshot, action)
    with open(path, 'r') as f:
        timestamp = f.read()
    return int(floor(float(timestamp)))

@app.route("/state-conductor/environment/<network>/<snapshot>/sampling", methods=["POST"])
def post_sampling_action(network: str, snapshot: str):
    if not request.is_json:
        return jsonify({"error": "request is not json"}), 400

    action = request.json["action"]
    app_logger.info(f"Sampling action=#{action}, for environment {network}/{snapshot}")

    if not action in ["begin", "end"]:
        msg = f"action `{action}` is not defined"
        app_logger.warn(msg)
        return jsonify({ "error": msg }), 400

    save_timestamp(network, snapshot, action)

    response = {
        "network": network,
        "snapshot": snapshot,
        "action": action,
    }
    # response
    return jsonify(response)

@app.route("/state-conductor/environment/<network>/<snapshot>/state", methods=["GET"])
def get_sampled_state_stats(network: str, snapshot: str):

    begin = get_timestamp(network, snapshot, 'begin')
    end   = get_timestamp(network, snapshot, 'end')
    duration = end - begin

    queries = {
        "RX_BPS_AVG": f'avg_over_time(irate(container_network_receive_bytes_total{{instance="namespace-relabeler:5000",name=~"clab-.*"}}[15s])[{duration}s:])*8',
        "RX_BPS_MAX": f'max_over_time(irate(container_network_receive_bytes_total{{instance="namespace-relabeler:5000",name=~"clab-.*"}}[15s])[{duration}s:])*8',
        "RX_BPS_MIN": f'min_over_time(irate(container_network_receive_bytes_total{{instance="namespace-relabeler:5000",name=~"clab-.*"}}[15s])[{duration}s:])*8',
        "TX_BPS_AVG": f'avg_over_time(irate(container_network_transmit_bytes_total{{instance="namespace-relabeler:5000",name=~"clab-.*"}}[15s])[{duration}s:])*8',
        "TX_BPS_MAX": f'max_over_time(irate(container_network_transmit_bytes_total{{instance="namespace-relabeler:5000",name=~"clab-.*"}}[15s])[{duration}s:])*8',
        "TX_BPS_MIN": f'min_over_time(irate(container_network_transmit_bytes_total{{instance="namespace-relabeler:5000",name=~"clab-.*"}}[15s])[{duration}s:])*8',
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
            value = raw_metric["value"][1] # 1個目がタイムスタンプ、2個目が値
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
