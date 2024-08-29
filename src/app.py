import logging
from flask import Flask, jsonify, request
from flask.logging import create_logger
from datetime import datetime, timezone
from pathlib import Path

from promclient import PrometheusClient

app = Flask(__name__)
app_logger = create_logger(app)
logging.basicConfig(level=logging.DEBUG)
# logging.basicConfig(level=logging.WARNING)

DATA_DIR = Path(__file__).parent
TIMESTAMP_DIR = DATA_DIR.joinpath("timestamp")
PROMETHEUS_URL = "http://prometheus:9090"

step_seconds=10
QUERY_RX_BPS = f'irate(container_network_receive_bytes_total{{name=~"clab-.*"}}[{step_seconds}s])*8'

def get_timestamp_filepath(network: str, snapshot: str, action: str) -> Path:
    return TIMESTAMP_DIR.joinpath(f"{network}-{snapshot}-{action}.txt")

def save_timestamp(network: str, snapshot: str, action: str) -> None:
    path = get_timestamp_filepath(network, snapshot, action)
    if not path.parent.exists():
        path.parent.mkdir(exist_ok=True)
    with open(path, 'w') as f:
        f.write(str(datetime.now(timezone.utc).timestamp()))

def get_timestamp(network: str, snapshot: str, action: str) -> str:
    path = get_timestamp_filepath(network, snapshot, action)
    with open(path, 'r') as f:
        timestamp = f.read()
    return timestamp

@app.route("/state-conductor/environment/<network>/<snapshot>/sampling", methods=["POST"])
def post_sampling_action(network: str, snapshot: str):
    if not request.is_json:
        return jsonify({"error": "request is not json"}), 500

    action = request.json["action"]
    app_logger.info(f"Sampling action=#{action}, for environment {network}/{snapshot}")

    if not action in ["begin", "end"]:
        msg = f"action `{action}` is not defined"
        app_logger.warn(msg)
        return { "error", msg }

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

    client = PrometheusClient(PROMETHEUS_URL)
    metrics = client.query_metrics(QUERY_RX_BPS, begin, end, step=step_seconds*2)

    state_data = {
        "network": network,
        "snapshot": snapshot,
        "state": metrics,
    }

    # response
    return jsonify(state_data)

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
