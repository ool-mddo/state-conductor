import logging
from flask import Flask, jsonify, request
from flask.logging import create_logger

app = Flask(__name__)
app_logger = create_logger(app)
logging.basicConfig(level=logging.DEBUG)
# logging.basicConfig(level=logging.WARNING)


@app.route("/state-conductor/environment/<network>/<snapshot>/sampling", methods=["POST"])
def post_sampling_action(network: str, snapshot: str):
    if not request.is_json:
        return {"error", "request is not json"}, 500

    action = request.json["action"]
    app_logger.info(f"Sampling action=#{action}, for environment {network}/{snapshot}")

    response = {
        "network": network,
        "snapshot": snapshot,
        "action": action
    }
    # response
    return jsonify(response)


@app.route("/state-conductor/environment/<network>/<snapshot>/state", methods=["GET"])
def get_sampled_state_stats(network: str, snapshot: str):
    state_data = {
        "network": network,
        "snapshot": snapshot,
        "state": []
    }

    # response
    return jsonify(state_data)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
