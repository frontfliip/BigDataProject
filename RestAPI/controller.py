from flask import Flask, jsonify

from service import Service

app = Flask(__name__)
service = Service()


@app.route('/events', methods=['GET'])
def create_event():
    return jsonify({"Response": "COOL"}), 200


@app.route('/{cryptocurrency}/trades/{minutes}', methods=['GET'])
def number_of_transaction_for_cryptocurrency_n_last_min(cryptocurrency, minutes):
    return jsonify({"Response": "COOL"}), 200


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=1488)
