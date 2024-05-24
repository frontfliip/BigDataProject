import logging

from flask import Flask, jsonify

from service import Service

logging.basicConfig(level=logging.DEBUG)
app = Flask(__name__)
service = Service()
logging.info("Controller is created")
print("Controller is created", flush=True)


@app.route('/try', methods=['GET'])
def check():
    result = service.get_check()
    return jsonify(result), 200


@app.route('/try2', methods=['GET'])
def chec2k():
    result = service.get_check2()
    return jsonify(result), 200


@app.route('/<cryptocurrency>/trades/<int:minutes>', methods=['GET'])
def number_of_transaction_for_cryptocurrency_n_last_min(cryptocurrency: str, minutes: int):
    result = service.n_transactions_for_crypto(cryptocurrency, minutes)
    return jsonify(result), 200
    # return jsonify({"Response": "COOL"}), 200


@app.route('/trades/hour', methods=['GET'])
def get_top_n_cryptocurrencies_per_hour(n):
    result = service.get_top_n_cryptocurrencies_per_hour(n)
    return jsonify({"Response": "COOL"}), 200


@app.route('/<cryptocurrency>/price', methods=['GET'])
def get_cryptocurrency_current_price(cryptocurrency):
    print(cryptocurrency, flush=True)
    print(type(cryptocurrency), flush=True)
    result = service.get_cryptocurrency_current_price(cryptocurrency)
    # return jsonify({"Response": "COOL"}), 200
    return jsonify(result), 200


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=1488)
