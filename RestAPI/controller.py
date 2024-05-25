from flask import Flask, jsonify
from service import Service

app = Flask(__name__)
service = Service()

@app.route('/<cryptocurrency>/trades/<int:minutes>', methods=['GET'])
def number_of_transaction_for_cryptocurrency_n_last_min(cryptocurrency: str, minutes: int):
    result = service.n_transactions_for_crypto(cryptocurrency, minutes)
    code = 200 if "Message" not in result.keys() else 404
    return jsonify(result), code


@app.route('/trades/hour/<int:n>', methods=['GET'])
def get_top_n_cryptocurrencies_per_hour(n):
    result = service.get_top_n_cryptocurrencies_per_hour(n)
    code = 200 if "Message" not in result.keys() else 404
    return jsonify(result), code


@app.route('/<cryptocurrency>/price', methods=['GET'])
def get_cryptocurrency_current_price(cryptocurrency):
    result = service.get_cryptocurrency_current_price(cryptocurrency)
    code = 200 if "Message" not in result.keys() else 404
    return jsonify(result), code


@app.route('/transactions/6-hours', methods=['GET'])
def get_agg_transactions():
    return service.get_agg_transactions()

@app.route('/volume/6-hours', methods=['GET'])
def get_agg_volume():
    return service.get_agg_volume()

@app.route('/total/12-hours', methods=['GET'])
def total_agg_stats():
    return service.total_agg_stats()


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=1488)
