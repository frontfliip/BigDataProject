import logging

from repository import Repository

logging.basicConfig(level=logging.DEBUG)


class Service:
    def __init__(self):
        self.repository = Repository()
        print("Service is created", flush=True)

    def n_transactions_for_crypto(self, cryptocurrency, minutes):
        return self.repository.number_of_transaction_for_cryptocurrency_n_last_min(cryptocurrency, minutes)

    def get_top_n_cryptocurrencies_per_hour(self, n):
        return self.repository.get_top_n_cryptocurrencies_per_hour(n)

    def get_cryptocurrency_current_price(self, cryptocurrency):
        return self.repository.get_cryptocurrency_current_price(cryptocurrency)

    def get_check(self):
        return self.repository.get_check()
