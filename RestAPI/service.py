from repository import Repository

class Service:
    def __init__(self):
        self.repository = Repository()

    # def n_transactions_for_crypto(self, cryptocurrency, minutes):
    #     return self.repository.number_of_transaction_for_cryptocurrency_n_last_min(cryptocurrency, minutes)

    # def get_top_n_cryptocurrencies_per_hour(self, n):
    #     return self.repository.get_top_n_cryptocurrencies_per_hour(n)

    def get_cryptocurrency_current_price(self, cryptocurrency):
        return self.repository.get_cryptocurrency_current_price(cryptocurrency)

    def get_agg_transactions(self):
        return self.repository.get_agg_transactions()

    def get_agg_volume(self):
        return self.repository.get_agg_volume()

    def total_agg_stats(self):
        return self.repository.total_agg_stats()
