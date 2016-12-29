from abc import ABCMeta, abstractmethod
import pymongo


class DataHandler(metaclass=ABCMeta):
    @abstractmethod
    def get_dates_data(self, stkcd, rptdt, period, attr):
        raise NotImplementedError("Should implement method name")

    @abstractmethod
    def get_fixedspan_dates_data(self, clname, start, end, attr):
        raise NotImplementedError("Should implement method name")

    @abstractmethod
    def get_data_given_dates(self, clname, dates, attr):
        raise NotImplementedError("Should implement method name")


class MongoDBHandler(DataHandler):
    def __init__(self, db_addr='mongodb://localhost:27017',
                 db_name='stockprices'):
        self.db_addr = db_addr
        self.db_name = db_name
        self.server = pymongo.MongoClient(db_addr)
        self.db = self.server[db_name]

    def get_dates_data(self, clname, rptdt, length, attr='CLOSE'):
        collection = self.db[clname]
        prices = collection.find({'DATE': {'$gte': rptdt}}).sort('DATE')
        # We pick one more day than period
        # the first day price will count for price change as basis
        p = list(prices[:length + 1])
        dates = [d['DATE'] for d in p]
        attr_data = [d[attr] for d in p]
        return dates, attr_data

    def get_fixedspan_dates_data(self, clname, start, end, attr):
        collection = self.db[clname]
        prices = collection.find({'DATE': {'$gte': start, '$lte': end}
                }).sort('DATE')
        p = list(prices)
        dates = [d['DATE'] for d in p]
        attr_data = [d[attr] for d in p]
        return dates, attr_data

    def get_data_given_dates(self, clname, dates, attr):
        collection = self.db[clname]
        prices = collection.find({'DATE': {'$in': dates}}).sort('DATE')
        p = [d[attr] for d in prices]
        return p
