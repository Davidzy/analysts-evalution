from verifytest import VerifyTest
import datetime
from data import MongoDBHandler


if __name__ == "__main__":
    mongo_handler = MongoDBHandler()
    dbaddr = 'mongodb://localhost:27017'
    dbname = 'analysts'
    collectionname = 'reports'
    begin_date = datetime.datetime(2010, 1, 1, 0, 0, 0)
    end_date = datetime.datetime(2012, 12, 31, 0, 0, 0)
    thistest = VerifyTest(
        data_handler=mongo_handler,
        db_addr=dbaddr,
        db_name=dbname,
        collection_name=collectionname,
        begin=begin_date,
        end=end_date,
        ranktypes=None,
        stks=None,
        ananms=None,
        veriperiod=[20, 60, 120, 240],
        doc_replace=True,
        sim_switch=False)
    thistest.run_test()