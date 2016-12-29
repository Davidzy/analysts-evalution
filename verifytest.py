import pymongo
import datetime
import numpy as np
import report as rv
from data import MongoDBHandler

from utils import dbop

RANKDICT = {'买入': 1, '增持': 2, '中性': 3, '减持': 4, '卖出': 5}


class VerifyTest(object):

    BASEINDEX = '000300'

    def __init__(self, data_handler, db_addr, db_name, collection_name,
                 begin, end, ranktypes=None, stks=None, ananms=None,
                 veriperiod=[20, 60, 120, 240], doc_replace=False,
                 sim_switch=False):
        # db parameters
        self.data_handler = data_handler
        self.db_addr = db_addr
        self.db_name = db_name
        self.collection_name = collection_name
        # test query parameters
        self.begin = begin
        self.end = end
        self.ranktypes = ranktypes
        self.stks = stks
        self.ananms = ananms
        self.veriperiod = veriperiod
        # get reports to be test
        self.doc_replace=doc_replace
        self.sim_switch=sim_switch
        self.query = self._generate_query(
            self.begin, self.end, self.ranktypes, self.stks, self.ananms)
        self.reports_collection = self._reports_collection()
        self.reports = list(self.reports_collection.find(self.query))

    def _reports_collection(self):
        client = pymongo.MongoClient(self.db_addr)
        db = client[self.db_name]
        collection = db[self.collection_name]
        return collection

    def _generate_query(self, begin, end, ranktypes=None, stks=None,
                        ananms=None):
        query = dict()
        query['Rptdt'] = {'$gte': begin, '$lte': end}
        if ranktypes:
            query['Stdrank'] = {'$in': ranktypes}
        if stks:
            query['Stkcd'] = {'$in': stks}
        if ananms:
            query['Ananm'] = {'$in': ananms}
        return query

    def _calc_xdays(self, days1, days2):
        daysx = (np.array(days1) - np.array(days2)) / np.array(self.veriperiod)
        return daysx

    def run_test(self):
        i = 0
        vnames = ['v' + str(p) for p in self.veriperiod]
        snames = ['s' + str(p) for p in self.veriperiod]
        xnames = ['x' + str(p) for p in self.veriperiod]
        onames = ['o' + str(p) for p in self.veriperiod]
        for report in self.reports:
            i += 1
            print("No.{no:d} / {total}: report {stkcd} {rank} on {date} by \
            {names} processed.".format(
                        no=i, stkcd=report['Stkcd'], total=len(self.reports),
                        date=report['Rptdt'], rank=report['Stdrank'],
                        names=', '.join(report['Ananm'])))
            print("The doc._id is {}".format(report['_id']))
            try:
                calc_v, calc_s, calc_x, calc_o = False, False, False, False

                if not (vnames[0] in report and vnames[-1] in report) \
                    or self.doc_replace:
                    calc_v = True

                if calc_v:
                    print("calculate verify days")
                    vdays = rv.actual_verified_length_compared_basic_index(
                        report, self.BASEINDEX, self.veriperiod,
                        self.data_handler
                    )
                    dbop.db_update_doc(
                        collection=self.reports_collection, doc=report,
                        nl=vnames, dl=vdays)
                else:
                    vdays = [report[mark] for mark in vnames]

                if not (snames[0] in report and snames[-1] in report) \
                    or self.sim_switch:
                    calc_s = True
                if calc_s:
                    print("deal with simulation")
                    sdays = rv.simulated_length(
                        report, self.BASEINDEX, self.veriperiod,
                        self.data_handler
                    )
                    dbop.db_update_doc(
                        collection=self.reports_collection, doc=report,
                        nl=snames, dl=sdays)
                else:
                    sdays = [report[mark] for mark in snames]

                if not (xnames[0] in report and xnames[-1] in report) \
                    or self.doc_replace:
                    calc_x = True
                if calc_x:
                    print("calculate x ration")
                    xratio = self._calc_xdays(vdays, sdays)
                    dbop.db_update_doc(
                        collection=self.reports_collection, doc=report,
                        nl=xnames, dl=xratio)

                if not (onames[0] in report and onames[-1] in report) \
                    or self.doc_replace:
                    calc_o = True
                if calc_o:
                    print("calculate verify days without comparing index")
                    odays = rv.actual_verified_length(
                        report, self.veriperiod, self.data_handler
                    )
                    dbop.db_update_doc(
                        collection=self.reports_collection, doc=report,
                        nl=onames, dl=odays)

            except (IndexError, ValueError, AssertionError) as e:
                print(i, e)
                print(report)
                self.reports_collection.delete_one({'_id': report['_id']})


if __name__ == "__main__":
    mongo_handler = MongoDBHandler()
    dbaddr = 'mongodb://localhost:27017'
    dbname = 'analysts'
    collectionname = 'reports'
    begin_date = datetime.datetime(2015, 1, 1, 0, 0, 0)
    end_date = datetime.datetime(2015, 12, 31, 0, 0, 0)
    thistest = VerifyTest(
        data_handler=mongo_handler,
        db_addr=dbaddr,
        db_name=dbname,
        collection_name=collectionname,
        begin=begin_date,
        end=end_date,
        ranktypes=None,
        stks=['000001'],
        ananms=None,
        veriperiod=[20, 60, 120, 240],
        doc_replace=False,
        sim_switch=False)
    thistest.run_test()









