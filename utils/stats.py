import datetime
import numpy as np
from scipy.stats.stats import pearsonr as correlation
from utils import stkmap
from data import MongoDBHandler


def history_stock_index_correlation(stk, index, end_date, timespan, attr,
                                    datahandler):
    """
    Correlation between a stock and base index given time span.

    :param stk:
    :param index:
    :return:
    """
    start_date = end_date - datetime.timedelta(timespan)
    dates, stk_price = datahandler.get_fixedspan_dates_data(
        stk, start_date, end_date, attr)

    if len(stk_price) < 10:
        # only calculate index sigma
        idx_dates, idx_price = datahandler.get_fixedspan_dates_data(
            index, start_date, end_date, attr)
        rho = 0
        sigma_stk = 0.1
        r2 = np.zeros(len(idx_price))
        idx_price = np.array(idx_price)
        r2[1:] = idx_price[1:] / idx_price[:-1] - 1
        sigma_idx = r2.std()
    else:
        idx_price = datahandler.get_data_given_dates(index, dates, attr)
        r1 = np.zeros(len(stk_price))
        stk_price = np.array(stk_price)
        r1[1:] = stk_price[1:] / stk_price[:-1] - 1
        sigma_stk = r1.std()
        r2 = np.zeros(len(idx_price))
        idx_price = np.array(idx_price)
        r2[1:] = idx_price[1:] / idx_price[:-1] - 1
        sigma_idx = r2.std()
        rho = correlation(r1, r2)[0]
    return rho, sigma_stk, sigma_idx


# it's a test.
if __name__ == "__main__":
    data_handler = MongoDBHandler()
    stk_name = stkmap.to_collection_name('000001')
    idx_name = stkmap.to_index_name('000001')
    r, stk, idx = history_stock_index_correlation(
        stk_name, idx_name, datetime.datetime(2015, 1, 1, 0, 0, 0), 365,
        'CLOSE', data_handler)
    print(r, stk, idx)

