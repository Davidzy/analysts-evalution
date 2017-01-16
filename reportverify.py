import numpy as np
from utils import (stkmap, stats, simulation)

RANKDICT = {'买入': 1, '增持': 2, '中性': 3, '减持': 4, '卖出': 5}


def _price_change(arr):
    """
    Calculate price changes rate.

    :param array: array of stock price.
    :return: array of change percent.
    """
    a = np.array(arr)
    return (a[1:] - a[0]) / a[0]


def _rank_achieved_length(pc1, pc2, rank, period):
    assert len(pc1) == len(pc2)
    p1 = pc1[:period]
    p2 = pc2[:period]
    dif = p1 - p2
    if rank == "买入":
        filtered_dif = filter(lambda x: x >= 0.15, dif)
        days = len(list(filtered_dif))
    elif rank == "增持":
        filtered_dif = filter(lambda x: x >= 0.05, dif)
        days = len(list(filtered_dif))
    elif rank == "中性":
        filtered_dif = filter(lambda x: -0.05 < x < 0.05, dif)
        days = len(list(filtered_dif))
    elif rank == "减持":
        filtered_dif = filter(lambda x: x <= -0.05, dif)
        days = len(list(filtered_dif))
    elif rank == "卖出":
        filtered_dif = filter(lambda x: x <= -0.15, dif)
        days = len(list(filtered_dif))
    else:
        print("Unknown rank")
        raise
    return days


def actual_verified_length_compared_basic_index(report, index, periods,
                                                datahandler):
    verified = list()
    r = report
    stkcd = r['Stkcd']
    stdrank = r['Stdrank']
    rptdt = r['Rptdt']
    period = max(periods)

    stkcl = stkmap.to_collection_name(stkcd)
    dates, close_price = datahandler.get_dates_data(
        stkcl, rptdt, period, attr='CLOSE')
    pc = _price_change(close_price)  # price changes

    idx = stkmap.to_index_name(index)
    base_price = datahandler.get_data_given_dates(idx, dates, attr='CLOSE')
    bc = _price_change(base_price)

    for period in periods:
        days = _rank_achieved_length(pc, bc, stdrank, period)
        verified.append(days)

    return verified


def actual_verified_length(report, periods, datahandler):
    verified = list()
    r = report
    stkcd = r['Stkcd']
    stdrank = r['Stdrank']
    rptdt = r['Rptdt']
    period = max(periods)

    stkcl = stkmap.to_collection_name(stkcd)
    dates, close_price = datahandler.get_dates_data(
        stkcl, rptdt, period, attr='CLOSE')
    pc = _price_change(close_price)  # price changes

    bc = np.zeros(len(pc))

    for period in periods:
        days = _rank_achieved_length(pc, bc, stdrank, period)
        verified.append(days)

    return verified


def _simulated_difference(df1, df2, rank, period):
    assert df1.shape == df2.shape
    sim_times = df1.shape[1]  # shape return a tuple row by column
    dif = df1[:period] - df2[:period]
    if rank == '买入':
        d = dif >= 0.15
    elif rank == '增持':
        d = dif >= 0.05
    elif rank == '中性':
        d1 = dif > -0.05
        d2 = dif < 0.05
        d = d1 & d2
    elif rank == '减持':
        d = dif <= -0.05
    elif rank == '卖出':
        d = dif <= -0.15
    else:
        print("Unknown rank.")
        raise
    days = d.sum().sum() / sim_times
    return days


def simulated_length(report, index, periods, datahandler):

    simed = list()

    stkcd = report['Stkcd']
    rptdt = report['Rptdt']
    stdrank = report['Stdrank']
    stk_clname = stkmap.to_collection_name(stkcd)
    idx_clname = stkmap.to_index_name(index)

    rho, s1, s2 = stats.history_stock_index_correlation(
        stk_clname, idx_clname, rptdt, 365, 'CLOSE', datahandler)
    length = max(periods)
    df_stk, df_idx = simulation.stk_index_price_simulation(
        0, s1, 0, s2, rho, length, 10000
    )

    for period in periods:
        days = _simulated_difference(df_stk, df_idx, stdrank, period)
        simed.append(days)
    return simed