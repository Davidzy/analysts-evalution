import numpy as np
import pandas as pd


def random_two_series(mu1, sigma1, mu2, sigma2, rho, horizon=120):
    """
    Generate two normal distributed series that have correlation rho.

    :param mu1:
    :param sigma1:
    :param mu2:
    :param sigma2:
    :param rho: correlation of two series.
    :param horizon: length of series to be generated.
    :return:
    """
    z1 = np.random.randn(horizon)  # x1 simulates stock price
    x1 = z1 * sigma1 + mu1
    z2 = np.random.randn(horizon)  #
    x2 = rho * sigma2 * z1 + np.sqrt(1 - rho ** 2) * sigma2 * z2 + mu2
    # 转换为价格的序列
    y1 = 1 + x1
    y2 = 1 + x2
    p1 = np.ones(horizon)  # 价格绝对涨跌幅，相对于第一天
    p2 = np.ones(horizon)
    for i in range(1, horizon):
        p1[i] = p1[i-1] * y1[i-1]
        p2[i] = p2[i-1] * y2[i-1]
    # for i in range(horizon):
    #     p1[i] = np.prod(y1[:i])
    #     p2[i] = np.prod(y2[:i])
    return p1, p2


def stk_index_price_simulation(mu_stk, sigma_stk, mu_idx, sigma_idx,
                               rho, horizon=120, sim_times=100):
    """
    Simulation prices of stocks and index given experiment times.

    :param mu_stk:
    :param sigma_stk:
    :param mu_idx:
    :param sigma_idx:
    :param rho:
    :param sim_times:
    :return: two pandas DataFrame, one for stock, one for index.
    """
    stk_sim = dict()
    idx_sim = dict()
    for e in range(sim_times):
        stk_sim[e], idx_sim[e] = random_two_series(
            mu_stk, sigma_stk, mu_idx, sigma_idx, rho, horizon)
    df1 = pd.DataFrame(stk_sim)
    df2 = pd.DataFrame(idx_sim)
    return df1, df2
