def to_collection_name(stkcd):
    """
    Get the collection name in db 'stockprices' given stock code.
    :param stkcd:
    :return:
    """
    collection_name = ('SH' + stkcd) if stkcd[0] == '6' else ('SZ' + stkcd)
    return collection_name


def to_index_name(idxcd):
    """
    Get the collection name in db 'stockprices' given index.
    沪深300 000300.SH
    上证指数 000001.SH
    上证50 000016.SH
    上证180 000010.SH
    中证500 000905.SH
    深圳成指 399001.SZ
    中小板指数 399005.SZ
    创业板指数 399006.SZ
    :param idxcd:
    :return:
    """
    indexes =  ['000300.SH', '000001.SH', '000016.SH', '000010.SH', '000905.SH',
                '399001.SZ', '399005.SZ', '399006.SZ']
    name = 'Idx' + idxcd
    return name
