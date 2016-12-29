#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Nov 25 18:48:25 2016

@author: david
"""
import pandas as pd
import pymongo


def merge_dataframe(folder, data_files):
    """
    Read input data files and save them a list.

    :param data_files: data file names
    :return: a list a Pandas DataFrame
    """
    data_folder = folder
    data = []
    for filename in data_files:
        df = pd.read_excel(
            data_folder + filename,
            header=[0],
            skiprows=[1,2],
            converters={'Stkcd': str})
        df['Rptdt'] = pd.to_datetime(df.Rptdt, infer_datetime_format=True)
        print(filename)
        data.append(df)
    return data


def dataframe_clean(d):
    """
    Reindex the concatenated DataFrame from 1 to len(df).
    :param d:
    :return:
    """
    df = pd.concat(d)
    # filter columns
    df = df[['Stkcd', 'Rptdt', 'Ananm', 'Brokern', 'Investrank', 'Stdrank',
             'Rankchg']]
    # drop NaN in Ananm field
    df.dropna(subset=['Ananm', 'Stdrank'], inplace=True)
    df.drop_duplicates(inplace=True)
    df.index = list(range(len(df)))  # reindex the DataFrame
    ser = df['Ananm'].map(lambda s: s.split(','))  # convert Ananm to list
    df['Ananm'] = ser
    return df


def insert_to_db(dataframe, dbaddr='mongodb://localhost:27017/',
                 dbname='analysts',
                 collectionname='reports'):
    """
    Connect to local mongodb server.

    :param dbaddr: address of mongoDB server
    :param dbname: name of database
    :param collectionname: name of collection
    """
    # Make connections
    mongodb_address = dbaddr
    client = pymongo.MongoClient(mongodb_address)
    db = client[dbname]
    collection = db[collectionname]

    # Reshape the DataFrame and convert it to a dict.
    df = dataframe
    tdf = df.T
    r = tdf.to_dict()
    # Bulk insert reports
    data = [r[i] for i in r]
    if collection.count() == 0:
        collection.insert_many(data)
    else:
        print("Collection is not empty. Do you want to delete all records?")
        print("Type 'yes' to confirm")
        s = input()
        if s == 'yes':
            collection.delete_many({})
            collection.insert_many(data)
            print("Insert {} reports to database.".format(collection.count()))
        else:
            print("Did not insert any reports to database.")


if __name__ == "__main__":
    folder = '/home/david/Mycode/analysts-refactoring/data/origin/'

    data_files = [
        'AF_Bench.xls',
        'AF_Bench1.xls',
        'AF_Bench2.xls',
        'AF_Bench3.xls',
        'AF_Bench4.xls',
        'AF_Bench5.xls',
        'AF_Bench6.xls',
        'AF_Bench7.xls',
        'AF_Bench8.xls']
    # data_files = ['AF_Bench.xls']
    ndf = dataframe_clean(merge_dataframe(folder, data_files))
    insert_to_db(ndf)