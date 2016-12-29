import pymongo


def db_update_doc(collection, doc, nl, dl):
    """

    :param collection:
    :param doc:
    :param nl: name list
    :param dl: data list
    :return:
    """
    id = doc['_id']
    dict_update = dict()
    for k, v in zip(nl, dl):
        dict_update[k] = v
    collection.update_one({'_id': id}, {'$set': dict_update})
