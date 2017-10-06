import glob
import json
import pandas as pd
import time
from sklearn.model_selection import train_test_split
from mind_palace.DictIntegerizer import DictIntegerizer
import cPickle as pickle
import os

def logBreak() :
    print "------------------------------------------"

def preparedata(data_path, pad_text ="<pad>",
                default_click_text = "<defaultclick>",
                test_size = 0.2,
                min_click_context = 0):

    filenames = glob.glob(data_path)

    start = time.clock()
    list_ = []
    for file_ in filenames:
        df = pd.read_csv(file_, sep="\t")
        list_.append(df)
        print "processed file : " + file_
    df = pd.concat(list_)
    print "time taken by data read : " + str(time.clock() - start)

    start = time.clock()
    productdict = DictIntegerizer(default = pad_text)
    productdict.get(pad_text)
    if default_click_text is not None :
        productdict.get(default_click_text)

    integerize = lambda x : productdict.get(x)
    df["positiveProductsInt"] = df["positiveProducts"].apply(integerize)

    def jsonandintegerize(jString) :
        strList = json.loads(jString)
        return map(integerize, strList)

    df = df[df["pastClickedProducts"].apply(lambda x: len(json.loads(x)) > min_click_context)]

    # jsonListCols = ["negativeProducts", "pastClickedProducts", "pastBoughtProducts"]
    jsonListCols = ["negativeProducts", "pastClickedProducts"]
    for col in jsonListCols :
        df[str(col + "Int")] = df[col].map(jsonandintegerize)


    # raw_data = df[["positiveProductsInt", "negativeProductsInt", "pastClickedProductsInt", "pastBoughtProductsInt"]]
    required_fields = ["positiveProductsInt", "negativeProductsInt", "pastClickedProductsInt"]
    raw_data = df[required_fields]
    trainFrame, testFrame = train_test_split(raw_data, test_size = test_size)

    # trainFrame = trainFrame[required_fields]
    # testFrame = testFrame[required_fields]

    click_len = raw_data["pastClickedProductsInt"].map(lambda x : len(x))
    print "histogram of num context clicks : "
    print click_len.value_counts()
    logBreak()

    print "time taken by data preprocess : " + str(time.clock() - start)
    print "data prep done"
    logBreak()

    return productdict, trainFrame, testFrame


def get_productdict_path(data_path):
    return data_path + "/productdict.pickle"

def get_train_path(data_path):
    return data_path + "/train.tsv"

def get_test_path(data_path):
    return data_path + "/test.tsv"

def get_productdict(data_path) :
    with open(data_path, 'rb') as handle:
        return pickle.load(handle)

if __name__ == '__main__' :
    raw_data_path = "/home/thejus/workspace/learn-cascading/data/sessionExplode-201708.MOB" + "/part-*"
    productdict, train, test = preparedata(raw_data_path, test_size=0.1)

    processed_data_path = "/home/thejus/workspace/learn-cascading/data/sessionExplode-201708.MOB.processed.10split"
    os.makedirs(processed_data_path)

    product_dict_file = get_productdict_path(processed_data_path)
    with open(product_dict_file, 'w+b') as handle:
        pickle.dump(productdict, handle, protocol=pickle.HIGHEST_PROTOCOL)

    train_file = get_train_path(processed_data_path)
    test_file = get_train_path(processed_data_path)

    train.to_csv(train_file, sep = "\t", index=False)
    test.to_csv(test_file, sep = "\t", index=False)

    logBreak()