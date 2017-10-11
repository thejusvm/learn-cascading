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

def process_file(data_path,
                productdict,
                min_click_context = 0):

    df = pd.read_csv(data_path, sep="\t")

    start = time.clock()

    integerize = lambda x : productdict.get(x)
    df["positiveProductsInt"] = df["positiveProducts"].apply(integerize)

    def jsonandintegerize(jString) :
        strList = json.loads(jString)
        return map(integerize, strList)

    df = df[df["pastClickedProducts"].apply(lambda x: len(json.loads(x)) >= min_click_context)]

    # jsonListCols = ["negativeProducts", "pastClickedProducts", "pastBoughtProducts"]
    jsonListCols = ["negativeProducts", "pastClickedProducts"]
    for col in jsonListCols :
        df[str(col + "Int")] = df[col].map(jsonandintegerize)

    required_fields = ["positiveProductsInt", "negativeProductsInt", "pastClickedProductsInt"]
    raw_data = df[required_fields]

    click_len = raw_data["pastClickedProductsInt"].map(lambda x : len(x))
    print "histogram of num context clicks : "
    print click_len.value_counts()
    logBreak()

    print "time taken by data preprocess : " + str(time.clock() - start)
    print "data prep done"
    logBreak()

    return raw_data


def get_productdict_path(data_path):
    return data_path + "/productdict.pickle"

def get_train_path(data_path):
    return data_path + "/train.tsv"

def get_test_path(data_path):
    return data_path + "/test.tsv"

def get_productdict(data_path) :
    with open(data_path, 'rb') as handle:
        return pickle.load(handle)

def prepare_data(raw_data_path,
                 processed_data_path,
                 pad_text ="<pad>",
                 default_click_text = "<defaultclick>"):

    productdict = DictIntegerizer(default = pad_text)
    productdict.get(pad_text)
    if default_click_text is not None :
        productdict.get(default_click_text)

    filenames = glob.glob(raw_data_path)
    start = time.clock()
    counter = 0
    for in_file in filenames:
        logBreak()
        print "start file processing : " + in_file + ", with dict size : " + str(productdict.currentCount)
        pd = process_file(in_file, productdict)
        print "end file processing : " + in_file + ", in " + str(time.clock() - start)
        out_file = processed_data_path + "/part-" + str(counter)
        print out_file
        pd.to_csv(out_file, sep ="\t", index=False)
        print "dumped content of " + in_file + " to " + out_file
        counter += 1
        logBreak()

    return dict


if __name__ == '__main__' :
    raw_data_path = "/home/thejus/workspace/learn-cascading/data/sessionExplode-201708.MOB" + "/part-*"
    processed_data_path = "/home/thejus/workspace/learn-cascading/data/sessionExplode-201708.MOB.processed"
    os.makedirs(processed_data_path)

    productdict = prepare_data(raw_data_path, processed_data_path)
    product_dict_file = get_productdict_path(processed_data_path)
    with open(product_dict_file, 'w+b') as handle:
        pickle.dump(productdict, handle, protocol=pickle.HIGHEST_PROTOCOL)

    logBreak()