import glob
import pandas as pd
import sys
import time
import json

"""

"""

def logBreak() :
    print "------------------------------------------"

def add_to_data_dict(dd, index, contexts, products):
    if len(contexts) == 0 :
        contexts = [{'productId' : ""}]
    for context in contexts:
        for product in products:
            key = context['productId']+":"+product['productId']
            if key not in dd :
                dd[key] = [0, 0]
            dd[key][index] += 1

def process_file(data_path, data_dict):



    df = pd.read_csv(data_path, sep="\t")
    df = df[df["findingMethod"].apply(lambda x: str(x).lower() == "search")]
    start = time.clock()
    df["positiveProducts"] = df["positiveProducts"].apply(lambda x : [json.loads(x)])
    df["negativeProducts"] = df["negativeProducts"].apply(json.loads)
    df["pastClickedProducts"] = df["pastClickedProducts"].apply(json.loads)

    df[["pastClickedProducts", "negativeProducts"]].apply(lambda x : add_to_data_dict(data_dict, 1, x[0], x[1]), axis=1)
    df[["pastClickedProducts", "positiveProducts"]].apply(lambda x : add_to_data_dict(data_dict, 0, x[0], x[1]), axis=1)

    print "time taken by data preprocess : " + str(time.clock() - start) + " with dict_size : " + str(len(data_dict))

def prepare_data(raw_data_path):
    filenames = glob.glob(raw_data_path)
    print "found ", len(filenames), " number of files"
    data_dict = {}
    for file in filenames:
        print "processing file : ", file
        process_file(file, data_dict)
    return data_dict

if __name__ == '__main__' :
    raw_data_path = sys.argv[1] #"/home/thejus/workspace/learn-cascading/data/sessionExplodeWithAttributes-201708.MOB.smaller" + "/part-*"
    output_path = sys.argv[2] #"/home/thejus/workspace/learn-cascading/data/sessionExplodeWithAttributes-201708.MOB.smaller.eda"
    datas = prepare_data(raw_data_path)

    with open(output_path, mode="w+b") as writer :
        flush_counter = 0
        for key in datas:
            val = datas[key]
            row = [key] + val + [float(val[0])/val[1] if val[1] != 0 else val[0]]
            writer.write("\t".join(map(str, row)) + '\n')
        writer.flush()
        writer.close()