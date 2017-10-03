import pandas as pd
import glob
import json
from operator import itemgetter

data_path = "/home/thejus/workspace/learn-cascading/data/sessionExplode-201708.MOB" + "/part-0000*"

filenames = glob.glob(data_path)

list_ = []
for file_ in filenames:
    df = pd.read_csv(file_, sep="\t")
    list_.append(df)
df = pd.concat(list_)

positiveCounts = {}
negativeCounts = {}

def add_to_dict(countDict, id) :
    if id not in countDict :
        countDict[id] = 1
    else :
        countDict[id] = countDict[id] + 1

df["positiveProducts"].apply(lambda x : add_to_dict(positiveCounts, x))
df["positiveProducts"].apply(lambda x : add_to_dict(negativeCounts, x))
df["negativeProducts"].apply(lambda x : map(lambda y : add_to_dict(negativeCounts, y), json.loads(x)))

print positiveCounts
print negativeCounts

ctr = []
ctrDict = {}

for id in negativeCounts :
    if id in positiveCounts :
        positiveCount = positiveCounts[id]
    else :
        positiveCount = 0
    negativeCount = negativeCounts[id]
    ctrVal = float(positiveCount) / float(negativeCount)
    ctr.append([id, ctrVal])
    ctrDict[id] = ctrVal

# productScore = sorted(ctr, key=itemgetter(1), reverse=True)
# for id in productScore:
#     print id[0] + " " + str(id[1])

# print df.head()



# df["positiveProductsInt"] = df["positiveProducts"].apply()

