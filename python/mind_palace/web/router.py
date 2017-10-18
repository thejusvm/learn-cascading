from flask import Flask, jsonify
from mind_palace.product_ranker.training.run_model import Scorer
import json
import numpy

app = Flask(__name__)

@app.route("/")
def hello():
    return "Welcome to Mind Palace!"


model_path = "mind_palace/product_ranker/training/saved_models/run.20171018-18-01-32"
model_path = "mind_palace/product_ranker/training/saved_models/run.20171018-19-40-57"
attributes_path = "/home/thejus/workspace/learn-cascading/data/product-attributes.MOB/part-*"
nn_version = 'counter-3000'
scorer = Scorer(model_path, nn_version, attributes_path)

def decimal_default(obj):
    if isinstance(obj, numpy.float32):
        return float(obj)
    raise TypeError

@app.route("/score")
def score():
    products_to_rank = ["MOBEQ98MNXHY4RU9", "MOBES9G5SJHYT9QX", "MOBEQ98TABTWXGTD", "MOBEWN63JHHEXPTD", "MOBEXNP9FJ9K5K53", "MOBEX9WXUSZVYHET", "MOBET6RH4XSXKM7D", "MOBEQ98TWG8X4HH3", "MOBECCA5FHQD43KA", "MOBEWN63NBDSMVPG", "MOBEU9WRGVXDPBSF", "MOBEU9WRZFFUYAXJ", "MOBEU9WRZHRVWXTK", "MOBEMK62PN2HU7EE", "MOBEX9WXZCZHWXUZ", "MOBEWXHUSBXVJ7NZ", "MOBET6RHXVZBJFNT", "MOBESDYMGHC37GCS", "MOBEN2YYKU9386TQ", "MOBEN2YYQH8PSYXG", "MOBECCA5Y5HBYR3Q", "MOBECCA5SMRSKCNY", "MOBEG4XWMBDGZVEX", "MOBEG4XWDK4WBGNU", "MOBEV7YDBCAFG3ZH", "MOBEN2XYK8WFEGM8", "MOBEJFHUFVAJ45YA", "MOBEJFHUGPWTZFQJ", "MOBEV7YD3CFBTENW", "MOBEVKFTCFFU2FE7", "MOBETM9FZWW5UEZG", "MOBEUF42PGDRYCQA", "MOBEUF424KXTP9CT", "MOBEUF42VHXZSQV7", "MOBEQ98T82CYVHGZ", "MOBETM93F7DGJNN5", "MOBETMH3ZYNDPVVC", "MOBEU35JUQMQQHWK", "MOBEU35JAZKVWRPV", "MOBESDYCQD3FJCFW", "MOBEZEMYH7FQBGBQ", "MOBEZENFZBPW8UMF", "MOBEKGT2HGDGADFW", "MOBEMK62JSRHU85T", "MOBEZPVEGADXHMHT", "MOBEZPVENHEVMQDZ", "MOBEQRYTXZXC8FZZ", "MOBETM93HMBGUQKH", "MOBEXHHKDHSA9UZC", "MOBECCA5BJUVUGNP", "MOBEHZTGXSGG2GRX", "MOBEK4ABQFH3SSP7", "MOBE9TGVE7ZBRAEN"]
    clicked_products = ["MOBETMH3ZYNDPVVC"]
    resp = scorer.score(products_to_rank, clicked_products)
    return jsonify(resp)


