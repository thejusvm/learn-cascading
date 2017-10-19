from flask import Flask, jsonify , request
from mind_palace.product_ranker.training.run_model import Scorer
import json
import numpy
import mind_palace.product_ranker.constants as CONST

app = Flask(__name__)

@app.route("/")
def hello():
    return "Welcome to Mind Palace!"

model_path = "mind_palace/product_ranker/training/saved_models/run.20171019-11-05-06"
attributes_path = "/home/thejus/workspace/learn-cascading/data/product-attributes.MOB/part-*"
nn_version = 'counter-13000'
scorer = Scorer(model_path, nn_version, attributes_path)
products_to_rank = ["MOBEQ98MNXHY4RU9", "MOBES9G5SJHYT9QX", "MOBEQ98TABTWXGTD", "MOBEWN63JHHEXPTD", "MOBEXNP9FJ9K5K53", "MOBEX9WXUSZVYHET", "MOBET6RH4XSXKM7D", "MOBEQ98TWG8X4HH3", "MOBECCA5FHQD43KA", "MOBEWN63NBDSMVPG", "MOBEU9WRGVXDPBSF", "MOBEU9WRZFFUYAXJ", "MOBEU9WRZHRVWXTK", "MOBEMK62PN2HU7EE", "MOBEX9WXZCZHWXUZ", "MOBEWXHUSBXVJ7NZ", "MOBET6RHXVZBJFNT", "MOBESDYMGHC37GCS", "MOBEN2YYKU9386TQ", "MOBEN2YYQH8PSYXG", "MOBECCA5Y5HBYR3Q", "MOBECCA5SMRSKCNY", "MOBEG4XWMBDGZVEX", "MOBEG4XWDK4WBGNU", "MOBEV7YDBCAFG3ZH", "MOBEN2XYK8WFEGM8", "MOBEJFHUFVAJ45YA", "MOBEJFHUGPWTZFQJ", "MOBEV7YD3CFBTENW", "MOBEVKFTCFFU2FE7", "MOBETM9FZWW5UEZG", "MOBEUF42PGDRYCQA", "MOBEUF424KXTP9CT", "MOBEUF42VHXZSQV7", "MOBEQ98T82CYVHGZ", "MOBETM93F7DGJNN5", "MOBETMH3ZYNDPVVC", "MOBEU35JUQMQQHWK", "MOBEU35JAZKVWRPV", "MOBESDYCQD3FJCFW", "MOBEZEMYH7FQBGBQ", "MOBEZENFZBPW8UMF", "MOBEKGT2HGDGADFW", "MOBEMK62JSRHU85T", "MOBEZPVEGADXHMHT", "MOBEZPVENHEVMQDZ", "MOBEQRYTXZXC8FZZ", "MOBETM93HMBGUQKH", "MOBEXHHKDHSA9UZC", "MOBECCA5BJUVUGNP", "MOBEHZTGXSGG2GRX", "MOBEK4ABQFH3SSP7", "MOBE9TGVE7ZBRAEN"]

product_data = "{\"REQUEST\":{\"resource\":\"/sherlock/stores/tyy/4io/debug\",\"products\":{\"count\":\"100\"},\"es\":{\"fl\":\"title:mField(title_lc)\"}},\"STATUS\":{\"code\":200},\"RESPONSE\":{\"num-found\":7307,\"products\":[{\"item_id\":\"ITMEQG86FJYZKDQ8\",\"title\":\"[\\\"4 gb ram\\\",\\\"redmi note 4 gold 64 gb \\\"]\",\"listing_id\":\"LSTMOBEQ98MNXHY4RU9HWK2LC\",\"product_id\":\"MOBEQ98MNXHY4RU9\",\"siblings\":null,\"attributes\":{\"listing_price\":12999}},{\"item_id\":\"ITMEUYD8YM5MWTGH\",\"title\":\"[\\\"4 gb ram\\\",\\\"oppo f3 plus gold 64 gb \\\"]\",\"listing_id\":\"LSTMOBES9G5SJHYT9QXIH4ZZC\",\"product_id\":\"MOBES9G5SJHYT9QX\",\"siblings\":null,\"attributes\":{\"listing_price\":24990}},{\"item_id\":\"ITMEQE48766XQZB7\",\"title\":\"[\\\"4 gb ram\\\",\\\"redmi note 4 black 64 gb \\\"]\",\"listing_id\":\"LSTMOBEQ98TABTWXGTDNZ53XZ\",\"product_id\":\"MOBEQ98TABTWXGTD\",\"siblings\":null,\"attributes\":{\"listing_price\":12999}},{\"item_id\":\"ITMEX9KPGKXMGPZC\",\"title\":\"[\\\"3 gb ram\\\",\\\"lenovo k8 plus venom black 32 gb \\\"]\",\"listing_id\":\"LSTMOBEWN63JHHEXPTD6ZB4XI\",\"product_id\":\"MOBEWN63JHHEXPTD\",\"siblings\":null,\"attributes\":{\"listing_price\":10999}},{\"item_id\":\"ITMEXNP94KCVDNQW\",\"title\":\"[\\\"4 gb ram\\\",\\\"honor 9i prestige gold 64 gb \\\"]\",\"listing_id\":\"LSTMOBEXNP9FJ9K5K53WZTGBE\",\"product_id\":\"MOBEXNP9FJ9K5K53\",\"siblings\":null,\"attributes\":{\"listing_price\":17999}},{\"item_id\":\"ITMEXNSRTZHBBNEG\",\"title\":\"[\\\"4 gb ram\\\",\\\"mi a1 black 64 gb \\\"]\",\"listing_id\":\"LSTMOBEX9WXUSZVYHETFSTZ7W\",\"product_id\":\"MOBEX9WXUSZVYHET\",\"siblings\":null,\"attributes\":{\"listing_price\":14999}},{\"item_id\":\"ITMET6RHMFZDTRHT\",\"title\":\"[\\\"3 gb ram\\\",\\\"samsung galaxy on nxt gold 64 gb \\\"]\",\"listing_id\":\"LSTMOBET6RH4XSXKM7D5I3RAP\",\"product_id\":\"MOBET6RH4XSXKM7D\",\"siblings\":null,\"attributes\":{\"listing_price\":14900}},{\"item_id\":\"ITMEQG88CNHYYUAF\",\"title\":\"[\\\"4 gb ram\\\",\\\"redmi note 4 dark grey 64 gb \\\"]\",\"listing_id\":\"LSTMOBEQ98TWG8X4HH3K9GEMH\",\"product_id\":\"MOBEQ98TWG8X4HH3\",\"siblings\":null,\"attributes\":{\"listing_price\":12999}},{\"item_id\":\"ITMEDHX3UY3QSFKS\",\"title\":\"[\\\"1 5 gb ram\\\",\\\"samsung galaxy on5 gold 8 gb \\\"]\",\"listing_id\":\"LSTMOBECCA5FHQD43KAK2QMJD\",\"product_id\":\"MOBECCA5FHQD43KA\",\"siblings\":null,\"attributes\":{\"listing_price\":6990}},{\"item_id\":\"ITMEX9KPQJFGWBKQ\",\"title\":\"[\\\"3 gb ram\\\",\\\"lenovo k8 plus fine gold 32 gb \\\"]\",\"listing_id\":\"LSTMOBEWN63NBDSMVPGHVUHOP\",\"product_id\":\"MOBEWN63NBDSMVPG\",\"siblings\":null,\"attributes\":{\"listing_price\":10999}},{\"item_id\":\"ITMEUYD6NHZYCMAA\",\"title\":\"[\\\"2 gb ram\\\",\\\"moto c plus fine gold 16 gb \\\"]\",\"listing_id\":\"LSTMOBEU9WRZHRVWXTKGQWIY8\",\"product_id\":\"MOBEU9WRZHRVWXTK\",\"siblings\":null,\"attributes\":{\"listing_price\":6999}},{\"item_id\":\"ITMEUYD6NHZYCMAA\",\"title\":\"[\\\"2 gb ram\\\",\\\"moto c plus pearl white 16 gb \\\"]\",\"listing_id\":\"LSTMOBEU9WRGVXDPBSF4CRZQO\",\"product_id\":\"MOBEU9WRGVXDPBSF\",\"siblings\":null,\"attributes\":{\"listing_price\":6999}},{\"item_id\":\"ITMEUYD6NHZYCMAA\",\"title\":\"[\\\"2 gb ram\\\",\\\"moto c plus starry black 16 gb \\\"]\",\"listing_id\":\"LSTMOBEU9WRZFFUYAXJWMIJBH\",\"product_id\":\"MOBEU9WRZFFUYAXJ\",\"siblings\":null,\"attributes\":{\"listing_price\":6999}},{\"item_id\":\"ITMEN6DAFTCQWZEG\",\"title\":\"[\\\"apple iphone 7 black 32 gb \\\"]\",\"listing_id\":\"LSTMOBEMK62PN2HU7EEINTGNU\",\"product_id\":\"MOBEMK62PN2HU7EE\",\"siblings\":null,\"attributes\":{\"listing_price\":46999}},{\"item_id\":\"ITMEXNSR2CWZBFHT\",\"title\":\"[\\\"4 gb ram\\\",\\\"mi a1 gold 64 gb \\\"]\",\"listing_id\":\"LSTMOBEX9WXZCZHWXUZELHO8V\",\"product_id\":\"MOBEX9WXZCZHWXUZ\",\"siblings\":null,\"attributes\":{\"listing_price\":14999}},{\"item_id\":\"ITMEWXHUUFBZCHRN\",\"title\":\"[\\\"apple iphone 6 gold 32 gb \\\"]\",\"listing_id\":\"LSTMOBEWXHUSBXVJ7NZVRFXNL\",\"product_id\":\"MOBEWXHUSBXVJ7NZ\",\"siblings\":null,\"attributes\":{\"listing_price\":25999}},{\"item_id\":\"ITMET6RHZEARBRGR\",\"title\":\"[\\\"3 gb ram\\\",\\\"samsung galaxy on nxt black 64 gb \\\"]\",\"listing_id\":\"LSTMOBET6RHXVZBJFNTIMB3LD\",\"product_id\":\"MOBET6RHXVZBJFNT\",\"siblings\":null,\"attributes\":{\"listing_price\":14900}},{\"item_id\":\"ITMETENDHGZ8GUMC\",\"title\":\"[\\\"3 gb ram\\\",\\\"panasonic eluga ray x champagne gold 32 gb \\\"]\",\"listing_id\":\"LSTMOBESDYMGHC37GCSBML8DV\",\"product_id\":\"MOBESDYMGHC37GCS\",\"siblings\":null,\"attributes\":{\"listing_price\":8999}},{\"item_id\":\"ITMEN2YYJFZPSPYG\",\"title\":\"[\\\"apple iphone 6s space grey 32 gb \\\"]\",\"listing_id\":\"LSTMOBEN2YYKU9386TQCCN1OM\",\"product_id\":\"MOBEN2YYKU9386TQ\",\"siblings\":null,\"attributes\":{\"listing_price\":38999}},{\"item_id\":\"ITMEN2YYNT6BZ3GG\",\"title\":\"[\\\"apple iphone 6s gold 32 gb \\\"]\",\"listing_id\":\"LSTMOBEN2YYQH8PSYXG4TGFKX\",\"product_id\":\"MOBEN2YYQH8PSYXG\",\"siblings\":null,\"attributes\":{\"listing_price\":38999}},{\"item_id\":\"ITMEUYD9C5ADB58R\",\"title\":\"[\\\"1 5 gb ram\\\",\\\"samsung galaxy on7 gold 8 gb \\\"]\",\"listing_id\":\"LSTMOBECCA5Y5HBYR3QPSGMZM\",\"product_id\":\"MOBECCA5Y5HBYR3Q\",\"siblings\":null,\"attributes\":{\"listing_price\":7990}},{\"item_id\":\"ITMEUYD9C5ADB58R\",\"title\":\"[\\\"1 5 gb ram\\\",\\\"samsung galaxy on7 black 8 gb \\\"]\",\"listing_id\":\"LSTMOBECCA5SMRSKCNYKCMXZU\",\"product_id\":\"MOBECCA5SMRSKCNY\",\"siblings\":null,\"attributes\":{\"listing_price\":7990}},{\"item_id\":\"ITMEUYD8SXDSSMDV\",\"title\":\"[\\\"2 gb ram\\\",\\\"samsung galaxy j7 6 new 2016 edition black 16 gb \\\"]\",\"listing_id\":\"LSTMOBEG4XWMBDGZVEX15B0YO\",\"product_id\":\"MOBEG4XWMBDGZVEX\",\"siblings\":null,\"attributes\":{\"listing_price\":13800}},{\"item_id\":\"ITMEUYD8SXDSSMDV\",\"title\":\"[\\\"2 gb ram\\\",\\\"samsung galaxy j7 6 new 2016 edition gold 16 gb \\\"]\",\"listing_id\":\"LSTMOBEG4XWDK4WBGNUARSCAX\",\"product_id\":\"MOBEG4XWDK4WBGNU\",\"siblings\":null,\"attributes\":{\"listing_price\":13800}},{\"item_id\":\"ITMEVAKGF8GMVYTE\",\"title\":\"[\\\"4 gb ram\\\",\\\"samsung galaxy on max gold 32 gb \\\"]\",\"listing_id\":\"LSTMOBEV7YDBCAFG3ZH5FUOXG\",\"product_id\":\"MOBEV7YDBCAFG3ZH\",\"siblings\":null,\"attributes\":{\"listing_price\":16900}},{\"item_id\":\"ITMEN2YYMNFCRXSZ\",\"title\":\"[\\\"apple iphone 6s rose gold 32 gb \\\"]\",\"listing_id\":\"LSTMOBEN2XYK8WFEGM8QJW5XA\",\"product_id\":\"MOBEN2XYK8WFEGM8\",\"siblings\":null,\"attributes\":{\"listing_price\":38999}},{\"item_id\":\"ITMEUYD8RZZNZPCG\",\"title\":\"[\\\"4 gb ram\\\",\\\"leeco le max2 rose gold 32 gb \\\"]\",\"listing_id\":\"LSTMOBEJFHUFVAJ45YASX7KYX\",\"product_id\":\"MOBEJFHUFVAJ45YA\",\"siblings\":null,\"attributes\":{\"listing_price\":14999}},{\"item_id\":\"ITMEUYD8RZZNZPCG\",\"title\":\"[\\\"4 gb ram\\\",\\\"leeco le max2 grey 32 gb \\\"]\",\"listing_id\":\"LSTMOBEJFHUGPWTZFQJJKV8WC\",\"product_id\":\"MOBEJFHUGPWTZFQJ\",\"siblings\":null,\"attributes\":{\"listing_price\":14999}},{\"item_id\":\"ITMEVMRQD5N5NCFA\",\"title\":\"[\\\"4 gb ram\\\",\\\"samsung galaxy on max black 32 gb \\\"]\",\"listing_id\":\"LSTMOBEV7YD3CFBTENWGYFVDR\",\"product_id\":\"MOBEV7YD3CFBTENW\",\"siblings\":null,\"attributes\":{\"listing_price\":16900}},{\"item_id\":\"ITMEVKFTUFR4D5E2\",\"title\":\"[\\\"4 gb ram\\\",\\\"mi max 2 black 64 gb \\\"]\",\"listing_id\":\"LSTMOBEVKFTCFFU2FE7DY06NZ\",\"product_id\":\"MOBEVKFTCFFU2FE7\",\"siblings\":null,\"attributes\":{\"listing_price\":16999}},{\"item_id\":\"ITMETM9FYPDA5RFS\",\"title\":\"[\\\"apple iphone se space grey 32 gb \\\"]\",\"listing_id\":\"LSTMOBETM9FZWW5UEZGJKPO31\",\"product_id\":\"MOBETM9FZWW5UEZG\",\"siblings\":null,\"attributes\":{\"listing_price\":22999}},{\"item_id\":\"ITMEUYD9KZVZMYX6\",\"title\":\"[\\\"3 gb ram\\\",\\\"infinix hot 4 pro magic gold 16 gb \\\"]\",\"listing_id\":\"LSTMOBEUF42PGDRYCQAXSRZKD\",\"product_id\":\"MOBEUF42PGDRYCQA\",\"siblings\":null,\"attributes\":{\"listing_price\":7499}},{\"item_id\":\"ITMEUYD9KZVZMYX6\",\"title\":\"[\\\"3 gb ram\\\",\\\"infinix hot 4 pro bordeaux red 16 gb \\\"]\",\"listing_id\":\"LSTMOBEUF424KXTP9CTEDER40\",\"product_id\":\"MOBEUF424KXTP9CT\",\"siblings\":null,\"attributes\":{\"listing_price\":7499}},{\"item_id\":\"ITMEUYD9KZVZMYX6\",\"title\":\"[\\\"3 gb ram\\\",\\\"infinix hot 4 pro quartz black 16 gb \\\"]\",\"listing_id\":\"LSTMOBEUF42VHXZSQV7YTF2IS\",\"product_id\":\"MOBEUF42VHXZSQV7\",\"siblings\":null,\"attributes\":{\"listing_price\":7499}},{\"item_id\":\"ITMEQGS9GWRFWPF2\",\"title\":\"[\\\"3 gb ram\\\",\\\"redmi note 4 gold 32 gb \\\"]\",\"listing_id\":\"LSTMOBEQ98T82CYVHGZAFKMUX\",\"product_id\":\"MOBEQ98T82CYVHGZ\",\"siblings\":null,\"attributes\":{\"listing_price\":10999}},{\"item_id\":\"ITMETM9FYY3N7ZFZ\",\"title\":\"[\\\"apple iphone se rose gold 32 gb \\\"]\",\"listing_id\":\"LSTMOBETM93F7DGJNN5VCOWGW\",\"product_id\":\"MOBETM93F7DGJNN5\",\"siblings\":null,\"attributes\":{\"listing_price\":22999}},{\"item_id\":\"ITMETMH3HFHNXTCJ\",\"title\":\"[\\\"apple iphone 6 space grey 32 gb \\\"]\",\"listing_id\":\"LSTMOBETMH3ZYNDPVVC7BAC1S\",\"product_id\":\"MOBETMH3ZYNDPVVC\",\"siblings\":null,\"attributes\":{\"listing_price\":25999}},{\"item_id\":\"ITMEU8GY94GHHNXQ\",\"title\":\"[\\\"2 gb ram\\\",\\\"samsung galaxy j3 pro black 16 gb \\\"]\",\"listing_id\":\"LSTMOBEU35JUQMQQHWK1HGEPZ\",\"product_id\":\"MOBEU35JUQMQQHWK\",\"siblings\":null,\"attributes\":{\"listing_price\":7990}},{\"item_id\":\"ITMEU35JFRYETGEW\",\"title\":\"[\\\"2 gb ram\\\",\\\"samsung galaxy j3 pro gold 16 gb \\\"]\",\"listing_id\":\"LSTMOBEU35JAZKVWRPVUXDI0L\",\"product_id\":\"MOBEU35JAZKVWRPV\",\"siblings\":null,\"attributes\":{\"listing_price\":7990}},{\"item_id\":\"ITMET968FT3G9EBZ\",\"title\":\"[\\\"3 gb ram\\\",\\\"panasonic eluga ray x space grey 32 gb \\\"]\",\"listing_id\":\"LSTMOBESDYCQD3FJCFWFJR1GO\",\"product_id\":\"MOBESDYCQD3FJCFW\",\"siblings\":null,\"attributes\":{\"listing_price\":8999}},{\"item_id\":\"ITMEZENFHM4MVPTW\",\"title\":\"[\\\"3 gb ram\\\",\\\"lenovo k6 power gold 32 gb \\\"]\",\"listing_id\":\"LSTMOBEZEMYH7FQBGBQRHVU0S\",\"product_id\":\"MOBEZEMYH7FQBGBQ\",\"siblings\":null,\"attributes\":{\"listing_price\":9999}},{\"item_id\":\"ITMEZENFHM4MVPTW\",\"title\":\"[\\\"3 gb ram\\\",\\\"lenovo k6 power grey 32 gb \\\"]\",\"listing_id\":\"LSTMOBEZENFZBPW8UMF7P8NY0\",\"product_id\":\"MOBEZENFZBPW8UMF\",\"siblings\":null,\"attributes\":{\"listing_price\":9999}},{\"item_id\":\"ITMEKGT2FBYWQGCV\",\"title\":\"[\\\"2 gb ram\\\",\\\"moto e3 power black 16 gb \\\"]\",\"listing_id\":\"LSTMOBEKGT2HGDGADFWJV9YSF\",\"product_id\":\"MOBEKGT2HGDGADFW\",\"siblings\":null,\"attributes\":{\"listing_price\":6999}},{\"item_id\":\"ITMEN6DAPSVXANRK\",\"title\":\"[\\\"apple iphone 7 rose gold 32 gb \\\"]\",\"listing_id\":\"LSTMOBEMK62JSRHU85TIU9DKW\",\"product_id\":\"MOBEMK62JSRHU85T\",\"siblings\":null,\"attributes\":{\"listing_price\":46999}},{\"item_id\":\"ITMEP4BHWQTFMPDD\",\"title\":\"[\\\"3 gb ram\\\",\\\"lenovo phab 2 champagne gold 32 gb \\\"]\",\"listing_id\":\"LSTMOBEZPVEGADXHMHTRHVPVC\",\"product_id\":\"MOBEZPVEGADXHMHT\",\"siblings\":null,\"attributes\":{\"listing_price\":10999}},{\"item_id\":\"ITMEP4BHWQTFMPDD\",\"title\":\"[\\\"3 gb ram\\\",\\\"lenovo phab 2 gunmetal gray 32 gb \\\"]\",\"listing_id\":\"LSTMOBEZPVENHEVMQDZISYRRI\",\"product_id\":\"MOBEZPVENHEVMQDZ\",\"siblings\":null,\"attributes\":{\"listing_price\":10999}},{\"item_id\":\"ITMEQRYT6NP8AAZK\",\"title\":\"[\\\"3 gb ram\\\",\\\"asus zenfone 3s max gold 32 gb \\\"]\",\"listing_id\":\"LSTMOBEQRYTXZXC8FZZ8AHKTJ\",\"product_id\":\"MOBEQRYTXZXC8FZZ\",\"siblings\":null,\"attributes\":{\"listing_price\":12999}},{\"item_id\":\"ITMETM9FRZQCDRCM\",\"title\":\"[\\\"apple iphone se gold 32 gb \\\"]\",\"listing_id\":\"LSTMOBETM93HMBGUQKHZVFD5O\",\"product_id\":\"MOBETM93HMBGUQKH\",\"siblings\":null,\"attributes\":{\"listing_price\":22999}},{\"item_id\":\"ITMEXHHKWMH2NRZ4\",\"title\":\"[\\\"3 gb ram\\\",\\\"panasonic eluga ray 700 marine blue 32 gb \\\"]\",\"listing_id\":\"LSTMOBEXHHKDHSA9UZCN8IO0S\",\"product_id\":\"MOBEXHHKDHSA9UZC\",\"siblings\":null,\"attributes\":{\"listing_price\":9999}},{\"item_id\":\"ITMEKSZMSQGPGYGY\",\"title\":\"[\\\"1 5 gb ram\\\",\\\"samsung galaxy on5 black 8 gb \\\"]\",\"listing_id\":\"LSTMOBECCA5BJUVUGNPCHWQWJ\",\"product_id\":\"MOBECCA5BJUVUGNP\",\"siblings\":null,\"attributes\":{\"listing_price\":6990}},{\"item_id\":\"ITMEN6DAF99NHHJZ\",\"title\":\"[\\\"apple iphone 7 gold 32 gb \\\"]\",\"listing_id\":\"LSTMOBEMK62HZHC6TFUHGVQFY\",\"product_id\":\"MOBEMK62HZHC6TFU\",\"siblings\":null,\"attributes\":{\"listing_price\":46999}},{\"item_id\":\"ITMEWF3UTFDFH3NF\",\"title\":\"[\\\"3 gb ram\\\",\\\"infinix note 4 champagne gold 32 gb \\\"]\",\"listing_id\":\"LSTMOBEWF3UK5RQZJRAOKPDKP\",\"product_id\":\"MOBEWF3UK5RQZJRA\",\"siblings\":null,\"attributes\":{\"listing_price\":8999}},{\"item_id\":\"ITMEWF3UTFDFH3NF\",\"title\":\"[\\\"3 gb ram\\\",\\\"infinix note 4 ice blue 32 gb \\\"]\",\"listing_id\":\"LSTMOBEWF3UCTQT3M6GJ3ADXL\",\"product_id\":\"MOBEWF3UCTQT3M6G\",\"siblings\":null,\"attributes\":{\"listing_price\":8999}},{\"item_id\":\"ITMEWF3UTFDFH3NF\",\"title\":\"[\\\"3 gb ram\\\",\\\"infinix note 4 milan black 32 gb \\\"]\",\"listing_id\":\"LSTMOBEWF3UPGQHFJXG2BIN4U\",\"product_id\":\"MOBEWF3UPGQHFJXG\",\"siblings\":null,\"attributes\":{\"listing_price\":8999}},{\"item_id\":\"ITMEX9KPKCGF7XF7\",\"title\":\"[\\\"4 gb ram\\\",\\\"lenovo k8 plus venom black 32 gb \\\"]\",\"listing_id\":\"LSTMOBEWN63AUNVTJGUXXW4K5\",\"product_id\":\"MOBEWN63AUNVTJGU\",\"siblings\":null,\"attributes\":{\"listing_price\":11999}},{\"item_id\":\"ITMES2ZJVWFNCXXR\",\"title\":\"[\\\"4 gb ram\\\",\\\"moto g5 plus lunar grey 32 gb \\\"]\",\"listing_id\":\"LSTMOBEQHMGMAUXS5BFN9JAPR\",\"product_id\":\"MOBEQHMGMAUXS5BF\",\"siblings\":null,\"attributes\":{\"listing_price\":13999}},{\"item_id\":\"ITMES2ZJVWFNCXXR\",\"title\":\"[\\\"4 gb ram\\\",\\\"moto g5 plus fine gold 32 gb \\\"]\",\"listing_id\":\"LSTMOBEQHMGED7F9CZ2KTTROW\",\"product_id\":\"MOBEQHMGED7F9CZ2\",\"siblings\":null,\"attributes\":{\"listing_price\":12999}},{\"item_id\":\"ITMEXD57FQMJZK7H\",\"title\":\"[\\\"3 gb ram\\\",\\\"ivoomi me3s champagne gold 32 gb \\\"]\",\"listing_id\":\"LSTMOBEXCTYPVGYCJHPVCUTR7\",\"product_id\":\"MOBEXCTYPVGYCJHP\",\"siblings\":null,\"attributes\":{\"listing_price\":6499}},{\"item_id\":\"ITMEQRYTX9ERR2HM\",\"title\":\"[\\\"3 gb ram\\\",\\\"asus zenfone 3s max black 32 gb \\\"]\",\"listing_id\":\"LSTMOBEQRYTF3GWU6FVO9EBTJ\",\"product_id\":\"MOBEQRYTF3GWU6FV\",\"siblings\":null,\"attributes\":{\"listing_price\":14999}},{\"item_id\":\"ITMEXGARG4FFX8YB\",\"title\":\"[\\\"4 gb ram\\\",\\\"zte blade a2 plus golden 32 gb \\\"]\",\"listing_id\":\"LSTMOBEXGARSBDDHZGF06BVPF\",\"product_id\":\"MOBEXGARSBDDHZGF\",\"siblings\":null,\"attributes\":{\"listing_price\":8999}},{\"item_id\":\"ITMEXGARG4FFX8YB\",\"title\":\"[\\\"4 gb ram\\\",\\\"zte blade a2 plus grey 32 gb \\\"]\",\"listing_id\":\"LSTMOBEXGARA3TCHNCATOSPAI\",\"product_id\":\"MOBEXGARA3TCHNCA\",\"siblings\":null,\"attributes\":{\"listing_price\":8999}},{\"item_id\":\"ITMEU8GYHSUYREGA\",\"title\":\"[\\\"2 gb ram\\\",\\\"samsung galaxy j3 pro white 16 gb \\\"]\",\"listing_id\":\"LSTMOBEU35JRJWCZR83UEREKI\",\"product_id\":\"MOBEU35JRJWCZR83\",\"siblings\":null,\"attributes\":{\"listing_price\":7990}},{\"item_id\":\"ITMEXPYEUXAHPQUM\",\"title\":\"[\\\"3 gb ram\\\",\\\"asus zenfone 4 selfie black 32 gb \\\"]\",\"listing_id\":\"LSTMOBEWPZFNAVKRMBR67S9JK\",\"product_id\":\"MOBEWPZFNAVKRMBR\",\"siblings\":null,\"attributes\":{\"listing_price\":9999}},{\"item_id\":\"ITMES3FCRFG5AUNP\",\"title\":\"[\\\"3 gb ram\\\",\\\"asus zenfone selfie silver 16 gb \\\"]\",\"listing_id\":\"LSTMOBES3FCHDHGTYYNSFVTCD\",\"product_id\":\"MOBES3FCHDHGTYYN\",\"siblings\":null,\"attributes\":{\"listing_price\":7999}},{\"item_id\":\"ITMEZENFGHDDRFMC\",\"title\":\"[\\\"4 gb ram\\\",\\\"lenovo k6 power gold 32 gb \\\"]\",\"listing_id\":\"LSTMOBEZENFSZGTQGWFUR1LY1\",\"product_id\":\"MOBEZENFSZGTQGWF\",\"siblings\":null,\"attributes\":{\"listing_price\":10999}},{\"item_id\":\"ITMEN6DAWZ2G5DRY\",\"title\":\"[\\\"apple iphone 7 silver 32 gb \\\"]\",\"listing_id\":\"LSTMOBEMK62HSQDSGGFHXPMBD\",\"product_id\":\"MOBEMK62HSQDSGGF\",\"siblings\":null,\"attributes\":{\"listing_price\":46999}},{\"item_id\":\"ITMETRSR4TJX94B4\",\"title\":\"[\\\"2 gb ram\\\",\\\"panasonic p85 grey 16 gb \\\"]\",\"listing_id\":\"LSTMOBETRSG3ZJCYCEDEWR8Q7\",\"product_id\":\"MOBETRSG3ZJCYCED\",\"siblings\":null,\"attributes\":{\"listing_price\":6499}},{\"item_id\":\"ITMEXUZYWKXGYWK7\",\"title\":\"[\\\"2 gb ram\\\",\\\"sansui horizon 2s rose gold rose 16 gb \\\"]\",\"listing_id\":\"LSTMOBEXG5R4M9VPRH8JG8DXM\",\"product_id\":\"MOBEXG5R4M9VPRH8\",\"siblings\":null,\"attributes\":{\"listing_price\":4999}},{\"item_id\":\"ITMERHQ8UHTEHUKG\",\"title\":\"[\\\"1 gb ram\\\",\\\"xolo era 1x 4g with volte black and gun metal 8 gb \\\"]\",\"listing_id\":\"LSTMOBEHMEKGCZCGMB8DCWHIY\",\"product_id\":\"MOBEHMEKGCZCGMB8\",\"siblings\":null,\"attributes\":{\"listing_price\":4499}},{\"item_id\":\"ITMERHQ8UHTEHUKG\",\"title\":\"[\\\"1 gb ram\\\",\\\"xolo era 1x 4g with volte chocolate brown gold 8 gb \\\"]\",\"listing_id\":\"LSTMOBEHMEKYNNAMTY9NOZ278\",\"product_id\":\"MOBEHMEKYNNAMTY9\",\"siblings\":null,\"attributes\":{\"listing_price\":4499}},{\"item_id\":\"ITMEUYDA4QGQETC6\",\"title\":\"[\\\"4 gb ram\\\",\\\"samsung galaxy s7 silver titanium 32 gb \\\"]\",\"listing_id\":\"LSTMOBEGFZPKXDMDBJ28J6IRC\",\"product_id\":\"MOBEGFZPKXDMDBJ2\",\"siblings\":null,\"attributes\":{\"listing_price\":39400}},{\"item_id\":\"ITMEUYDA4QGQETC6\",\"title\":\"[\\\"4 gb ram\\\",\\\"samsung galaxy s7 gold platinum 32 gb \\\"]\",\"listing_id\":\"LSTMOBEGFZPWJHYT7NXKNHWSQ\",\"product_id\":\"MOBEGFZPWJHYT7NX\",\"siblings\":null,\"attributes\":{\"listing_price\":39400}},{\"item_id\":\"ITMEUYDA4QGQETC6\",\"title\":\"[\\\"4 gb ram\\\",\\\"samsung galaxy s7 black onyx 32 gb \\\"]\",\"listing_id\":\"LSTMOBEGFZPKG44NJFAEOHFLQ\",\"product_id\":\"MOBEGFZPKG44NJFA\",\"siblings\":null,\"attributes\":{\"listing_price\":39400}},{\"item_id\":\"ITMEXCZYDDZNVHD2\",\"title\":\"[\\\"4 gb ram\\\",\\\"honor 6x silver 64 gb \\\"]\",\"listing_id\":\"LSTMOBEWA9MCCHK9MRSVZCCVL\",\"product_id\":\"MOBEWA9MCCHK9MRS\",\"siblings\":null,\"attributes\":{\"listing_price\":13999}},{\"item_id\":\"ITMEXCZYDDZNVHD2\",\"title\":\"[\\\"3 gb ram\\\",\\\"honor 6x gold 32 gb \\\"]\",\"listing_id\":\"LSTMOBEWA9MNQZYQPHZXKJUHP\",\"product_id\":\"MOBEWA9MNQZYQPHZ\",\"siblings\":null,\"attributes\":{\"listing_price\":11999}},{\"item_id\":\"ITMEXCZYDDZNVHD2\",\"title\":\"[\\\"3 gb ram\\\",\\\"honor 6x grey 32 gb \\\"]\",\"listing_id\":\"LSTMOBEWA9MA5HCNFN6JQA20L\",\"product_id\":\"MOBEWA9MA5HCNFN6\",\"siblings\":null,\"attributes\":{\"listing_price\":11999}},{\"item_id\":\"ITMEXCZYDDZNVHD2\",\"title\":\"[\\\"3 gb ram\\\",\\\"honor 6x silver 32 gb \\\"]\",\"listing_id\":\"LSTMOBEWA9MFBMZD4Y7AHAYJL\",\"product_id\":\"MOBEWA9MFBMZD4Y7\",\"siblings\":null,\"attributes\":{\"listing_price\":11999}},{\"item_id\":\"ITMEXCZYDDZNVHD2\",\"title\":\"[\\\"4 gb ram\\\",\\\"honor 6x gold 64 gb \\\"]\",\"listing_id\":\"LSTMOBEWA9MRD9QSSTJKKCLFQ\",\"product_id\":\"MOBEWA9MRD9QSSTJ\",\"siblings\":null,\"attributes\":{\"listing_price\":13999}},{\"item_id\":\"ITMEX9KPPDXZNZ8G\",\"title\":\"[\\\"4 gb ram\\\",\\\"lenovo k8 plus fine gold 32 gb \\\"]\",\"listing_id\":\"LSTMOBEWN63TT7HQWGADHTSRG\",\"product_id\":\"MOBEWN63TT7HQWGA\",\"siblings\":null,\"attributes\":{\"listing_price\":11999}},{\"item_id\":\"ITMEUYDAKFEWGVK3\",\"title\":\"[\\\"4 gb ram\\\",\\\"honor 8\\\"]\",\"listing_id\":\"LSTMOBEM38ENZEBZFY5R0EVZ8\",\"product_id\":\"MOBEM38ENZEBZFY5\",\"siblings\":null,\"attributes\":{\"listing_price\":29999}},{\"item_id\":\"ITMEUYDAKFEWGVK3\",\"title\":\"[\\\"4 gb ram\\\",\\\"honor 8\\\"]\",\"listing_id\":\"LSTMOBEM38ESXH5WDFFUBFZCE\",\"product_id\":\"MOBEM38ESXH5WDFF\",\"siblings\":null,\"attributes\":{\"listing_price\":29999}},{\"item_id\":\"ITMEUYDAKFEWGVK3\",\"title\":\"[\\\"4 gb ram\\\",\\\"honor 8\\\"]\",\"listing_id\":\"LSTMOBEM38EGGJRGNHRHKDTFB\",\"product_id\":\"MOBEM38EGGJRGNHR\",\"siblings\":null,\"attributes\":{\"listing_price\":29999}},{\"item_id\":\"ITMEVKGW4HZ8XEDG\",\"title\":\"[\\\"3 gb ram\\\",\\\"panasonic p55 max champagne gold 16 gb \\\"]\",\"listing_id\":\"LSTMOBEVKGWPP9ZJEQGSL6I1S\",\"product_id\":\"MOBEVKGWPP9ZJEQG\",\"siblings\":null,\"attributes\":{\"listing_price\":8499}},{\"item_id\":\"ITMEX8H6AZNVHCHY\",\"title\":\"[\\\"3 gb ram\\\",\\\"asus zenfone 4 selfie gold 32 gb \\\"]\",\"listing_id\":\"LSTMOBEWPZFX6USKUW7LZYETY\",\"product_id\":\"MOBEWPZFX6USKUW7\",\"siblings\":null,\"attributes\":{\"listing_price\":9999}},{\"item_id\":\"ITMEQE4HTST9587B\",\"title\":\"[\\\"3 gb ram\\\",\\\"redmi note 4 black 32 gb \\\"]\",\"listing_id\":\"LSTMOBEQ98THNGR4FD5V3OUHB\",\"product_id\":\"MOBEQ98THNGR4FD5\",\"siblings\":null,\"attributes\":{\"listing_price\":10999}},{\"item_id\":\"ITMEJJ6K6ZCZ293D\",\"title\":\"[\\\"4 gb ram\\\",\\\"lenovo vibe k5 note silver 32 gb \\\"]\",\"listing_id\":\"LSTMOBEJJ6KHHPR2Q3JYEXPWG\",\"product_id\":\"MOBEJJ6KHHPR2Q3J\",\"siblings\":null,\"attributes\":{\"listing_price\":10499}},{\"item_id\":\"ITMEX8H6TPFGJQHH\",\"title\":\"[\\\"4 gb ram\\\",\\\"asus zenfone 4 selfie dual camera black 64 gb \\\"]\",\"listing_id\":\"LSTMOBEWPZFH8QQDT5VNOOOIN\",\"product_id\":\"MOBEWPZFH8QQDT5V\",\"siblings\":null,\"attributes\":{\"listing_price\":14999}},{\"item_id\":\"ITMEVPZ53SBHRREU\",\"title\":\"[\\\"1 gb ram\\\",\\\"intex aqua a4 black 8 gb \\\"]\",\"listing_id\":\"LSTMOBEVPZ5ZM8WFGYKUXKK6L\",\"product_id\":\"MOBEVPZ5ZM8WFGYK\",\"siblings\":null,\"attributes\":{\"listing_price\":3499}},{\"item_id\":\"ITMEXFRGGWESECK9\",\"title\":\"[\\\"1 gb ram\\\",\\\"micromax spark 4g cosmic grey 8 gb \\\"]\",\"listing_id\":\"LSTMOBEXFRGEQVVUK9HSS4TSC\",\"product_id\":\"MOBEXFRGEQVVUK9H\",\"siblings\":null,\"attributes\":{\"listing_price\":4499}},{\"item_id\":\"ITMEXFRGGWESECK9\",\"title\":\"[\\\"1 gb ram\\\",\\\"micromax spark 4g black 8 gb \\\"]\",\"listing_id\":\"LSTMOBEXFRGMRFESQ6MQXKX26\",\"product_id\":\"MOBEXFRGMRFESQ6M\",\"siblings\":null,\"attributes\":{\"listing_price\":4499}},{\"item_id\":\"ITMEXFRGGWESECK9\",\"title\":\"[\\\"1 gb ram\\\",\\\"micromax spark 4g champagne 8 gb \\\"]\",\"listing_id\":\"LSTMOBEXFRGFQUSAHBHB99AYZ\",\"product_id\":\"MOBEXFRGFQUSAHBH\",\"siblings\":null,\"attributes\":{\"listing_price\":4499}},{\"item_id\":\"ITMEWVG36YYRCWTN\",\"title\":\"[\\\"1 gb ram\\\",\\\"panasonic p77 white 16 gb \\\"]\",\"listing_id\":\"LSTMOBEWVG3G75URZQPTDPPPJ\",\"product_id\":\"MOBEWVG3G75URZQP\",\"siblings\":null,\"attributes\":{\"listing_price\":5299}},{\"item_id\":\"ITMEVT7ZHNHXMFZC\",\"title\":\"[\\\"4 gb ram\\\",\\\"moto m silver 64 gb \\\"]\",\"listing_id\":\"LSTMOBENQAVFTG6FPXX20CUZK\",\"product_id\":\"MOBENQAVFTG6FPXX\",\"siblings\":null,\"attributes\":{\"listing_price\":12999}},{\"item_id\":\"ITMEVT7ZHNHXMFZC\",\"title\":\"[\\\"3 gb ram\\\",\\\"moto m grey 32 gb \\\"]\",\"listing_id\":\"LSTMOBENQAVMG6YZDGXHJ8QYQ\",\"product_id\":\"MOBENQAVMG6YZDGX\",\"siblings\":null,\"attributes\":{\"listing_price\":11999}},{\"item_id\":\"ITMEVT7ZHNHXMFZC\",\"title\":\"[\\\"4 gb ram\\\",\\\"moto m gold 64 gb \\\"]\",\"listing_id\":\"LSTMOBENQAVANRMEGAP3UGWXV\",\"product_id\":\"MOBENQAVANRMEGAP\",\"siblings\":null,\"attributes\":{\"listing_price\":12999}},{\"item_id\":\"ITMEVT7ZHNHXMFZC\",\"title\":\"[\\\"4 gb ram\\\",\\\"moto m grey 64 gb \\\"]\",\"listing_id\":\"LSTMOBENQATHQTKG7AVGDUDDN\",\"product_id\":\"MOBENQATHQTKG7AV\",\"siblings\":null,\"attributes\":{\"listing_price\":12999}},{\"item_id\":\"ITMEN6DAQHTBUZHJ\",\"title\":\"[\\\"apple iphone 7 black 128 gb \\\"]\",\"listing_id\":\"LSTMOBEMK62E7YZEVZ8CAIHKE\",\"product_id\":\"MOBEMK62E7YZEVZ8\",\"siblings\":null,\"attributes\":{\"listing_price\":55499}},{\"item_id\":\"ITMEXRDXPQZC9BAZ\",\"title\":\"[\\\"1 gb ram\\\",\\\"intex aqua 5 5 vr champagne white 8 gb \\\"]\",\"listing_id\":\"LSTMOBENF7FPKYZGQGYG8BDMA\",\"product_id\":\"MOBENF7FPKYZGQGY\",\"siblings\":null,\"attributes\":{\"listing_price\":4999}},{\"item_id\":\"ITMEXPYEXZZHZYXT\",\"title\":\"[\\\"4 gb ram\\\",\\\"asus zenfone 4 selfie dual camera gold 64 gb \\\"]\",\"listing_id\":\"LSTMOBEWPZFHZJZZNAMOSFVW7\",\"product_id\":\"MOBEWPZFHZJZZNAM\",\"siblings\":null,\"attributes\":{\"listing_price\":14999}},{\"item_id\":\"ITMEWVG3BG5GQZZN\",\"title\":\"[\\\"1 gb ram\\\",\\\"panasonic p77 grey 16 gb \\\"]\",\"listing_id\":\"LSTMOBEWVG3TMQH6Z9RWK3JUK\",\"product_id\":\"MOBEWVG3TMQH6Z9R\",\"siblings\":null,\"attributes\":{\"listing_price\":5299}}],\"debug\":{\"solr-req\":{\"distrib\":\"false\",\"group\":\"on\",\"q.alt\":\"*:*\",\"zone\":\"GLOBAL\",\"solr-core\":\"electronicsCore\",\"orig_rows\":\"100\",\"facet-optimization\":\"true\",\"groupingImpl\":\"default\",\"BechmarkType\":\"all\",\"fl\":\",listing_id:id,product_id:_root_,item_id,title:mField(title_lc)\",\"probabilistic.matchType\":\"cachedWeight\",\"wt\":\"javabin\",\"facet.limit\":\"2000\",\"main-api-call\":\"true\",\"shard.url\":\"http://10.33.129.82:25280/solr/electronicsLow-shard8/\",\"CollapsePlugin\":\"collapseScorer\",\"listingsCount\":\"1\",\"facet\":\"on\",\"start\":\"0\",\"debugQuery\":\"on\",\"correlationId\":\"09fae534-30be-46f2-9d21-cf71105867be\",\"rows\":\"100\",\"orig_start\":\"0\",\"version\":\"2\",\"collapseSort\":\"nrtBool(GLOBAL_is_available) desc,loglinear(nrtCase(GLOBAL_sla,0.2,0-0,1,1-1,0.95,2-2,0.85,3-3,0.5,4-5,0.2,6-7,0.2,8-8,0.2),3,div(naturalMinOfL(sub(sum(nrtInt(listing_price),nrtCase(GLOBAL_shipping_type,200,inter_zone,listing_shipping_charge_inter_zone_dsint,intra_zone,listing_shipping_charge_intra_zone_dsint,intra_city,listing_shipping_charge_intra_city_dsint)),nrtInt(offer_value))),sub(sum(nrtInt(listing_price),nrtCase(GLOBAL_shipping_type,200,inter_zone,listing_shipping_charge_inter_zone_dsint,intra_zone,listing_shipping_charge_intra_zone_dsint,intra_city,listing_shipping_charge_intra_city_dsint)),nrtInt(offer_value))),10,nrtCase(GLOBAL_shipping_type,0.95,intra_city,1,intra_zone,0.99,inter_zone,0.95),2,nrtCase(lqs_override,0.09,alpha-0,1,alpha-1,0.98,alpha-2,0.95,alpha-3,0.5,alpha-4,0.1,alpha-ee,1.1,fa-0,0.99,fa-1,0.97,fa-2,0.94,fa-3,0.5,fa-4,0.1,fa-ee,1.09,0,0.90,1,0.88,2,0.86,3,0.45,4,0.09,ee,0.99,gold-0,0.96,gold-1,0.94,gold-2,0.91,gold-3,0.48,gold-4,0.10,gold-ee,1.06,silver-0,0.95,silver-1,0.93,silver-2,0.90,silver-3,0.48,silver-4,0.10,silver-ee,1.05),0.3) desc\",\"facet.field\":[\"{!ex=resolution_type}resolution_type_string\",\"{!ex=network_type}network_type_string\",\"{!ex=battery_capacity}battery_capacity_string\",\"{!ex=brand}brand_string\",\"{!ex=secondary_camera}secondary_camera_megapixel_string\",\"{!ex=type}type_string\",\"{!ex=operating_system}operating_system_string\",\"{!ex=operating_system_version_name}operating_system_version_name_string\",\"{!ex=sim_type}sim_type_string\",\"{!ex=clock_speed}clock_speed_string\",\"{!ex=ram}ram_string\",\"{!ex=internal_storage}internal_storage_string\",\"{!ex=speciality}speciality_string\",\"{!ex=primary_camera}rear_camera_string\",\"{!ex=screen_size}screensize_range_string\",\"features_string\",\"{!ex=number_of_cores}number_of_cores_string\",\"{!ex=processor_brand}processor_brand_string\"],\"isShard\":\"true\",\"facet.query\":[\"{!ex=price_range}((listing_price_inrt:[2001 TO 3999]))\",\"{!ex=fulfilled_by}((is_flipkart_advantage_bnrt:\\\"true\\\"))\",\"{!ex=offer_type}((offer_type_enrt:\\\"BASKET\\\"))\",\"{!ex=offer_type}((offer_type_enrt:\\\"EXCHANGE\\\"))\",\"{!ex=price_range}((listing_price_inrt:[4000 TO 6999]))\",\"{!ex=price_range}((listing_price_inrt:[13000 TO 15999]))\",\"{!ex=price_range}((listing_price_inrt:[10000 TO 12999]))\",\"{!ex=price_range}((listing_price_inrt:[16000 TO 19999]))\",\"{!ex=price_range}((listing_price_inrt:[25000 TO 29999]))\",\"{!ex=price_range}((listing_price_inrt:[20000 TO 24999]))\",\"{!ex=price_range}((listing_price_inrt:[50001 TO 999999999]))\",\"{!ex=price_range}((listing_price_inrt:[7000 TO 9999]))\",\"{!ex=offer_type}((offer_type_enrt:\\\"BANK\\\"))\",\"{!ex=price_range}((listing_price_inrt:[30000 TO 49999]))\",\"{!ex=budget}((listing_price_inrt:[30000 TO 999999999]))\",\"{!ex=budget}((listing_price_inrt:[1 TO 4999]))\",\"{!ex=budget}((listing_price_inrt:[15000 TO 29999]))\",\"{!ex=budget}((listing_price_inrt:[10000 TO 14999]))\",\"{!ex=budget}((listing_price_inrt:[5000 TO 9999]))\",\"{!ex=availability}((GLOBAL_is_available_bnrt:\\\"true\\\"))\",\"{!ex=offer_type}((offer_type_enrt:\\\"LISTING\\\"))\",\"{!ex=offer_type}((offer_type_enrt:\\\"EMI\\\"))\",\"{!ex=price_range}((listing_price_inrt:[1 TO 2000]))\"],\"collapseSortNames\":\"availability,\",\"group.ngroups\":\"on\",\"sort\":\"nrtBool(GLOBAL_is_available) desc,promotion_score desc,normloglinear(nrtCase(GLOBAL_sla,0.2,0-0,1,1-1,0.95,2-2,0.85,3-3,0.5,4-5,0.2,6-7,0.2,8-8,0.2),20,div(def(item_cpr_001_dslong,0),3.19424384E8),6,nrtCase(GLOBAL_shipping_type,0.95,intra_city,1,intra_zone,0.99,inter_zone,0.95),10,nrtCase(lqs,0.1,0,1,1,0.98,2,0.95,3,0.5,4,0.1,ee,1),300,nrtCase(listing_tier,0.9,none,0.9,none-fa,0.99,none-alpha,1,none-gold,0.96,none-silver,0.95,pl,0.99,pl-fa,1.09,pl-alpha,1.1,pl-gold,1.06,pl-silver,1.05),300) desc\",\"sortMissingLast\":\"true\",\"group.field\":\"_root_\",\"trackPopularityScores\":\"\",\"fq\":[\"is_live_bnrt:\\\"true\\\"\",\"+(igor-store_lc:\\\"tyy\\\") +(igor-store_lc:\\\"4io\\\")\",\"product_state:\\\"ready\\\"\",\"-listing_mp_id:\\\"GROCERY\\\"\",\"listing_status_bnrt:\\\"true\\\"\",\"is_discoverable:\\\"true\\\"\"],\"addqf\":\"compatibility_text^0.0001 compatibility_title_text^0.0001\",\"qt\":\"dismax\",\"fsv\":\"true\",\"debug.explain.structured\":\"true\",\"htype\":\"\",\"group.truncate\":\"on\",\"enable-new-grouping\":\"true\",\"facet.listingcountfix\":\"true\",\"block_join_doc_type\":\"Listing\"},\"parsedquery\":\"FkToChildBlockJoinQuery(FkToChildBlockJoinQuery (+(+*:* (div(lValueSource(float(floSalesBoost)),const(1000)))^100.0) +block_join_doc_type:Product^0.0))\"},\"augmentation-object\":null,\"classification\":null,\"spell-suggestions\":null,\"partial-matches\":null,\"semantic-deductions\":null,\"query-classification\":null}}"
product_data = json.loads(product_data)
product_data = product_data['RESPONSE']['products']
product_attr_dict = {}
for each_product in product_data:
    product_attr_dict[each_product['product_id']] = each_product

# print product_attr_dict

def enrich(each_product) :
    product_id_ = each_product['product_id']
    if product_id_ in product_attr_dict:
        product_data = product_attr_dict[product_id_]
    else :
        product_data = {}
    necessary_keys = ["title", "attributes"]
    for key in necessary_keys :
        if key in product_data :
            each_product[key] = product_data[key]
    return each_product

@app.route("/score")
def score():
    args = request.args
    if "clicked" not in args :
        clicked_products = [CONST.DEFAULT_CLICK_TEXT]
    else :
        clicked_arg = args['clicked']
        clicked_products = clicked_arg.split(",")
    resp = scorer.score(products_to_rank, clicked_products)
    resp = map(enrich, resp)

    body = {"REQUEST" : args, "RESPONSE" : {"products" : resp}}
    return jsonify(body)


