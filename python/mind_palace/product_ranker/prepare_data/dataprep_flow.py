from integerize_product_attributes import integerize_product_attributes
from integerize_clickstream import integerize_clickstream
from enhance_clickstream import enhance_clickstream
from mind_palace.commons.helpers import logBreak
import os
import glob

dicts_path_suffix = "attribute_dicts.pickle"
integerized_attributes_path_suffix = "integerized_attributes"
integerized_clickstream_path_suffix = "integerized_clickstream"
enhanced_clickstream_path_suffix = "enhanced_clickstream.tfr"

def log(x) :
    print x
    logBreak()

def make_dir(path) :
    if not os.path.exists(path):
        os.makedirs(path)

def flow(attributes,
         attributes_path,
         clickstream_path,
         output_path,
         num_enhance_times=1) :

    make_dir(output_path)

    attribute_dicts_path = output_path + "/" + dicts_path_suffix
    integerized_attributes_path = output_path + "/" + integerized_attributes_path_suffix
    integerized_clickstream_path = output_path + "/" + integerized_clickstream_path_suffix
    enhanced_clickstream_path_prefix = output_path + "/" + enhanced_clickstream_path_suffix

    log("integerizing attributes")
    attribute_dicts = integerize_product_attributes(attributes, attributes_path, integerized_attributes_path, attribute_dicts_path)
    log("done integerizing attributes")
    log("attribute dict sizes : " + str(map(str, attribute_dicts.values())))


    log("integerizing clickstream")
    make_dir(integerized_clickstream_path)
    integerize_clickstream(attributes, attribute_dicts, clickstream_path, integerized_clickstream_path)
    log("done integerizing clickstream")

    for i in range(num_enhance_times) :
        str_i = str(i)
        log("enhancing clickstream : " + str_i)
        enhanced_clickstream_path = enhanced_clickstream_path_prefix + "." + str_i
        make_dir(enhanced_clickstream_path)
        globbed_integerized_clickstream_path = glob.glob(integerized_clickstream_path + "/part-*")
        enhance_clickstream(attributes, integerized_attributes_path, globbed_integerized_clickstream_path, enhanced_clickstream_path)
        log("done enhancing clickstream : " + str_i)


if __name__ == '__main__' :

    attributes = ["productId", "brand", "vertical"]
    attributesPath = "/home/thejus/workspace/learn-cascading/data/product-attributes.MOB/part-*"
    clickstreamPathBase = "/home/thejus/workspace/learn-cascading/data/sessionExplodeWithAttributes-201708.MOB.smaller"
    clickstreamPath = clickstreamPathBase + "/part-00003"
    outputPath = clickstreamPathBase + ".int"


    flow(attributes, attributesPath, clickstreamPath, outputPath, num_enhance_times=3)

