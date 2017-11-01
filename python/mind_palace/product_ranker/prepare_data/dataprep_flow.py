from integerize_product_attributes import integerize_product_attributes
from integerize_clickstream import integerize_clickstream
from enhance_clickstream import enhance_clickstream
from mind_palace.commons.helpers import logBreak
import os
import glob
import sys

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

def get_attributedicts_path(output_path) :
    return output_path + "/" + dicts_path_suffix

def get_integerized_attributes_path(output_path) :
    return output_path + "/" + integerized_attributes_path_suffix

def get_enhanceddata_path(output_path, version) :
    return output_path + "/" + enhanced_clickstream_path_suffix + "." + str(version)

def get_trainingdata_path(output_path) :
    return output_path + "/" + enhanced_clickstream_path_suffix + ".*"

def flow(attributes,
         attributes_path,
         clickstream_path,
         output_path,
         num_enhance_times=1) :

    make_dir(output_path)

    attribute_dicts_path = get_attributedicts_path(output_path)
    integerized_attributes_path = get_integerized_attributes_path(output_path)
    integerized_clickstream_path = output_path + "/" + integerized_clickstream_path_suffix

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
        enhanced_clickstream_path = get_enhanceddata_path(output_path, i)
        make_dir(enhanced_clickstream_path)
        globbed_integerized_clickstream_path = glob.glob(integerized_clickstream_path + "/part-*")
        enhance_clickstream(attributes, integerized_attributes_path, globbed_integerized_clickstream_path, enhanced_clickstream_path)
        log("done enhancing clickstream : " + str_i)


if __name__ == '__main__' :

    attributes = ["productId", "brand", "vertical"]
    attributesPathBase = sys.argv[1] #"/home/thejus/workspace/learn-cascading/data/product-attributes.MOB/part-*"
    clickstreamPathBase = sys.argv[2] #"/home/thejus/workspace/learn-cascading/data/sessionExplodeWithAttributes-201708.MOB.smaller"
    outputPath = sys.argv[3]

    attributesPath = attributesPathBase + "/part-*"
    clickstreamPath = clickstreamPathBase + "/part-*"

    flow(attributes, attributesPath, clickstreamPath, outputPath, num_enhance_times=3)

