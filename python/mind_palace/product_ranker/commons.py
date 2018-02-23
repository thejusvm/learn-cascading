import cPickle as pickle
from mind_palace.DictIntegerizer import DictIntegerizer


def _new_dictintegerizer(attribute, deafult_dicy_keys):
    dict_i = DictIntegerizer(default=deafult_dicy_keys, name=attribute)
    return dict_i

def init_attribute_dicts(attributes, default_dict_keys=None) :
    attribute_dicts = {}
    for attribute in attributes :
        attributedict = _new_dictintegerizer(attribute, default_dict_keys)
        attribute_dicts[attribute] = attributedict
    return attribute_dicts

def read_attribute_dicts(attribute_dict_path):
    with open(attribute_dict_path, 'rb') as handle:
        attribute_dict = pickle.load(handle)
    return attribute_dict

def write_attribute_dicts(attribute_dict_path, dicts):
    with open(attribute_dict_path, 'w+b') as handle:
        pickle.dump(dicts, handle, protocol=pickle.HIGHEST_PROTOCOL)


def generate_key(key_prefix, attribute):
    return key_prefix + "_" + attribute


def generate_feature_names(attributes, feature_prefixes):
    features = []
    for attribute in attributes :
        for feature_prefix in feature_prefixes :
            features.append(generate_key(feature_prefix, attribute))
    return features