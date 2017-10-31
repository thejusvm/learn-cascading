import cPickle as pickle
import glob
import os
import tensorflow as tf
import time

from mind_palace.product_ranker.models import model_factory as mf
from mind_palace.product_ranker.models.model import model
from mind_palace.product_ranker.models.modelconfig import modelconfig, AttributeConfig
from mind_palace.product_ranker.prepare_data.clickstream_iterator import ClickstreamDataset
from mind_palace.product_ranker.prepare_data.integerize_clickstream import get_attributedict_path, get_attributedict
from mind_palace.product_ranker.training.trainingcontext import trainingcontext, getTraningContextDir
from mind_palace.product_ranker.prepare_data.dataprep_flow import get_attributedicts_path, get_trainingdata_path

"""
    generates training data with
        * random negative samples padded
        * click context padded with appropriate CONST.PAD_TEXT index
    Operates on top of output of integerize_clickstream
"""

def logBreak() :
    print "------------------------------------------"

def print_dict(dict_1) :
    for key in dict_1:
        print str(key) + " : " + str(dict_1.get(key))

def save_traincxt(trainCxt) :
    train_context_model_dir = trainCxt.getTrainCxtDir()
    with open(train_context_model_dir, 'w+b') as handle:
        pickle.dump(trainCxt, handle, protocol=pickle.HIGHEST_PROTOCOL)

def train(train_cxt) :
    """@type train_cxt: trainingcontext"""

    modelconf = trainCxt.model_config
    logBreak()
    print "Using train context : "
    print_dict(trainCxt.__dict__)
    logBreak()
    print "Using model config : "
    print_dict(modelconf.__dict__)
    logBreak()


    ################################### Prepareing datasets
    attributes = map(lambda x : x.name, modelconf.attributes_config)
    dataset = ClickstreamDataset(attributes, batch_size=train_cxt.batch_size, shuffle=True)
    # HACK : Using extremely large batch_size, to reuse the same code as training. no method in Dataset to say batch all in one go
    test_dataset = ClickstreamDataset(attributes, batch_size=train_cxt.max_test_size, shuffle=False)
    ################################### Start model building

    attribute_dicts = get_attributedict(train_cxt.attributedict_path)
    for attribute_config in modelconf.attributes_config :
        attribute_name = attribute_config.name
        attribute_dict = attribute_dicts[attribute_name]
        attribute_config.vocab_size = attribute_dict.dictSize()

    mod = mf.get_model(modelconf) #type: model
    logBreak()

    mod.feed_input(dataset.get_next)
    loss = mod.loss()
    minimize_step = mod.minimize_step()

    mod.feed_input(test_dataset.get_next)
    test_loss = mod.loss()
    summaries = mod.test_summaries()

    sess = tf.Session()
    sess.run(tf.global_variables_initializer())

    loss_summary = tf.summary.scalar("loss", loss)
    test_loss_summary = tf.summary.scalar("test_loss", test_loss)

    test_summaries = [test_loss_summary]
    for summary in summaries :
        test_summary = tf.summary.scalar("test_" + summary[0], summary[1])
        test_summaries.append(test_summary)
    merged_summary = tf.summary.merge(test_summaries)

    summary_writer = None
    if trainCxt.publish_summary :
        print "creating summary writer, publishing summaries to : " + train_cxt.summary_dir
        logBreak()
        summary_writer = tf.summary.FileWriter(trainCxt.summary_dir, sess.graph)

    ################################### End model building

    ################################### Saving dict to file

    if trainCxt.save_model :
        if not os.path.exists(trainCxt.model_dir):
            os.makedirs(trainCxt.model_dir)
        nn_model_dir = trainCxt.getNnDir()
        save_traincxt(trainCxt)

        print "saved trainCxt into " + trainCxt.model_dir
        logBreak()
    ################################### End saving dict to file

    ################################### Start model training

    saver = tf.train.Saver()
    if os.path.exists(trainCxt.model_dir):
        nn_dir = tf.train.latest_checkpoint(trainCxt.model_dir)
        if nn_dir is not None :
            saver.restore(sess, nn_dir)
            print "restoring model from : " + nn_dir
        else:
            print "no model to restore from : " + trainCxt.model_dir
    logBreak()

    if trainCxt.restore_model_dir is not None and os.path.isdir(trainCxt.restore_model_dir) :
        restore_nn_dir = tf.train.latest_checkpoint(trainCxt.restore_model_dir)
        print "restoring tf model from : " + restore_nn_dir
        saver.restore(sess, restore_nn_dir)
        logBreak()

    num_batches_per_latency_track = int(trainCxt.latency_track_iters / trainCxt.batch_size)
    print "model training started"

    elapsed_time = 0
    for epoch in range(trainCxt.num_epochs) :
        print "epoch : " + str(epoch)
        dataset.initialize_iterator(sess, train_cxt.train_path)
        epoch_start = time.time()
        while True :
            try :
                start = time.time()
                _, loss_val, summary = sess.run([minimize_step, loss, loss_summary])
                elapsed_time += time.time() - start
                # print str(trainCxt.train_counter) + " processing one batch took : " + str(elapsed_time)
                if summary_writer is not None :
                    summary_writer.add_summary(summary, trainCxt.train_counter)
                    if trainCxt.train_counter % num_batches_per_latency_track == 0 :
                        latency_summary = tf.Summary(value=[tf.Summary.Value(tag="per_"+str(trainCxt.latency_track_iters)+"_latency", simple_value=elapsed_time)])
                        summary_writer.add_summary(latency_summary, trainCxt.train_counter * trainCxt.batch_size)
                        elapsed_time = 0

                if summary_writer is not None and trainCxt.train_counter % trainCxt.test_summary_publish_iters == 0 :
                    test_dataset.initialize_iterator(sess, train_cxt.test_path)
                    start = time.time()
                    all_summary = sess.run(merged_summary)
                    print str(trainCxt.train_counter) + " processing test batch took : " + str(time.time() - start)
                    summary_writer.add_summary(all_summary, trainCxt.train_counter)

                if trainCxt.save_model and trainCxt.save_model_num_iter != None and trainCxt.train_counter % trainCxt.save_model_num_iter == 0:
                    saver.save(sess, nn_model_dir + ".counter" , global_step = trainCxt.train_counter)
                    save_traincxt(trainCxt)
                    print "saved nn model on counter " + str(trainCxt.train_counter) + " into : " + nn_model_dir

                trainCxt.train_counter = trainCxt.train_counter + 1
            except tf.errors.OutOfRangeError:
                break
        print "processing epoch took : " + str(time.time() - epoch_start)

        ################################### Saving model to file
        if trainCxt.save_model_on_epoch and trainCxt.save_model :
            saver.save(sess, nn_model_dir + ".epoch", global_step = epoch)
            save_traincxt(trainCxt)
            print "saved nn on epoch " + str(epoch) + "model into : " + nn_model_dir
            logBreak()
        ################################### End model to file

    if trainCxt.save_model :
        nn_model_dir = trainCxt.model_dir + '/nn'
        saver.save(sess, nn_model_dir)
        save_traincxt(trainCxt)
        print "saved nn model into : " + nn_model_dir
        logBreak()

    print "model training ended"
    logBreak()

    ################################### End model training

if __name__ == '__main__' :

    restore_model_path = None
    # restore_model_path = "saved_models/run.20171023-17-01-09"
    if restore_model_path is not None :
        dir = getTraningContextDir(restore_model_path)
        print "loading training context : " + dir
        with open(dir, 'rb') as handle:
            trainCxt = pickle.load(handle)
    else :

        timestamp = time.localtime()
        currentdate = time.strftime('%Y%m%d-%H-%M-%S', timestamp)

        root_data_path = "/home/thejus/workspace/learn-cascading/data/sessionExplodeWithAttributes-201708.MOB.smaller.int"

        trainCxt = trainingcontext()
        trainCxt.date = currentdate
        trainCxt.data_path = get_trainingdata_path(root_data_path)
        trainCxt.attributedict_path = get_attributedicts_path(root_data_path)
        trainCxt.model_dir = "saved_models/run." + currentdate
        trainCxt.summary_dir = "summary/sessionsimple." + currentdate
        trainCxt.test_size = 0.03
        trainCxt.num_epochs = 25
        trainCxt.num_negative_samples = 20
        trainCxt.min_click_context = 0
        trainCxt.publish_summary = True
        trainCxt.save_model = True
        trainCxt.save_model_num_iter = 20000
        trainCxt.test_summary_publish_iters = 10000
        trainCxt.restore_model_dir = None #"saved_models/run.20171023-13-26-35"
        trainCxt.batch_size = 500

        dataFiles = sorted(glob.glob(trainCxt.data_path + "/part-*"))
        numFiles = len(dataFiles)
        trainSize = int(numFiles * (1 - trainCxt.test_size))
        trainCxt.train_path = dataFiles[:trainSize]
        trainCxt.test_path = dataFiles[trainSize:]

        modelconf = modelconfig("softmax_model")
        # modelconf.layer_count = [1024, 512, 256]
        modelconf.use_context = True
        modelconf.enable_default_click = False
        modelconf.reuse_context_dict = False
        # modelconf.attributes_config = [AttributeConfig("productId", 30), AttributeConfig("brand", 15), AttributeConfig("vertical", 5)]
        modelconf.attributes_config = [AttributeConfig("productId", 30), AttributeConfig("brand", 15)]
        # modelconf.attributes_config = [AttributeConfig("productId", 50)]
        trainCxt.model_config = modelconf

    train(trainCxt)

