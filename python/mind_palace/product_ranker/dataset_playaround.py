import tensorflow as tf

dataset1 = tf.contrib.data.Dataset.from_tensor_slices(tf.random_uniform([4, 10]))
print(dataset1.output_types)  # ==> "tf.float32"
print(dataset1.output_shapes)  # ==> "(10,)"

dataset2 = tf.contrib.data.Dataset.from_tensor_slices((tf.random_uniform([4]), tf.random_uniform([4, 100], maxval=100, dtype=tf.int32)))
print(dataset2.output_types)  # ==> "(tf.float32, tf.int32)"
print(dataset2.output_shapes)  # ==> "((), (100,))"

dataset3 = tf.contrib.data.Dataset.zip((dataset1, dataset2))
print(dataset3.output_types)  # ==> (tf.float32, (tf.float32, tf.int32))
print(dataset3.output_shapes)  # ==> "(10, ((), (100,)))"

dataset = tf.contrib.data.Dataset.from_tensor_slices(
    {"a": tf.random_uniform([4]),
     "b": tf.random_uniform([4, 100], maxval=100, dtype=tf.int32)})
print(dataset.output_types)  # ==> "{'a': tf.float32, 'b': tf.int32}"
print(dataset.output_shapes)

sess = tf.Session()

# dataset = tf.contrib.data.Dataset.range(100)
# iterator = dataset.make_one_shot_iterator()
# next_element = iterator.get_next()

# for i in range(100):
#     value = sess.run(next_element)
#     print value

max_value = tf.placeholder(tf.int64, shape=[])
dataset = tf.contrib.data.Dataset.range(max_value)
batch_dataset = dataset.batch(5)
iterator = batch_dataset.make_initializable_iterator()
next_element = iterator.get_next()

sess.run(iterator.initializer, feed_dict={max_value: 10})
for i in range(11):
    try :
        value = sess.run(next_element)
        print value
        print "-----"
    except tf.errors.OutOfRangeError:
        break

# sess.run(iterator.initializer, feed_dict={max_value: 100})
# for i in range(100):
#     value = sess.run(next_element)
#     print value

# training_dataset = tf.contrib.data.Dataset.range(100).map(
#     lambda x: x + tf.random_uniform([], -10, 10, tf.int64))
# validation_dataset = tf.contrib.data.Dataset.range(50)
# iterator = tf.contrib.data.Iterator.from_structure(training_dataset.output_types,
#                                    training_dataset.output_shapes)
# next_element = iterator.get_next()
#
# training_init_op = iterator.make_initializer(training_dataset)
# validation_init_op = iterator.make_initializer(validation_dataset)
#
# # Run 20 epochs in which the training dataset is traversed, followed by the
# # validation dataset.
# for _ in range(20):
#     # Initialize an iterator over the training dataset.
#     sess.run(training_init_op)
#     for _ in range(100):
#         print sess.run(next_element)
#
#     # Initialize an iterator over the validation dataset.
#     sess.run(validation_init_op)
#     for _ in range(50):
#         print sess.run(next_element)
#
