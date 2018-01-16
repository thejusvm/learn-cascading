
import tensorflow as tf
import numpy as np

sess = tf.Session()

a = tf.constant(np.arange(1, 13, dtype=np.int32),
                shape=[2, 2, 3])

b = tf.constant(np.arange(1, 7, dtype=np.int32),
                shape=[2, 3])
b = tf.transpose(b)
b_1 = tf.transpose(b)
b_1 = tf.expand_dims(b_1, 0)

print sess.run(a)
print "--------------------------------"
print sess.run(b)
print "--------------------------------"

c = tf.multiply(a, b_1)
c_matmul = tf.reduce_sum(c, reduction_indices=-1)

print a
print b
print c
print "--------------------------------"
print sess.run(c)
print "--------------------------------"
print sess.run(c_matmul)