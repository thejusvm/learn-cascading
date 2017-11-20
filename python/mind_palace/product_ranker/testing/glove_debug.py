import math
import numpy as np
import sys
from sklearn.model_selection import train_test_split
from mind_palace.product_ranker.models.glove import glove
from mind_palace.product_ranker.models.modelconfig import modelconfig, AttributeConfig, EmbeddingDicts
import tensorflow as tf



def score(i, j) :
    return (math.fabs(5 - i) + 1) * (math.fabs(5 - j) + 1)/ 100

co_occur=[]
# for i in range(0, 11):
#     for j in range(0, 11):
#         co_occur.append([i, j, score(i,j)])

moving_point = 4
distance = 2
for i in range(0, 4):
    co_occur.append([i, moving_point, distance])
co_occur = np.array(co_occur, dtype=float)

print "co occur data : "
print co_occur
print "-------------------------"

# sys.exit()
train = co_occur

# train, test = train_test_split(co_occur, test_size=.1)
# print train
train_cols = [train[:,0].astype(int), train[:,1].astype(int), train[:,2]]
# test_cols = [test[:,0].astype(int), test[:,1].astype(int), test[:,2]]
# print test

cxt_dict = [[-1.0, -1.0], [1.0, -1.0], [-1.0, 1.0], [1.0, 1.0], [5.0, 5.0]]
# cxt_dict = None
modelconf = modelconfig("glove")
modelconf.attributes_config = AttributeConfig("productId", 2, 5,
                                              override_embeddings=EmbeddingDicts(softmax_weights=None, softmax_bias=None, context_dict=cxt_dict),
                                              per_attribute_learning_rate=[0.01, 0.01, 0.01, 0.01, 0.96])
mod = glove(modelconf)

mod.feed_input(["focal", "context", "score"], train_cols)
loss = mod.loss()
train_step = mod.minimize_step()

sess = tf.Session()
sess.run(tf.global_variables_initializer())

print "init embeddings : "
print sess.run([mod.focal_embeddings])
print "-------------------------"

# for x in sess.run([mod.gradient, mod.gradient__1, mod.input_specific_learning_rate_for_indices, mod.gradient__2]) :
#     print x
#     print "........."
#
# print sess.run([train_step])
# print "modified embeddings : "
# print sess.run([mod.focal_embeddings])
# print "-------------------------"
# sys.exit()

# print sess.run([mod.focal_learning_rate, mod.context_learning_rate])
# sys.exit()

for i in range(0, 10000):
    sess.run([train_step])
    # if i % 100 == 0:
    #     print fourth
scorer = mod.score()
score = sess.run(scorer)

# mod.feed_input(["focal", "context", "score"], test_cols)
# test_scorer = mod.score()
# print test_scorer
# test_score = sess.run(test_scorer)

print "scores : "
for i in range(len(train)) :
    print train[i], [score[j][i] for j in range(len(score))]

print "----------------------------"

"trained embeddings : "
for x in sess.run([mod.focal_embeddings]):
    print x

# for i in range(len(test)) :
#     print test[i], [test_score[j][i] for j in range(len(score))]


