# KNN.py
# Standalone Python/Spark program to perform KNN implementation in pyspark
from __future__ import division
import sys
import math
# import distance
import numpy
from pyspark import SparkContext


# defining euclidean_distance():
def dist(x, y):# computing distance between 2 records in 55 dimensions
    # 1 record from test ->x , 1 record from train -> y
    inp = y
    if (len(x) == 0):
        x = [0] * 54
    elif (len(y) == 0):
        y = [0] * 54
    else:
        x = x.split(",")
        y = y.split(",")

    res = (
    math.pow((int(x[1]) - int(y[1])), 2), math.pow((int(x[2]) - int(y[2])), 2), math.pow((int(x[3]) - int(y[3])), 2),
    math.pow((int(x[4]) - int(y[4])), 2), math.pow((int(x[5]) - int(y[5])), 2), math.pow((int(x[6]) - int(y[6])), 2),
    math.pow((int(x[7]) - int(y[7])), 2), math.pow((int(x[8]) - int(y[8])), 2), math.pow((int(x[9]) - int(y[9])), 2),
    math.pow((int(x[10]) - int(y[10])), 2), math.pow((int(x[11]) - int(y[11])), 2),
    math.pow((int(x[12]) - int(y[12])), 2), math.pow((int(x[13]) - int(y[13])), 2),
    math.pow((int(x[14]) - int(y[14])), 2), math.pow((int(x[15]) - int(y[15])), 2),
    math.pow((int(x[16]) - int(y[16])), 2), math.pow((int(x[17]) - int(y[17])), 2),
    math.pow((int(x[18]) - int(y[18])), 2), math.pow((int(x[19]) - int(y[19])), 2),
    math.pow((int(x[20]) - int(y[20])), 2), math.pow((int(x[21]) - int(y[21])), 2),
    math.pow((int(x[22]) - int(y[22])), 2), math.pow((int(x[23]) - int(y[23])), 2),
    math.pow((int(x[24]) - int(y[24])), 2), math.pow((int(x[25]) - int(y[25])), 2),
    math.pow((int(x[26]) - int(y[26])), 2), math.pow((int(x[27]) - int(y[27])), 2),
    math.pow((int(x[28]) - int(y[28])), 2), math.pow((int(x[29]) - int(y[29])), 2),
    math.pow((int(x[30]) - int(y[30])), 2), math.pow((int(x[31]) - int(y[31])), 2),
    math.pow((int(x[32]) - int(y[32])), 2), math.pow((int(x[33]) - int(y[33])), 2),
    math.pow((int(x[34]) - int(y[34])), 2), math.pow((int(x[35]) - int(y[35])), 2),
    math.pow((int(x[36]) - int(y[36])), 2), math.pow((int(x[37]) - int(y[37])), 2),
    math.pow((int(x[38]) - int(y[38])), 2), math.pow((int(x[39]) - int(y[39])), 2),
    math.pow((int(x[40]) - int(y[40])), 2), math.pow((int(x[41]) - int(y[41])), 2),
    math.pow((int(x[42]) - int(y[42])), 2), math.pow((int(x[43]) - int(y[43])), 2),
    math.pow((int(x[44]) - int(y[44])), 2), math.pow((int(x[45]) - int(y[45])), 2),
    math.pow((int(x[46]) - int(y[46])), 2), math.pow((int(x[47]) - int(y[47])), 2),
    math.pow((int(x[48]) - int(y[48])), 2), math.pow((int(x[49]) - int(y[49])), 2),
    math.pow((int(x[50]) - int(y[50])), 2), math.pow((int(x[51]) - int(y[51])), 2),
    math.pow((int(x[52]) - int(y[52])), 2), math.pow((int(x[53]) - int(y[53])), 2))
    sum = 0
    for x in res:
        sum = sum + x
    result = [math.sqrt(sum), inp]
    return result


def parse(k, vals):
    vals.sort()  # sort it based on the distance
    r = vals[0:3]  # hold the top 3 shortest distance
    t = k[-1]
    return k, r

# Determining the Cover Type category by identifying record with shortest distance of N records (N -> k values)
def categ(k, ll):
    datas = []
    t = k[-1]
    for l in ll:
        datas.append(l[1])
    ct = [10000000] * 7
    for x in datas:
        inp = x
        tp = inp.split(",")[-1]
        tp = int(tp) - 1
        if ct[tp] == 10000000:
            ct[tp] = 1
        else:
            ct[tp] = ct[tp] + 1
    max_idx = 10
    max_limit = 10000000
    val = 0
    i = 0
    while (i < len(ct)):
        if ct[i] != 10000000 and ct[i] > val:
            max_idx = i
        i = i + 1
    return t, max_idx + 1

# Defining a function to calculate model accuracy
def accuracy(x, y):
    x = int(x)
    y = int(y)
    if (x == y):
        return 1
    else:
        return 0

# Main function
if __name__ == "__main__":
    sc = SparkContext(appName="KNN implementation in pyspark")
    train_data = sc.textFile(sys.argv[1])
    header = train_data.first()
    train_data = train_data.filter(lambda row: row != header)  # filter out header)
    testset, trainingset = train_data.randomSplit([0.4, 0.6])
    # rain_data=sc.parallelize(train_data1)
    total = testset.count()
    nearestNeigbours = testset.cartesian(trainingset).map(
        lambda (test, train): (test, dist(test, train))).groupByKey().map(lambda x: parse(x[0], list(x[1]))).map(
        lambda x: categ(x[0], list(x[1]))).map(lambda (x, y): accuracy(x, y)).filter(lambda x: x == 1).count()
    # nearestNeigbours = testset.cartesian(trainingset).map(lambda (test,train): (test,dist(test, train))).groupByKey().map(lambda x:parse(x[0], list(x[1])))
    accuracy = nearestNeigbours / total
    print(accuracy * 100)
# data, prediction
