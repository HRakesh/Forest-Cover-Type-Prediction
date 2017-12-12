from __future__ import division
from pyspark import SparkContext
import os
import sys
import numpy
import math
import decimal as d
# import re
# import string
# import pandas as pd
# from collections import Counter
# r,continuousVarData,coverTypeProbability,likeProbWil,likeProbSt
# continuousVarData['elevation'].get(0).idx[0]
# coverType.idx[0]
# Prob of Like sT n CT
# Prob of Lie WA n CT
def typeClassification(r, contData, ctProb, lkProbWa, lkProbSt):
    predCT = []
    lSt = 0
    idx_st = 0
    idx_wa = 0
    list_st = r[15:55]
    list_wa = r[11:15]

    idx = 0

    while (idx < len(list_st)):
        if (list_st[idx] is '1'):
            idx_st = idx
        idx = idx + 1

    idx = 0

    while (idx < len(list_wa)):
        if (list_wa[idx] is '1'):
            idx_wa = idx
        idx = idx + 1






    # remove the numpy.random  method and replcae it with
    # mean=86.2 contval
    # st=9.7 contval
    # x=r[]

    # result = (1/(math.sqrt(2*math.pi)*st))* math.exp((-math.pow((x-mean),2))/(2*math.pow(st,2)))
    # replace numpy.random.normal(contData.get("slope")[0][0],contData.get("slope")[0][1],r[3])  ----->
    # (1/(math.sqrt(2*math.pi)*contData.get("slope")[0][1]))* math.exp((-math.pow((r[3]-contData.get("slope")[0][0]),2))/(2*math.pow(contData.get("slope")[0][1],2)))


#slope, elevation, elev to HDHyd, elev VD hyd, elevHdRoad, hRoadways, fire hyd, hill 9 AM

    p1=(1 / (math.sqrt(2 * math.pi) * contData.get("slope")[0][1])) * math.exp(
    (-math.pow((r[3] - contData.get("slope")[0][0]), 2)) / (2 * math.pow(contData.get("slope")[0][1], 2)))

    p2=(1 / (math.sqrt(2 * math.pi) * contData.get("elevation")[0][1])) * math.exp(
    (-math.pow((r[1] - contData.get("elevation")[0][0]), 2)) / (2 * math.pow(contData.get("elevation")[0][1], 2)))

   # p3=(1 / (math.sqrt(2 * math.pi) * contData.get("aspect")[0][1])) * math.exp(
    #(-math.pow((r[2] - contData.get("aspect")[0][0]), 2)) / (2 * math.pow(contData.get("aspect")[0][1], 2)))

    p4=(1 / (math.sqrt(2 * math.pi) * contData.get("hDFpts")[0][1])) * math.exp(
    (-math.pow((r[10] - contData.get("hDFpts")[0][0]), 2)) / (2 * math.pow(contData.get("hDFpts")[0][1], 2)))

    #p5=(1 / (math.sqrt(2 * math.pi) * contData.get("hilsde12")[0][1])) * math.exp(
    #(-math.pow((r[8] - contData.get("hilsde12")[0][0]), 2)) / (2 * math.pow(contData.get("hilsde12")[0][1], 2)))

    #p6=(1 / (math.sqrt(2 * math.pi) * contData.get("hilsde3")[0][1])) * math.exp(
    #(-math.pow((r[9] - contData.get("hilsde3")[0][0]), 2)) / (2 * math.pow(contData.get("hilsde3")[0][1], 2)))

    p7=(1 / (math.sqrt(2 * math.pi) * contData.get("hDrodways")[0][1])) * math.exp(
    (-math.pow((r[6] - contData.get("hDrodways")[0][0]), 2)) / (2 * math.pow(contData.get("hDrodways")[0][1], 2)))

    p8=(1 / (math.sqrt(2 * math.pi) * contData.get("hilsde9")[0][1])) * math.exp(
    (-math.pow((r[7] - contData.get("hilsde9")[0][0]), 2)) / (2 * math.pow(contData.get("hilsde9")[0][1], 2)))

    p9=(1 / (math.sqrt(2 * math.pi) * contData.get("vDhydr")[0][1])) * math.exp(
    (-math.pow((r[5] - contData.get("vDhydr")[0][0]), 2)) / (2 * math.pow(contData.get("vDhydr")[0][1], 2)))

    p10=(1 / (math.sqrt(2 * math.pi) * contData.get("hDhydro")[0][1])) * math.exp(
    (-math.pow((r[4] - contData.get("hDhydro")[0][0]), 2)) / (2 * math.pow(contData.get("hDhydro")[0][1], 2)))

    pl =[p1,p2,p4,p7,p8,p9,p10]
    ct1=1
    for k in pl:
        if k==0:
            k=1

        ct1=ct1*k

    #ct1=ct1*ctProb[0]*lkProbSt[0][idx_st] * lkProbWa[0][idx_wa]

    p1=(1 / (math.sqrt(2 * math.pi) * contData.get("slope")[1][1])) * math.exp(
    (-math.pow((r[3] - contData.get("slope")[1][0]), 2)) / (2 * math.pow(contData.get("slope")[1][1], 2)))
    p2=(1 / (math.sqrt(2 * math.pi) * contData.get("elevation")[1][1])) * math.exp(
    (-math.pow((r[1] - contData.get("elevation")[1][0]), 2)) / (2 * math.pow(contData.get("elevation")[1][1], 2)))
    p3=(1 / (math.sqrt(2 * math.pi) * contData.get("aspect")[1][1])) * math.exp(
    (-math.pow((r[2] - contData.get("aspect")[1][0]), 2)) / (2 * math.pow(contData.get("aspect")[1][1], 2)))
    p4=(1 / (math.sqrt(2 * math.pi) * contData.get("hDFpts")[1][1])) * math.exp(
    (-math.pow((r[10] - contData.get("hDFpts")[1][0]), 2)) / (2 * math.pow(contData.get("hDFpts")[1][1], 2)))
    p5=(1 / (math.sqrt(2 * math.pi) * contData.get("hilsde12")[1][1])) * math.exp(
    (-math.pow((r[8] - contData.get("hilsde12")[1][0]), 2)) / (2 * math.pow(contData.get("hilsde12")[1][1], 2)))
    p6=(1 / (math.sqrt(2 * math.pi) * contData.get("hilsde3")[1][1])) * math.exp(
    (-math.pow((r[9] - contData.get("hilsde3")[1][0]), 2)) / (2 * math.pow(contData.get("hilsde3")[1][1], 2)))
    p7=(1 / (math.sqrt(2 * math.pi) * contData.get("hDrodways")[1][1])) * math.exp(
    (-math.pow((r[6] - contData.get("hDrodways")[1][0]), 2)) / (2 * math.pow(contData.get("hDrodways")[1][1], 2)))
    p8=(1 / (math.sqrt(2 * math.pi) * contData.get("hilsde9")[1][1])) * math.exp(
    (-math.pow((r[7] - contData.get("hilsde9")[1][0]), 2)) / (2 * math.pow(contData.get("hilsde9")[1][1], 2)))
    p9=(1 / (math.sqrt(2 * math.pi) * contData.get("vDhydr")[1][1])) * math.exp(
    (-math.pow((r[5] - contData.get("vDhydr")[1][0]), 2)) / (2 * math.pow(contData.get("vDhydr")[1][1], 2)))
    p10=(1 / (math.sqrt(2 * math.pi) * contData.get("hDhydro")[1][1])) * math.exp(
    (-math.pow((r[4] - contData.get("hDhydro")[1][0]), 2)) / (2 * math.pow(contData.get("hDhydro")[1][1], 2)))

    pl =[p1,p2,p4,p7,p8,p9,p10]
    ct2=1
    for k in pl:
        if k==0:
            k=1
        ct2=ct2*k

    #ct2=ct2* ctProb[1] *lkProbSt[1][idx_st] * lkProbWa[1][idx_wa]

    p1=(1 / (math.sqrt(2 * math.pi) * contData.get("slope")[2][1])) * math.exp(
    (-math.pow((r[3] - contData.get("slope")[2][0]), 2)) / (2 * math.pow(contData.get("slope")[2][1], 2)))
    p2=(1 / (math.sqrt(2 * math.pi) * contData.get("elevation")[2][1])) * math.exp(
    (-math.pow((r[1] - contData.get("elevation")[2][0]), 2)) / (2 * math.pow(contData.get("elevation")[2][1], 2)))
    p3=(1 / (math.sqrt(2 * math.pi) * contData.get("aspect")[2][1])) * math.exp(
    (-math.pow((r[2] - contData.get("aspect")[2][0]), 2)) / (2 * math.pow(contData.get("aspect")[2][1], 2)))
    p4=(1 / (math.sqrt(2 * math.pi) * contData.get("hDFpts")[2][1])) * math.exp(
    (-math.pow((r[10] - contData.get("hDFpts")[2][0]), 2)) / (2 * math.pow(contData.get("hDFpts")[2][1], 2)))
    p5=(1 / (math.sqrt(2 * math.pi) * contData.get("hilsde12")[2][1])) * math.exp(
    (-math.pow((r[8] - contData.get("hilsde12")[2][0]), 2)) / (2 * math.pow(contData.get("hilsde12")[2][1], 2)))
    p6=(1 / (math.sqrt(2 * math.pi) * contData.get("hilsde3")[2][1])) * math.exp(
    (-math.pow((r[9] - contData.get("hilsde3")[2][0]), 2)) / (2 * math.pow(contData.get("hilsde3")[2][1], 2)))
    p7=(1 / (math.sqrt(2 * math.pi) * contData.get("hDrodways")[2][1])) * math.exp(
    (-math.pow((r[6] - contData.get("hDrodways")[2][0]), 2)) / (2 * math.pow(contData.get("hDrodways")[2][1], 2)))
    p8=(1 / (math.sqrt(2 * math.pi) * contData.get("hilsde9")[2][1])) * math.exp(
    (-math.pow((r[7] - contData.get("hilsde9")[2][0]), 2)) / (2 * math.pow(contData.get("hilsde9")[2][1], 2)))
    p9=(1 / (math.sqrt(2 * math.pi) * contData.get("vDhydr")[2][1])) * math.exp(
    (-math.pow((r[5] - contData.get("vDhydr")[2][0]), 2)) / (2 * math.pow(contData.get("vDhydr")[2][1], 2)))
    p10=(1 / (math.sqrt(2 * math.pi) * contData.get("hDhydro")[2][1])) * math.exp(
    (-math.pow((r[4] - contData.get("hDhydro")[2][0]), 2)) / (2 * math.pow(contData.get("hDhydro")[2][1], 2)))
    pl =[p1,p2,p4,p7,p8,p9,p10]
    ct3=1
    for k in pl:
        if k==0:
            k=1
        ct3=ct3*k
    #ct3=ct3* ctProb[2] *lkProbSt[2][idx_st] * lkProbWa[2][idx_wa]

    p1=(1 / (math.sqrt(2 * math.pi) * contData.get("slope")[3][1])) * math.exp(
    (-math.pow((r[3] - contData.get("slope")[3][0]), 2)) / (2 * math.pow(contData.get("slope")[3][1], 2)))
    p2=(1 / (math.sqrt(2 * math.pi) * contData.get("elevation")[3][1])) * math.exp(
    (-math.pow((r[1] - contData.get("elevation")[3][0]), 2)) / (2 * math.pow(contData.get("elevation")[3][1], 2)))
    p3=(1 / (math.sqrt(2 * math.pi) * contData.get("aspect")[3][1])) * math.exp(
    (-math.pow((r[2] - contData.get("aspect")[3][0]), 2)) / (2 * math.pow(contData.get("aspect")[3][1], 2)))
    p4=(1 / (math.sqrt(2 * math.pi) * contData.get("hDFpts")[3][1])) * math.exp(
    (-math.pow((r[10] - contData.get("hDFpts")[3][0]), 2)) / (2 * math.pow(contData.get("hDFpts")[3][1], 2)))
    p5=(1 / (math.sqrt(2 * math.pi) * contData.get("hilsde12")[3][1])) * math.exp(
    (-math.pow((r[8] - contData.get("hilsde12")[3][0]), 2)) / (2 * math.pow(contData.get("hilsde12")[3][1], 2)))
    p6=(1 / (math.sqrt(2 * math.pi) * contData.get("hilsde3")[3][1])) * math.exp(
    (-math.pow((r[9] - contData.get("hilsde3")[3][0]), 2)) / (2 * math.pow(contData.get("hilsde3")[3][1], 2)))
    p7=(1 / (math.sqrt(2 * math.pi) * contData.get("hDrodways")[3][1])) * math.exp(
    (-math.pow((r[6] - contData.get("hDrodways")[3][0]), 2)) / (2 * math.pow(contData.get("hDrodways")[3][1], 2)))
    p8=(1 / (math.sqrt(2 * math.pi) * contData.get("hilsde9")[3][1])) * math.exp(
    (-math.pow((r[7] - contData.get("hilsde9")[3][0]), 2)) / (2 * math.pow(contData.get("hilsde9")[3][1], 2)))
    p9=(1 / (math.sqrt(2 * math.pi) * contData.get("vDhydr")[3][1])) * math.exp(
    (-math.pow((r[5] - contData.get("vDhydr")[3][0]), 2)) / (2 * math.pow(contData.get("vDhydr")[3][1], 2)))
    p10=(1 / (math.sqrt(2 * math.pi) * contData.get("hDhydro")[3][1])) * math.exp(
    (-math.pow((r[4] - contData.get("hDhydro")[3][0]), 2)) / (2 * math.pow(contData.get("hDhydro")[3][1], 2)))

    pl =[p1,p2,p4,p7,p8,p9,p10]
    ct4=1
    for k in pl:
        if k==0:
            k=1
        ct4=ct4*k

    #ct4=ct4*ctProb[3]  * lkProbSt[3][idx_st] * lkProbWa[3][idx_wa]

    p1=(1 / (math.sqrt(2 * math.pi) * contData.get("slope")[4][1])) * math.exp(
    (-math.pow((r[3] - contData.get("slope")[4][0]), 2)) / (2 * math.pow(contData.get("slope")[4][1], 2)))

    p2=(1 / (math.sqrt(2 * math.pi) * contData.get("elevation")[4][1])) * math.exp(
    (-math.pow((r[1] - contData.get("elevation")[4][0]), 2)) / (2 * math.pow(contData.get("elevation")[4][1], 2)))

    p3=(1 / (math.sqrt(2 * math.pi) * contData.get("aspect")[4][1])) * math.exp(
    (-math.pow((r[2] - contData.get("aspect")[4][0]), 2)) / (2 * math.pow(contData.get("aspect")[4][1], 2)))

    p4=(1 / (math.sqrt(2 * math.pi) * contData.get("hDFpts")[4][1])) * math.exp(
    (-math.pow((r[10] - contData.get("hDFpts")[4][0]), 2)) / (2 * math.pow(contData.get("hDFpts")[4][1], 2)))

    p5=(1 / (math.sqrt(2 * math.pi) * contData.get("hilsde12")[4][1])) * math.exp(
    (-math.pow((r[8] - contData.get("hilsde12")[4][0]), 2)) / (2 * math.pow(contData.get("hilsde12")[4][1], 2)))

    p6=(1 / (math.sqrt(2 * math.pi) * contData.get("hilsde3")[4][1])) * math.exp(
    (-math.pow((r[9] - contData.get("hilsde3")[4][0]), 2)) / (2 * math.pow(contData.get("hilsde3")[4][1], 2)))

    p7=(1 / (math.sqrt(2 * math.pi) * contData.get("hDrodways")[4][1])) * math.exp(
    (-math.pow((r[6] - contData.get("hDrodways")[4][0]), 2)) / (2 * math.pow(contData.get("hDrodways")[4][1], 2)))

    p8=(1 / (math.sqrt(2 * math.pi) * contData.get("hilsde9")[4][1])) * math.exp(
    (-math.pow((r[7] - contData.get("hilsde9")[4][0]), 2)) / (2 * math.pow(contData.get("hilsde9")[4][1], 2)))

    p9=(1 / (math.sqrt(2 * math.pi) * contData.get("vDhydr")[4][1])) * math.exp(
    (-math.pow((r[5] - contData.get("vDhydr")[4][0]), 2)) / (2 * math.pow(contData.get("vDhydr")[4][1], 2)))

    p10=(1 / (math.sqrt(2 * math.pi) * contData.get("hDhydro")[4][1])) * math.exp(
    (-math.pow((r[4] - contData.get("hDhydro")[4][0]), 2)) / (2 * math.pow(contData.get("hDhydro")[4][1], 2)))

    pl =[p1,p2,p4,p7,p8,p9,p10]
    ct5=1
    for k in pl:
        if k==0:
            k=1

        ct5=ct5*k

    #ct5=ct5*ctProb[4] * lkProbSt[4][idx_st] * lkProbWa[4][idx_wa]

    p1=(1 / (math.sqrt(2 * math.pi) * contData.get("slope")[5][1])) * math.exp(
    (-math.pow((r[3] - contData.get("slope")[5][0]), 2)) / (2 * math.pow(contData.get("slope")[5][1], 2)))

    p2=(1 / (math.sqrt(2 * math.pi) * contData.get("elevation")[5][1])) * math.exp(
    (-math.pow((r[1] - contData.get("elevation")[5][0]), 2)) / (2 * math.pow(contData.get("elevation")[5][1], 2)))

    p3=(1 / (math.sqrt(2 * math.pi) * contData.get("aspect")[5][1])) * math.exp(
    (-math.pow((r[2] - contData.get("aspect")[5][0]), 2)) / (2 * math.pow(contData.get("aspect")[5][1], 2)))

    p4=(1 / (math.sqrt(2 * math.pi) * contData.get("hDFpts")[5][1])) * math.exp(
    (-math.pow((r[10] - contData.get("hDFpts")[5][0]), 2)) / (2 * math.pow(contData.get("hDFpts")[5][1], 2)))

    p5=(1 / (math.sqrt(2 * math.pi) * contData.get("hilsde12")[5][1])) * math.exp(
    (-math.pow((r[8] - contData.get("hilsde12")[5][0]), 2)) / (2 * math.pow(contData.get("hilsde12")[5][1], 2)))

    p6=(1 / (math.sqrt(2 * math.pi) * contData.get("hilsde3")[5][1])) * math.exp(
    (-math.pow((r[9] - contData.get("hilsde3")[5][0]), 2)) / (2 * math.pow(contData.get("hilsde3")[5][1], 2)))

    p7=(1 / (math.sqrt(2 * math.pi) * contData.get("hDrodways")[5][1])) * math.exp(
    (-math.pow((r[6] - contData.get("hDrodways")[5][0]), 2)) / (2 * math.pow(contData.get("hDrodways")[5][1], 2)))

    p8=(1 / (math.sqrt(2 * math.pi) * contData.get("hilsde9")[5][1])) * math.exp(
    (-math.pow((r[7] - contData.get("hilsde9")[5][0]), 2)) / (2 * math.pow(contData.get("hilsde9")[5][1], 2)))

    p9=(1 / (math.sqrt(2 * math.pi) * contData.get("vDhydr")[5][1])) * math.exp(
    (-math.pow((r[5] - contData.get("vDhydr")[5][0]), 2)) / (2 * math.pow(contData.get("vDhydr")[5][1], 2)))

    p10=(1 / (math.sqrt(2 * math.pi) * contData.get("hDhydro")[5][1])) * math.exp(
    (-math.pow((r[4] - contData.get("hDhydro")[5][0]), 2)) / (2 * math.pow(contData.get("hDhydro")[5][1], 2)))

    pl =[p1,p2,p4,p7,p8,p9,p10]
    ct6=1
    for k in pl:
        if k==0:
            k=1
        ct6=ct6*k

    #ct6=ct6* ctProb[5] * lkProbSt[5][idx_st] * lkProbWa[5][idx_wa]

    p1=(1 / (math.sqrt(2 * math.pi) * contData.get("slope")[6][1])) * math.exp((-math.pow((r[3] - contData.get("slope")[6][0]), 2)) / (2 * math.pow(contData.get("slope")[6][1], 2)))

    p2=(1 / (math.sqrt(2 * math.pi) * contData.get("elevation")[6][1])) * math.exp((-math.pow((r[1] - contData.get("elevation")[6][0]), 2)) / (2 * math.pow(contData.get("elevation")[6][1], 2)))

    p3=(1 / (math.sqrt(2 * math.pi) * contData.get("aspect")[6][1])) * math.exp(
    (-math.pow((r[2] - contData.get("aspect")[6][0]), 2)) / (2 * math.pow(contData.get("aspect")[6][1], 2)))

    p4=(1 / (math.sqrt(2 * math.pi) * contData.get("hDFpts")[6][1])) * math.exp(
    (-math.pow((r[10] - contData.get("hDFpts")[6][0]), 2)) / (2 * math.pow(contData.get("hDFpts")[6][1], 2)))

    p5=(1 / (math.sqrt(2 * math.pi) * contData.get("hilsde12")[6][1])) * math.exp(
    (-math.pow((r[8] - contData.get("hilsde12")[6][0]), 2)) / (2 * math.pow(contData.get("hilsde12")[6][1], 2)))

    p6=(1 / (math.sqrt(2 * math.pi) * contData.get("hilsde3")[6][1])) * math.exp(
    (-math.pow((r[9] - contData.get("hilsde3")[6][0]), 2)) / (2 * math.pow(contData.get("hilsde3")[6][1], 2)))

    p7=(1 / (math.sqrt(2 * math.pi) * contData.get("hDrodways")[6][1])) * math.exp(
    (-math.pow((r[6] - contData.get("hDrodways")[6][0]), 2)) / (2 * math.pow(contData.get("hDrodways")[6][1], 2)))

    p8=(1 / (math.sqrt(2 * math.pi) * contData.get("hilsde9")[6][1])) * math.exp(
    (-math.pow((r[7] - contData.get("hilsde9")[6][0]), 2)) / (2 * math.pow(contData.get("hilsde9")[6][1], 2)))

    p9=(1 / (math.sqrt(2 * math.pi) * contData.get("vDhydr")[6][1])) * math.exp(
    (-math.pow((r[5] - contData.get("vDhydr")[6][0]), 2)) / (2 * math.pow(contData.get("vDhydr")[6][1], 2)))

    p10=(1 / (math.sqrt(2 * math.pi) * contData.get("hDhydro")[6][1])) * math.exp(
    (-math.pow((r[4] - contData.get("hDhydro")[6][0]), 2)) / (2 * math.pow(contData.get("hDhydro")[6][1], 2)))

    pl =[p1,p2,p4,p7,p8,p9,p10]
    ct7=1
    for k in pl:
        if k==0:
            k=1
        ct7=ct7*k

    #ct7=ct7*ctProb[6] * lkProbSt[6][idx_st] * lkProbWa[6][idx_wa]
    # Normalize the data
    sumVal = ct1 + ct2 + ct3 + ct4 + ct5 + ct6 + ct7
    if (sumVal==0):
      return r[-1],0
    ct1 = ct1 / sumVal
    ct2 = ct2 / sumVal
    ct3 = ct3 / sumVal
    ct4 = ct4 / sumVal
    ct5 = ct5 / sumVal
    ct6 = ct6 / sumVal
    ct7 = ct7 / sumVal
    m = ct1
    tp = 1
    if ct2 > m:
        m = ct2
        tp = 2
    elif ct3 > m:
        m = ct3
        tp = 3
    elif ct4 > m:
        m = ct4
        tp = 4
    elif ct5 > m:
        m = ct5
        tp = 5
    elif ct6 > m:
        m = ct6
        tp = 6
    elif ct7 > m:
        m = ct7
        tp = 7
    # actual, predicted
    return r[-1],tp


def meanAndStdCalc(rddCT, col):
    if rddCT.count() != 0:
        result = (float(numpy.mean(rddCT.map(lambda x: x[col]).collect())), float(numpy.std(rddCT.map(lambda x: x[col]).collect())))
    else:
        return 0, 0
    return result

def accuracy(x,y):
    x= int(round(x))
    y=int(y)

    if (x == y):
      return 1
    else:
      return 0

if __name__ == "__main__":
    sc = SparkContext(appName="NBUserInput")

    # import pandas #provides data structures to quickly analyze data
    # Since this code runs on Kaggle server, train data can be accessed directly in the 'input' folder
    dataset = sc.textFile("Train.csv")
    #dataset = sc.textFile(sys.argv[1])
    train, test = dataset.randomSplit([0.8, 0.2])
    # header = dataset.first()
    # dataset = dataset.filter(row => row != header)
    data = train.map(lambda l: l.split(','))
    # xvalRDD = yxinputFile.map(lambda x: x.split(',')).map(lambda value: value[1:]).persist()
    # total records count
    n = data.count() - 1
    print ("Total count of records :",n)

    # coverType Count
    coverTypeCount = []
    dataCoverType = data.map(lambda x: x[-1]).persist()
    coverTypeCount.append(dataCoverType.filter(lambda x: x in '1').count())
    coverTypeCount.append(dataCoverType.filter(lambda x: x in '2').count())
    coverTypeCount.append(dataCoverType.filter(lambda x: x in '3').count())
    coverTypeCount.append(dataCoverType.filter(lambda x: x in '4').count())
    coverTypeCount.append(dataCoverType.filter(lambda x: x in '5').count())
    coverTypeCount.append(dataCoverType.filter(lambda x: x in '6').count())
    coverTypeCount.append(dataCoverType.filter(lambda x: x in '7').count())
    print ("count based on coverType")
    print(coverTypeCount)
    conditionalSoilTpCount = numpy.zeros((7, 40), dtype=float)
    # conditional proability  for soilType

    for ct in range(7):
        for st in range(40):
            conditionalSoilTpCount[ct][st] = data.filter(lambda r: r[-1] in str(ct + 1)).filter(
                lambda s: s[st + 15] in '1').count()
    #print ("count based on coverType and Soil Type")
    #print (conditionalSoilTpCount)

    conditionalWilCount = numpy.zeros((7, 4), dtype=float)
    # conditional proability  for soilType
    for ct in range(7):
        for st in range(4):
            conditionalWilCount[ct][st] = data.filter(lambda r: r[-1] in str(ct + 1)).filter(
                lambda s: s[st + 11] in '1').count()
    #print ("count based on coverType and wilderness")
    #print (conditionalWilCount)

    # count of soilType idx=15 and 40 types are present

    soilTypeCount = []

    for i in range(15, 55, 1):
        soilTypeCount.append(data.map(lambda x: x[i]).filter(lambda x: x in '1').count())

    #print ("count based on soilType ")
    #print (soilTypeCount)
    # countCT_1= dataCovType.filter(lambda x: x in '5')
    wildernessCount = []
    for i in range(11, 15, 1):
        wildernessCount.append(data.map(lambda x: x[i]).filter(lambda x: x in '1').count())

    #print (wildernessCount)

    length = len(coverTypeCount)
    #print (length)

    # probability of coverType
    coverTypeProbability = []
    for j in range(length):
        val = float(coverTypeCount[j]) / n
        # coverTypeProbability.insert(x,val)
        coverTypeProbability.append(val)
    print ("Probability of coverType P(CT_):")
    print (coverTypeProbability)

    # probability of wilderness area
    wildernessareaProbability = []
    for j in range(len(wildernessCount)):
        val = float(wildernessCount[j]) / n
        # coverTypeProbability.insert(x,val)
        wildernessareaProbability.append(val)
    # print ("total count")
    # print (wildernessareaProbability)

    # probability of soil type
    soiltypeProbability = []
    for j in range(len(soilTypeCount)):
        val = float(soilTypeCount[j]) / n
        # coverTypeProbability.insert(x,val)
        soiltypeProbability.append(val)

    #print (soiltypeProbability)

    # probability of conditional count

    likeProbSt = numpy.zeros((7, 40), dtype=float)
    for ct in range(7):
        for st in range(40):
           # if conditionalSoilTpCount[ct][st] != 0 and coverTypeCount[ct] != 0:
             likeProbSt[ct][st] = (float(conditionalSoilTpCount[ct][st]+1) / ((coverTypeCount[ct])+n))
           # else:
               # likeProbSt[ct][st] = float(0)
    print ("Conditional Probability  of soilType | CoverType")
    print (likeProbSt)
    likeProbWil = numpy.zeros((7, 4), dtype=float)
    for ct in range(7):
        for st in range(4):
           # if conditionalWilCount[ct][st] != 0 and coverTypeCount[ct] != 0:
             likeProbWil[ct][st] = (float(conditionalWilCount[ct][st]+1) / ((coverTypeCount[ct])+n))
           # else:
             #   likeProbWil[ct][st] = float(0)
    print ("Conditional Probability  of Wilderness Area | CoverType")
    print (likeProbWil)

    # Elevation

    # grab the column
    # rdd for calculation
    ready_dataset = data.map(lambda r: (
    r[-1], float(r[1]), float(r[2]),float(r[3]), float(r[4]), float(r[5]), float(r[6]), float(r[7]), float(r[8]),
    float(r[9]), float(r[10])))

    rdd_CT1 = ready_dataset.filter(lambda r: r[0] in '1')
    rdd_CT2 = ready_dataset.filter(lambda r: r[0] in '2')
    rdd_CT3 = ready_dataset.filter(lambda r: r[0] in '3')
    rdd_CT4 = ready_dataset.filter(lambda r: r[0] in '4')
    rdd_CT5 = ready_dataset.filter(lambda r: r[0] in '5')
    rdd_CT6 = ready_dataset.filter(lambda r: r[0] in '6')
    rdd_CT7 = ready_dataset.filter(lambda r: r[0] in '7')
    # meanAndStdCalc(rddCT, col,coverTypeCount)
    continuousVarData = {}

    elevation_CT1 = meanAndStdCalc(rdd_CT1, 1)
    elevation_CT2 = meanAndStdCalc(rdd_CT2, 1)
    elevation_CT3 = meanAndStdCalc(rdd_CT3, 1)
    elevation_CT4 = meanAndStdCalc(rdd_CT4, 1)
    elevation_CT5 = meanAndStdCalc(rdd_CT5, 1)
    elevation_CT6 = meanAndStdCalc(rdd_CT6, 1)
    elevation_CT7 = meanAndStdCalc(rdd_CT7, 1)
    continuousVarData["elevation"] = [elevation_CT1, elevation_CT2, elevation_CT3, elevation_CT4, elevation_CT5,
                                      elevation_CT6, elevation_CT7]

    aspect_CT1 = meanAndStdCalc(rdd_CT1, 2)
    aspect_CT2 = meanAndStdCalc(rdd_CT2, 2)
    aspect_CT3 = meanAndStdCalc(rdd_CT3, 2)
    aspect_CT4 = meanAndStdCalc(rdd_CT4, 2)
    aspect_CT5 = meanAndStdCalc(rdd_CT5, 2)
    aspect_CT6 = meanAndStdCalc(rdd_CT6, 2)
    aspect_CT7 = meanAndStdCalc(rdd_CT7, 2)
    continuousVarData["aspect"] = [aspect_CT1, aspect_CT2, aspect_CT3, aspect_CT4, aspect_CT5, aspect_CT6, aspect_CT7]

    slope_CT1 = meanAndStdCalc(rdd_CT1, 3)
    slope_CT2 = meanAndStdCalc(rdd_CT2, 3)
    slope_CT3 = meanAndStdCalc(rdd_CT3, 3)
    slope_CT4 = meanAndStdCalc(rdd_CT4, 3)
    slope_CT5 = meanAndStdCalc(rdd_CT5, 3)
    slope_CT6 = meanAndStdCalc(rdd_CT6, 3)
    slope_CT7 = meanAndStdCalc(rdd_CT7, 3)
    continuousVarData["slope"] = [slope_CT1, slope_CT2, slope_CT3, slope_CT4, slope_CT5,
                                  slope_CT6, slope_CT7]

    hDhydro_CT1 = meanAndStdCalc(rdd_CT1, 4)
    hDhydro_CT2 = meanAndStdCalc(rdd_CT2, 4)
    hDhydro_CT3 = meanAndStdCalc(rdd_CT3, 4)
    hDhydro_CT4 = meanAndStdCalc(rdd_CT4, 4)
    hDhydro_CT5 = meanAndStdCalc(rdd_CT5, 4)
    hDhydro_CT6 = meanAndStdCalc(rdd_CT6, 4)
    hDhydro_CT7 = meanAndStdCalc(rdd_CT7, 4)
    continuousVarData["hDhydro"] = [hDhydro_CT1, hDhydro_CT2, hDhydro_CT3, hDhydro_CT4, hDhydro_CT5,
                                    hDhydro_CT6, hDhydro_CT7]

    vDhydr_CT1 = meanAndStdCalc(rdd_CT1, 5)
    vDhydr_CT2 = meanAndStdCalc(rdd_CT2, 5)
    vDhydr_CT3 = meanAndStdCalc(rdd_CT3, 5)
    vDhydr_CT4 = meanAndStdCalc(rdd_CT4, 5)
    vDhydr_CT5 = meanAndStdCalc(rdd_CT5, 5)
    vDhydr_CT6 = meanAndStdCalc(rdd_CT6, 5)
    vDhydr_CT7 = meanAndStdCalc(rdd_CT7, 5)
    continuousVarData["vDhydr"] = [vDhydr_CT1, vDhydr_CT2, vDhydr_CT3, vDhydr_CT4, vDhydr_CT5,
                                   vDhydr_CT6, vDhydr_CT7]

    hDrodways_CT1 = meanAndStdCalc(rdd_CT1, 6)
    hDrodways_CT2 = meanAndStdCalc(rdd_CT2, 6)
    hDrodways_CT3 = meanAndStdCalc(rdd_CT3, 6)
    hDrodways_CT4 = meanAndStdCalc(rdd_CT4, 6)
    hDrodways_CT5 = meanAndStdCalc(rdd_CT5, 6)
    hDrodways_CT6 = meanAndStdCalc(rdd_CT6, 6)
    hDrodways_CT7 = meanAndStdCalc(rdd_CT7, 6)
    continuousVarData["hDrodways"] = [hDrodways_CT1, hDrodways_CT2, hDrodways_CT3, hDrodways_CT4, hDrodways_CT5,
                                      hDrodways_CT6, hDrodways_CT7]

    hilsde9_CT1 = meanAndStdCalc(rdd_CT1, 7)
    hilsde9_CT2 = meanAndStdCalc(rdd_CT2, 7)
    hilsde9_CT3 = meanAndStdCalc(rdd_CT3, 7)
    hilsde9_CT4 = meanAndStdCalc(rdd_CT4, 7)
    hilsde9_CT5 = meanAndStdCalc(rdd_CT5, 7)
    hilsde9_CT6 = meanAndStdCalc(rdd_CT6, 7)
    hilsde9_CT7 = meanAndStdCalc(rdd_CT7, 7)
    continuousVarData["hilsde9"] = [hilsde9_CT1, hilsde9_CT2, hilsde9_CT3, hilsde9_CT4, hilsde9_CT5,
                                    hilsde9_CT6, hilsde9_CT7]

    hilsde12_CT1 = meanAndStdCalc(rdd_CT1, 8)
    hilsde12_CT2 = meanAndStdCalc(rdd_CT2, 8)
    hilsde12_CT3 = meanAndStdCalc(rdd_CT3, 8)
    hilsde12_CT4 = meanAndStdCalc(rdd_CT4, 8)
    hilsde12_CT5 = meanAndStdCalc(rdd_CT5, 8)
    hilsde12_CT6 = meanAndStdCalc(rdd_CT6, 8)
    hilsde12_CT7 = meanAndStdCalc(rdd_CT7, 8)
    continuousVarData["hilsde12"] = [hilsde12_CT1, hilsde12_CT2, hilsde12_CT3, hilsde12_CT4, hilsde12_CT5,
                                     hilsde12_CT6, hilsde12_CT7]

    hilsde3_CT1 = meanAndStdCalc(rdd_CT1, 9)
    hilsde3_CT2 = meanAndStdCalc(rdd_CT2, 9)
    hilsde3_CT3 = meanAndStdCalc(rdd_CT3, 9)
    hilsde3_CT4 = meanAndStdCalc(rdd_CT4, 9)
    hilsde3_CT5 = meanAndStdCalc(rdd_CT5, 9)
    hilsde3_CT6 = meanAndStdCalc(rdd_CT6, 9)
    hilsde3_CT7 = meanAndStdCalc(rdd_CT7, 9)
    continuousVarData["hilsde3"] = [hilsde3_CT1, hilsde3_CT2, hilsde3_CT3, hilsde3_CT4, hilsde3_CT5,
                                    hilsde3_CT6, hilsde3_CT7]

    hDFpts_CT1 = meanAndStdCalc(rdd_CT1, 10)
    hDFpts_CT2 = meanAndStdCalc(rdd_CT2, 10)
    hDFpts_CT3 = meanAndStdCalc(rdd_CT3, 10)
    hDFpts_CT4 = meanAndStdCalc(rdd_CT4, 10)
    hDFpts_CT5 = meanAndStdCalc(rdd_CT5, 10)
    hDFpts_CT6 = meanAndStdCalc(rdd_CT6, 10)
    hDFpts_CT7 = meanAndStdCalc(rdd_CT7, 10)
    continuousVarData["hDFpts"] = [hDFpts_CT1, hDFpts_CT2, hDFpts_CT3, hDFpts_CT4, hDFpts_CT5,
                                   hDFpts_CT6, hDFpts_CT7]
    print("The Mean and Standard Deviation of the Continuous Variables")
    print(continuousVarData) # This value is later used in gaussian formula to find the probability of a given Continuous data

    # continuousVarData['elevation'].get(0).idx[0]
    # coverType.idx[0]
    # Prob of Like sT n CT
    # Prob of Lie WA n CT
    # ct1 = ctProb[0] * lkProbSt[0][idx_st]*lkProbWa[0][idx_wa] * numpy.random.normal(contData.get("slope")[0][0],contData.get("slope")[0][1],r[3])* numpy.random.normal(contData.get("elevation")[0][0],contData.get("elevation")[0][1],r[1]) * numpy.random.normal(contData.get("aspect")[0][0],contData.get("aspect")[0][1],r[2]) * numpy.random.normal(contData.get("hDFpts")[0][0],contData.get("hDFpts")[0][1],r[10]) * numpy.random.normal(contData.get("hilsde12")[0][0],contData.get("hilsde12")[0][1],r[8]) * numpy.random.normal(contData.get("hilsde3")[0][0],contData.get("hilsde3")[0][1],r[9]) * numpy.random.normal(contData.get("hDrodways")[0][0],contData.get("hDrodways")[0][1],r[6]) * numpy.random.normal(contData.get("hilsde9")[0][0],contData.get("hilsde9")[0][1],r[7]) * numpy.random.normal(contData.get("vDhydr")[0][0],contData.get("vDhydr")[0][1],r[5]) * numpy.random.normal(contData.get("hDhydro")[0][0],contData.get("hDhydro")[0][1],r[4])


    #test_Data = sc.textFile("Test.csv")
    parsed_data = test.map(lambda r: r.split(','))
    fdata = parsed_data.map(lambda r: (
    r[0], float(r[1]),float(r[2]), float(r[3]), float(r[4]), float(r[4]), float(r[5]),float(r[6]), float(r[7]),
    float(r[8]), float(r[9]), r[10], r[11], r[12], r[13], r[14], r[15], r[16], r[17], r[18], r[19], r[20], r[21],
    r[22], r[23], r[24], r[25], r[26], r[27], r[28], r[29], r[30], r[31], r[32], r[33], r[34], r[35], r[36], r[37],
    r[38], r[39], r[40], r[41], r[42], r[43], r[44], r[45], r[46], r[47], r[48], r[49], r[50], r[51], r[52], r[53],
    r[54],float(r[-1])))
    #idx_wa=1
    #idx_st=3
    #contData=continuousVarData
    #lkProbWa = likeProbWil
    #lkProbSt = likeProbSt
    #ctProb = coverTypeProbability
    #r= fdata.first()
    resultdata = fdata.map(lambda r: typeClassification(r, continuousVarData, coverTypeProbability, likeProbWil, likeProbSt))
    print("The actual and  predicted values are:")
    print(resultdata.collect())
    acc=resultdata.map(lambda x: accuracy(float(x[0]),float(x[1]))).filter(lambda x: x==1).count()
    print("The accuracy of the model ")
    print((acc/fdata.count())*100)
    ##resultdata.saveAsTextFile("C:\Users\Rakesh Harish\PycharmProjects\Spark\FCResult.csv")
    # print (fdata.collect())from pyspark import SparkContext