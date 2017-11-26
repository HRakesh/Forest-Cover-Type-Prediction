from pyspark import SparkContext
import os
import sys
import numpy
# import re
# import string
# import pandas as pd
# from collections import Counter
if __name__ == "__main__":
    sc = SparkContext(appName="NBUserInput")

#import pandas #provides data structures to quickly analyze data
#Since this code runs on Kaggle server, train data can be accessed directly in the 'input' folder
    dataset =  sc.textFile("C:\Users\Rakesh Harish\UNCC Books\Cloud\Project\Data\Train.csv")

    data = dataset.map(lambda l: l.split(','))
    #xvalRDD = yxinputFile.map(lambda x: x.split(',')).map(lambda value: value[1:]).persist()
    # total records count
    n= data.count()-1
    print ("total count")
    print(n)
    # coverType Count
    coverTypeCount =[]
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
    conditionalSoilTpCount = numpy.zeros((7,40),dtype=float)
    # conditional proability  for soilType

    for ct in range(7):
        for st in range(40):
            conditionalSoilTpCount[ct][st]=data.filter(lambda r: r[-1] in str(ct+1)).filter(lambda s: s[st+15] in '1').count()
    print ("count based on coverType and Soil Type")
    print (conditionalSoilTpCount)


    conditionalWilCount = numpy.zeros((7,4),dtype=float)
    # conditional proability  for soilType
    for ct in range(7):
        for st in range(4):
            conditionalWilCount[ct][st]=data.filter(lambda r: r[-1] in str(ct+1)).filter(lambda s: s[st+11] in '1').count()
    print ("count based on coverType and wilderness")
    print (conditionalWilCount)


    #count of soilType idx=15 and 40 types are present

    soilTypeCount =[]

    for i in range(15,55,1):
        soilTypeCount.append(data.map(lambda x: x[i]).filter(lambda x: x in '1').count())

    print ("count based on soilType ")
    print (soilTypeCount)
    #countCT_1= dataCovType.filter(lambda x: x in '5')
    wildernessCount =[]
    for i in range(11,14,1):
        wildernessCount.append(data.map(lambda x: x[i]).filter(lambda x: x in '1').count())

    print (wildernessCount)

    length =len(coverTypeCount)
    print (length)

    #probability of coverType
    coverTypeProbability = []
    for j in range(length):
        val=float(coverTypeCount[j])/n
        #coverTypeProbability.insert(x,val)
        coverTypeProbability.append(val)
    print ("Probability of coverType P(CT)")
    print (coverTypeProbability)

    #probability of wilderness area
    wildernessareaProbability = []
    for j in range(len(wildernessCount)):
        val=float(wildernessCount[j])/n
        #coverTypeProbability.insert(x,val)
        wildernessareaProbability.append(val)
    #print ("total count")
    #print (wildernessareaProbability)

    #probability of soil type
    soiltypeProbability = []
    for j in range(len(soilTypeCount)):
        val=float(soilTypeCount[j])/n
        #coverTypeProbability.insert(x,val)
        soiltypeProbability.append(val)

    #print (soiltypeProbability)

    #probability of conditional count

    likeProbSt =numpy.zeros((7,40),dtype=float)
    for ct in range(7):
        for st in range(40):
            if conditionalSoilTpCount[ct][st]!=0 and coverTypeCount[ct]!=0:
                likeProbSt[ct][st]= float(conditionalSoilTpCount[ct][st])/coverTypeCount[ct]
            else:
                likeProbSt[ct][st]=float(0)
    print ("Probability of LikelHood of soilType and CoverType")
    print (likeProbSt)
    likeProbWil =numpy.zeros((7,4),dtype=float)
    for ct in range(7):
        for st in range(4):
            if conditionalWilCount[ct][st] != 0 and coverTypeCount[ct]!=0:
                likeProbWil[ct][st]= float(conditionalWilCount[ct][st])/coverTypeCount[ct]
            else:
                likeProbWil[ct][st]=float(0)

    print (likeProbWil)

    #Elevation

    # grab the column