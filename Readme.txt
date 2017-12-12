#################################################
FOREST COVER TYPE PREDICTION USING PYSPARK
#################################################

ITCS 6190 - Cloud Computing for Data Analysis

Project/Website Report Link: https://goo.gl/avqXos

Team Members
Harish, Rakesh (800984018)
Mohapatra, Ajinkya (800963333)
Subbiah, Sivalingam (800969237)

Required libraries and packages:

1) pyspark
2) math
3) numpy
4) pandas
5) sklearn
6) pandas
7) sys
8) sklearn


Steps to run:

1) Copy all the given code to the required directory

2) Following are the names of the files and their actions:

	NB.py - Naive Bayes implimentation from scratch, predicts the forest type value 1 to 7 and also accuracy
	NB-lib.py - Naive Bayes algorithm implemented using sklearn library

	KNN.py - K Nearest Neighbor implimentation from scratch for K values of 3,5,7,11,15
	KNN-lib.py - K Nearest Neighbor algorithm implemented using sklearn library

3) To run the file, follow the below command:
	
	1. NB.py and KNN.py
	
	spark-submit <python filename> <input filename> > <output filename>

	Example: spark-submit NB.py Data.csv > NBOutput.txt

	2. NB-lib.py and KNN-lib.py

	python <python filename> <input filename> > <output filename>
	
	Example: python NB.py Data.csv > NBlibOutput.txt

Note:
1) Make sure that the file is named as Data.csv
2) Data.csv file should be present in the same directory as the pyspark file
3) Output will be displayed in the console if the last part of the cmd is not provided in cmd prompt.
