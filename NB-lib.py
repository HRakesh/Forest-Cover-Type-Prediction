import numpy as np
import sys
from sklearn.cross_validation import train_test_split
import pandas as pd
from sklearn.naive_bayes import GaussianNB
from sklearn.metrics import accuracy_score

df = pd.read_csv(sys.argv[1]) #'C:/Users/Ajinkya/Desktop/train.csv'
X = np.array(df.ix[:,1:55]) 	# end index is exclusive
y = np.array(df['Cover_Type']) 
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=42)

# initialize the model 
model = GaussianNB()


# Train the model using the training sets 
model.fit(X_train, y_train)

# predict the response
pred = model.predict(X_test)

diff = pred - y_test
from collections import Counter
s = Counter(diff)
acc = s[0]
print("The accuracy is:")
print(acc/len(y_test)*100)