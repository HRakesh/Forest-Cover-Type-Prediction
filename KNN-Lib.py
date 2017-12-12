import numpy as np
from sklearn.cross_validation import train_test_split
import pandas as pd
import sys
df = pd.read_csv(sys.argv[1]) #'C:/Users/Ajinkya/Desktop/train.csv'
X = np.array(df.ix[:,1:55]) 	# end index is exclusive
y = np.array(df['Cover_Type']) 
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=42)
from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import accuracy_score

# instantiate learning model (k = 3)
knn = KNeighborsClassifier(n_neighbors=3)

# fitting the model
knn.fit(X_train, y_train)

# predict the response
pred = knn.predict(X_test)

# evaluate accuracy
#print (pred)
#print (len(y_test))
diff = pred - y_test
from collections import Counter
s = Counter(diff)
acc = s[0]
print("for k =3")
print(acc/len(y_test)*100)