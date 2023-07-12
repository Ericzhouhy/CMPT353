import numpy as np
import pandas as pd
from skimage.color import lab2rgb
import sys
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import GaussianNB
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import FunctionTransformer
from skimage.color import rgb2lab
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler


labelled = pd.read_csv(sys.argv[1])
unlabelled = pd.read_csv(sys.argv[2])

X_temp = labelled.drop(['city', 'year'], axis=1)
y_temp = labelled["city"]
X_train, X_valid, y_train, y_valid = train_test_split(X_temp, y_temp)


bayes_model = make_pipeline(StandardScaler(),GaussianNB())
bayes_model.fit(X_train, y_train)

knn_model = make_pipeline(StandardScaler(),KNeighborsClassifier())
knn_model.fit(X_train, y_train)
print("Knn test:", knn_model.score(X_valid, y_valid))

rf_model = make_pipeline(StandardScaler(),RandomForestClassifier())
rf_model.fit(X_train, y_train)

unlabelled_data = unlabelled.drop(['city', 'year'], axis=1)
prediction = knn_model.predict(unlabelled_data)


pd.Series(prediction).to_csv(sys.argv[3], index=False, header=False)