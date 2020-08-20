from utility.utils import *
import scipy as sp
from scipy.spatial import distance
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score, roc_auc_score, confusion_matrix
class MahalanobisBinaryClassifier():
    def __init__(self, xtrain, ytrain):
        self.xtrain_pos = xtrain.loc[ytrain == 1, :]
        self.xtrain_neg = xtrain.loc[ytrain == 0, :]

    def predict_proba(self, xtest):
        pos_neg_dists = [(p,n) for p, n in zip(mahalanobis(xtest, self.xtrain_pos), mahalanobis(xtest, self.xtrain_neg))]
        return np.array([(1-n/(p+n), 1-p/(p+n)) for p,n in pos_neg_dists])

    def predict(self, xtest):
        return np.array([np.argmax(row) for row in self.predict_proba(xtest)])


def mahalanobis(x=None, data=None, cov=None):
    """Compute the Mahalanobis Distance between each row of x and the data  
    x    : vector or matrix of data with, say, p columns.
    data : ndarray of the distribution from which Mahalanobis distance of each observation of x is to be computed.
    cov  : covariance matrix (p x p) of the distribution. If None, will be computed from data.
    """
    x_minus_mu = x - np.mean(data)
    if not cov:
        cov = np.cov(data.values.T)
    inv_covmat = sp.linalg.inv(cov)
    left_term = np.dot(x_minus_mu, inv_covmat)
    mahal = np.dot(left_term, x_minus_mu.T)
    return mahal.diagonal()
    
df = pd.read_csv('https://raw.githubusercontent.com/selva86/datasets/master/BreastCancer.csv', 
                 usecols=['Cl.thickness', 'Cell.size', 'Marg.adhesion', 
                          'Epith.c.size', 'Bare.nuclei', 'Bl.cromatin', 'Normal.nucleoli', 
                          'Mitoses', 'Class'])

df.dropna(inplace=True)  # drop missing values.
df.head()
xtrain, xtest, ytrain, ytest = train_test_split(df.drop('Class', axis=1), df['Class'], test_size=.3, random_state=100)

clf = MahalanobisBinaryClassifier(xtrain, ytrain)        
pred_probs = clf.predict_proba(xtest)
pred_class = clf.predict(xtest)

# Pred and Truth
pred_actuals = pd.DataFrame([(pred, act) for pred, act in zip(pred_class, ytest)], columns=['pred', 'true'])
print(pred_actuals[:5])        

truth = pred_actuals.loc[:, 'true']
pred = pred_actuals.loc[:, 'pred']
scores = np.array(pred_probs)[:, 1]
print("AUROC: ", roc_auc_score(truth, scores))
print("Confusion Matrix: \n")
print(confusion_matrix(truth, pred))
print("Accuracy Score: ")
print(accuracy_score(truth, pred))
print("Classification Report: \n" )
print(classification_report(truth, pred))




# xtrain_pos = xtrain.loc[ytrain == 1, :]
# xtrain_neg = xtrain.loc[ytrain == 0, :]
# # import pandas as pd
# # data = pd.read_csv('data/harddrive.csv')[:1000]

# # print(data.head())
# # data.to_csv('data/harddrive_.csv')

# from utility.utils import * 
# from pyspark.ml.classification import LogisticRegression
# import numpy as np
# from sklearn.model_selection import train_test_split


# some_df = spark.sparkContext.parallelize([
#  ("A", "no"),
#  ("B", "yes"),
#  ("B", "yes"),
#  ("B", "no")]
#  ).toDF(["user_id", "phone_number"])
# pandas_df = some_df.toPandas()
# print(pandas_df.head())
# # X, y = np.arange(10).reshape((5, 2)), range(5)
# # X_train, X_test, y_train, y_test = train_test_split( X, y, test_size=0.33, random_state=42)

# # logmodel = LogisticRegression()
# # logmodel.fit(X_train, y_train)
# # Predictions = logmodel.predict(X_test)
# # print(classification_report(y_test,predictions))