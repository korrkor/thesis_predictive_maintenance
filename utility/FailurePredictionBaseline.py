from utility.Config import *
from utility.utils import *

class FailurePredictionBaseline():
    def __init__(self):
        self.predictors = ["smart_5_raw","smart_187_raw","smart_188_raw","smart_197_raw","smart_198_raw"]

    def baseline_performance(self, df):
        actual = map(int, get_col_as_list(df, "failure"))
        predicted = map(int, get_col_as_list(df, "predicted_failure"))
        results = confusion_matrix(actual, predicted)
        print 'Confusion Matrix :'
        print(results) 
        print('Accuracy Score :',accuracy_score(actual, predicted))
        print('Report : ')
        print(classification_report(actual, predicted)) 

    def create_predicted_failure(self,df):
        fv = udf(check_feature_value)
        for feature in self.predictors:
            df = df.withColumn("predicted_failure",lit(fv(df[feature])))
        return cache(df)
 
    def get_features(self,df):
        features = ["failure"] +  self.predictors
        return cache(get_select_columns(df, features))
 