from utility.Config import *
from utility.utils import *

class FailurePredictionLogistic():
    def __init__(self):
        self.predictors = ["smart_5_raw","smart_187_raw","smart_188_raw","smart_197_raw","smart_198_raw"]
        self.logreg = LogisticRegression()

    def split_data(self, df):
        new_df = df.toPandas()
        X = new_df[self.predictors]
        y = new_df.failure
        # X_train,X_test,y_train,y_test=
        return train_test_split(X,y,test_size=0.25,random_state=0)

    def logistic_regression(self, X_train,X_test,y_train,y_test):
        self.logreg.fit(X_train,y_train)
        y_pred=self.logreg.predict(X_test)
        cnf_matrix = metrics.confusion_matrix(y_test, y_pred)
        print("Confustion Matrix: ")
        print(cnf_matrix)
        # plt.show()
        return y_pred , cnf_matrix

    def create_heat_map(self,y_pred, y_test, cnf_matrix):
        class_names=[0,1] # name  of classes
        fig, ax = plt.subplots()
        tick_marks = np.arange(len(class_names))
        plt.xticks(tick_marks, class_names)
        plt.yticks(tick_marks, class_names)
        
        sns.heatmap(pd.DataFrame(cnf_matrix), annot=True, cmap="YlGnBu" ,fmt='g')
        ax.xaxis.set_label_position("top")
        plt.tight_layout()
        plt.title('Confusion matrix', y=1.1)
        plt.ylabel('Actual label')
        plt.xlabel('Predicted label')
        plt.show()
        print("Accuracy:",metrics.accuracy_score(y_test, y_pred))
        print("Precision:",metrics.precision_score(y_test, y_pred))
        print("Recall:",metrics.recall_score(y_test, y_pred))
    
    def roc_curve(self,X_test, y_test):
        y_pred_proba = self.logreg.predict_proba(X_test)[::,1]
        fpr, tpr, _ = metrics.roc_curve(y_test,  y_pred_proba)
        auc = metrics.roc_auc_score(y_test, y_pred_proba)
        plt.plot(fpr,tpr,label="data 1, auc="+str(auc))
        plt.legend(loc=4)
        plt.show()
    
    
    def get_features(self,df):
        features = ["failure"] +  self.predictors
        return cache(get_select_columns(df, features))
 