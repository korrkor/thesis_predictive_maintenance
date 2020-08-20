import sys
from  utility.DataCleaning import * 
from  utility.DataExploration import * 
from  utility.FailurePredictionBaseline import *
from  utility.FailurePredictionLogistic import *
from  utility.utils import *


def main():
    print("maining")
    logger = Logger("Predictive Maintenance - DataCleaning")
    df = read_csv(filename)
    # df = df.repartition(7000)
    # df.show()
    #print("this is the partitions , ",df.rdd.getNumPartitions())
    #logger.info("DATA CLEANING *****************************************************")
    dc = DataCleaning()
#
    #logger.info("Populating Manufacturers *****************************")
    df_temp = dc.populate_MFG(df)
    df_temp.show()
    #logger.info("Filtering out seagate ***********************")
    #df_temp = df_temp.where(col("MFG").isin("Seagte"))
    ##df_temp.show()
    #logger.info("Converting to datetime *****************************")
    #df_temp = dc.convert_to_datetime(df_temp)
    #logger.info("Dropping columns with more than 90% nan *****************************")
    ##df_temp = dc.drop_nan(df_temp) 
    #logger.info("Filling null values **************************")
    #df_temp = dc.fill_missing_values(df_temp)
    #logger.info("Removing normalised features *****************************")
    #df_temp = dc.remove_normalised_features(df_temp)
    #logger.info("Removing out of bounds temperatures *****************************")
    #df_temp = dc.out_of_bounds_temperature(df_temp)
    #logger.info("Removing out of bounds drive days *****************************")
    #df_temp = dc.out_of_bounds_drive_days(df_temp)
    #logger.info("Changing cols to float *****************************")
    #df_temp = dc.change_cols_to_float(df_temp)
    ## write_df_to_file(df_temp,"cleaning")
#
    #logger = Logger("Predictive Maintenance - DataExploration")
    #logger.info("DATA EXPLORATION **************************************************************")
    #de = DataExploration()
#
    #logger.info("Total number of drives ************************************")
    ##de.get_num_drives(df_temp)
    #logger.info("Total number of alive drives ************************************")
    ##de.get_num_alive(df_temp) 
    #logger.info("Total number of failed drives ************************************")
    ##de.get_num_failed(df_temp)
    #logger.info("Caculating corr between temp and failure ************************************")
    ##de.calculate_temperature_average(df_temp)
#
#
    #logger = Logger("Predictive Maintenance - FailurePrediction(Baseline)")
    #logger.info("FAILURE PREDICTION **************************************************************")
    #fpb = FailurePredictionBaseline()
    #df_fpb = fpb.get_features(df_temp)
    #df_fpb = fpb.create_predicted_failure(df_fpb)
    ##fpb.baseline_performance(df_fpb)
#
#
    #logger = Logger("Predictive Maintenance - FailurePrediction(LogisticRegression)")
    #logger.info("Starting logistic Regression ******************************")
    #fpl = FailurePredictionLogistic()
    #logger.info("Splitting the data into test and train : 0.25 test ************************** ")
    #X_train,X_test,y_train,y_test= fpl.split_data(df_fpb)
    #logger.info("Training the logistic regression model and predicting failure***************")
    #y_pred, cnf_matrix= fpl.logistic_regression(X_train,X_test,y_train,y_test)
    #logger.info("Create heatmap of confusion matrix ********************** ")
    #fpl.create_heat_map(y_pred,y_test,cnf_matrix)
    #logger.info("Creating ROC curve of the logistic regression model  *******************")
    #fpl.roc_curve(X_test,y_test)
if __name__ == '__main__':
	main()

