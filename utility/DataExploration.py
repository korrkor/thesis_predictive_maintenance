import os
from utility.Config import *
from utility.utils import *

class DataExploration:
    def __init__(self):
        self.filename = append_id(filename,"cleaning")
        # self.df_target = read_csv(self.filename)

    def calculate_temperature_average(self,df_temp): 
        # smart_194_raw is temperature SMART Statistic
        # write_df_to_file(df_temp, "exploration")
        df_temp_avg = df_temp\
                                .groupBy(["MFG","model"])\
                                .agg(round(mean("smart_194_raw"),2).alias("avg_Temp"))\
                                .orderBy("avg_Temp") 
        model_list = get_col_as_list(df_temp_avg, df_temp_avg.model)
        correlation = self.correlation_temp_failure(df_temp, model_list)
        correlation.show()
    
    def correlation_temp_failure(self,df, model_list):
        # model_list = ["TOSHIBA MD04ABA400V"] 
        new_df = self.create_dataframe()
        for model in model_list:
            df_temp = df.filter(df.model == str(model))
            drive_age = self.get_drive_age_per_model(df_temp)
            stat = df_temp.stat.corr("smart_194_raw","failure")
            assembler = self.vector_assembler(df_temp)
            p_value = self.get_p_value(assembler)
            significance = str(self.significance(p_value))
            statistic = self.get_statistic(assembler)
            num_dead = self.num_failed_per_model(df_temp,str(model))
            num_alive = self.num_alive_per_model(df_temp, str(model))
            new_df = self.populate_df(new_df, model, stat, significance, p_value, num_dead, num_alive, drive_age)
        new_df = new_df.orderBy("p_value")
        return new_df
        

    def get_drive_age_per_model(self, df):
        df = df.select(((mean(df.smart_9_raw)/24)/365).alias("drive_age"))
        return df.select("drive_age").rdd.flatMap(lambda x: x).collect()[0] 
       
    def create_dataframe(self):
        schema = StructType([
        StructField("Model",StringType(),True),
        StructField("stat",DoubleType(),True),
        StructField("Significance",StringType(),True),
        StructField("p_value",DoubleType(),True),
        StructField("Num_dead",IntegerType(),True),
        StructField("Num_alive",IntegerType(),True),
        StructField("Drive_Age (years)",DoubleType(),True)
        ])
        return spark.createDataFrame([], schema)
    
    def populate_df(self,new_df, model, stat, significance, p_value, num_dead, num_alive, drive_age):
        newRow = spark.createDataFrame([(model, stat, significance, p_value, num_dead, num_alive,drive_age)])
        new_df = new_df.union(newRow)
        return new_df 


    def num_failed_per_model(self, df, model):
        df = df.filter((df.model == model) & (df.failure == 1) )
        df = df.select(countDistinct("serial_number").alias("distinct_model"))
        col_list = get_col_as_list(df,df.distinct_model)
        return  col_list[0]

    def num_alive_per_model(self, df, model):
        df = df.filter((df.model == model) & (df.failure == 0) )
        df = df.select(countDistinct("serial_number").alias("distinct_model"))
        col_list = get_col_as_list(df,df.distinct_model)
        return col_list[0]

    def significance(self,p_value):
        return (p_value <= 0.05)

    def vector_assembler(self,df):
        assembler = VectorAssembler(inputCols=['smart_194_raw'],outputCol="vector_features")
        vectorized_df = assembler.transform(df).select('failure', 'vector_features')
        r = ChiSquareTest.test(vectorized_df, "vector_features", "failure").head()
        return r

    def get_statistic(self, r):
        return str(r.statistics)

    def get_p_value(self,r):
        return float(r.pValues[0])
    
    def get_num_drives(self,df):
        df_temp = df.select(countDistinct("serial_number")).show()
        return df_temp
    
    def get_num_failed(self,df):
        print("Total number of failed drives ************************************: get_num_failed ")
        df_temp = df.filter(df.failure == 1)
        df_temp = df_temp.select(countDistinct("serial_number")).show()
        return df_temp
    
    def get_num_alive(self,df):
        df_temp = df.filter(df.failure == 0)
        df_temp = df_temp.select(countDistinct("serial_number")).show()
        return df_temp
