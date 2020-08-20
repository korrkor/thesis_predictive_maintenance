from utility.Config import *
from utility.Spark import *
from utility.Logger import Logger
from pyspark.sql.types import *
import sys, os, glob
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
from pyspark.mllib.stat import Statistics
from pyspark.sql.functions import * 
from pyspark.ml.regression import GeneralizedLinearRegression
from pyspark.ml.feature import VectorAssembler, Imputer,StringIndexer, VectorAssembler
from pyspark.mllib.regression import LabeledPoint
from pyspark.ml.stat import ChiSquareTest
from sklearn.metrics import confusion_matrix 
from sklearn.metrics import accuracy_score 
from sklearn import metrics 
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report 
# import numpy as np
# import matplotlib.pyplot as plt
import seaborn as sns
# from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
# from sklearn.model_selection import train_test_split

spark = get_spark_session("")
spark.sparkContext.setLogLevel("FATAL")

def drop_nan(df):
    aggregated_row = df.select([(count(when(col(c).isNull(), c))/df.count()).alias(c) for c in df.columns]).collect()
    aggregated_dict_list = [row.asDict() for row in aggregated_row]
    aggregated_dict = aggregated_dict_list[0]  
    col_null_g_75p=list({i for i in aggregated_dict if aggregated_dict[i] > 0.75})
    df = df.drop(*col_null_g_75p).cache()
    return df

def cache(df):
    return df.cache() 

def mean_udf(v):
    return round(v.mean(), 2)

def read_csv(filename):
    customSchema = create_custom_schema()
    return cache(spark.read.csv(filename, header="true", schema=customSchema))

def write_df_to_file(df, uid):
    new_uid = append_id(filename, uid)
    df.write.csv(new_uid, header="true")

# def generate_id(size=7, chars=string.ascii_uppercase + string.digits):
#     return ''.join(random.choice(chars) for _ in range(size))

def append_id(filename, uid):
    name, ext = os.path.splitext(filename)
    return "{name}_{uid}{ext}".format(name=name, uid=uid, ext=ext)

def get_smart_stats(df):
    return [i for i in df.columns if i[:len('smart')]=='smart']

def get_col_as_list(df ,col_):
    return df.select(col_).rdd.flatMap(lambda x: x).collect()

def vector_assembler(df):
    smart_list  = get_smart_stats(df)
    assembler = VectorAssembler(inputCols=smart_list,outputCol="features")
    return assembler.transform(df)

def del_file():
    os.remove(filename)

def matchManufacturer(manufacturer):
    n={"HU":"HGST","Hitachi":"HGST","ST":"Seagate","WD":"WDC","MD":"Toshiba","HM":"HGST","HD":"HGST","HGST":"HGST","TOSHIBA":"TOSHIBA","SAMSUNG":"SAMSUNG","Samsung":"SAMSUNG"}
    for k in n:
        if str(manufacturer).startswith(k):
            return n[k]
        elif manufacturer == k:
            return n[k] 

def create_custom_schema():
    customSchema = StructType([
    StructField("date", StringType(), True),
    StructField("serial_number", StringType(), True),
    StructField("model", StringType(), True),
    StructField("capacity_bytes", DoubleType(), True),
    StructField("failure", DoubleType(), True),
    StructField("smart_1_normalized", DoubleType(), True),
    StructField("smart_1_raw", DoubleType(), True),
    StructField("smart_2_normalized", DoubleType(), True),
    StructField("smart_2_raw", DoubleType(), True),
    StructField("smart_3_normalized", DoubleType(), True),
    StructField("smart_3_raw", DoubleType(), True),
    StructField("smart_4_normalized", DoubleType(), True),
    StructField("smart_4_raw", DoubleType(), True),
    StructField("smart_5_normalized", DoubleType(), True),
    StructField("smart_5_raw", DoubleType(), True),
    StructField("smart_7_normalized", DoubleType(), True),
    StructField("smart_7_raw", DoubleType(), True),
    StructField("smart_8_normalized", DoubleType(), True),
    StructField("smart_8_raw", DoubleType(), True),
    StructField("smart_9_normalized", DoubleType(), True),
    StructField("smart_9_raw", DoubleType(), True),
    StructField("smart_10_normalized", DoubleType(), True),
    StructField("smart_10_raw", DoubleType(), True),
    StructField("smart_11_normalized", DoubleType(), True),
    StructField("smart_11_raw", DoubleType(), True),
    StructField("smart_12_normalized", DoubleType(), True),
    StructField("smart_12_raw", DoubleType(), True),
    StructField("smart_13_normalized", DoubleType(), True),
    StructField("smart_13_raw", DoubleType(), True),
    StructField("smart_15_normalized", DoubleType(), True),
    StructField("smart_15_raw", DoubleType(), True),
    StructField("smart_22_normalized", DoubleType(), True),
    StructField("smart_22_raw", DoubleType(), True),
    StructField("smart_177_normalized", DoubleType(), True),
    StructField("smart_177_raw", DoubleType(), True),
    StructField("smart_179_normalized", DoubleType(), True),
    StructField("smart_179_raw", DoubleType(), True),
    StructField("smart_181_normalized", DoubleType(), True),
    StructField("smart_181_raw", DoubleType(), True),
    StructField("smart_182_normalized", DoubleType(), True),
    StructField("smart_182_raw", DoubleType(), True),
    StructField("smart_183_normalized", DoubleType(), True),
    StructField("smart_183_raw", DoubleType(), True),
    StructField("smart_184_normalized", DoubleType(), True),
    StructField("smart_184_raw", DoubleType(), True),
    StructField("smart_187_normalized", DoubleType(), True),
    StructField("smart_187_raw", DoubleType(), True),
    StructField("smart_188_normalized", DoubleType(), True),
    StructField("smart_188_raw", DoubleType(), True),
    StructField("smart_189_normalized", DoubleType(), True),
    StructField("smart_189_raw", DoubleType(), True),
    StructField("smart_190_normalized", DoubleType(), True),
    StructField("smart_190_raw", DoubleType(), True),
    StructField("smart_191_normalized", DoubleType(), True),
    StructField("smart_191_raw", DoubleType(), True),
    StructField("smart_192_normalized", DoubleType(), True),
    StructField("smart_192_raw", DoubleType(), True),
    StructField("smart_193_normalized", DoubleType(), True),
    StructField("smart_193_raw", DoubleType(), True),
    StructField("smart_194_normalized", DoubleType(), True),
    StructField("smart_194_raw", DoubleType(), True),
    StructField("smart_195_normalized", DoubleType(), True),
    StructField("smart_195_raw", DoubleType(), True),
    StructField("smart_196_normalized", DoubleType(), True),
    StructField("smart_196_raw", DoubleType(), True),
    StructField("smart_197_normalized", DoubleType(), True),
    StructField("smart_197_raw", DoubleType(), True),
    StructField("smart_198_normalized", DoubleType(), True),
    StructField("smart_198_raw", DoubleType(), True),
    StructField("smart_199_normalized", DoubleType(), True),
    StructField("smart_199_raw", DoubleType(), True),
    StructField("smart_200_normalized", DoubleType(), True),
    StructField("smart_200_raw", DoubleType(), True),
    StructField("smart_201_normalized", DoubleType(), True),
    StructField("smart_201_raw", DoubleType(), True),
    StructField("smart_220_normalized", DoubleType(), True),
    StructField("smart_220_raw", DoubleType(), True),
    StructField("smart_222_normalized", DoubleType(), True),
    StructField("smart_222_raw", DoubleType(), True),
    StructField("smart_223_normalized", DoubleType(), True),
    StructField("smart_223_raw", DoubleType(), True),
    StructField("smart_224_normalized", DoubleType(), True),
    StructField("smart_224_raw", DoubleType(), True),
    StructField("smart_225_normalized", DoubleType(), True),
    StructField("smart_225_raw", DoubleType(), True),
    StructField("smart_226_normalized", DoubleType(), True),
    StructField("smart_226_raw", DoubleType(), True),
    StructField("smart_235_normalized", DoubleType(), True),
    StructField("smart_235_raw", DoubleType(), True),
    StructField("smart_240_normalized", DoubleType(), True),
    StructField("smart_240_raw", DoubleType(), True),
    StructField("smart_241_normalized", DoubleType(), True),
    StructField("smart_241_raw", DoubleType(), True),
    StructField("smart_242_normalized", DoubleType(), True),
    StructField("smart_242_raw", DoubleType(), True),
    StructField("smart_250_normalized", DoubleType(), True),
    StructField("smart_250_raw", DoubleType(), True),
    StructField("smart_251_normalized", DoubleType(), True),
    StructField("smart_251_raw", DoubleType(), True),
    StructField("smart_252_normalized", DoubleType(), True),
    StructField("smart_252_raw", DoubleType(), True),
    StructField("smart_254_normalized", DoubleType(), True),
    StructField("smart_254_raw", DoubleType(), True),
    StructField("smart_255_normalized", DoubleType(), True),
    StructField("smart_255_raw", DoubleType(), True)
    ])
    return customSchema

def check_feature_value(feature_value):
    return 1 if feature_value > 0 else 0

def change_cols_to_float(df):
    list_smart_cols = get_smart_stats(df)   
    list_smart_cols.append("failure")
    list_smart_cols.append("capacity_bytes")
    for c in list_smart_cols:
        df = df.withColumn(c,col(c).cast(FloatType()))
    return cache(df)

def get_select_columns(df, cols_list):
    return df.select([c for c in df.columns if c in cols_list])

