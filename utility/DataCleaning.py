from utility.Config import *
from utility.utils import *

class DataCleaning:
    def __init__(self):
        self.filename = filename

    def convert_to_datetime(self,df):
        df = df.withColumn('date_time', 
                   to_date(unix_timestamp(col('date'), 'yyyy-MM-dd').cast("timestamp")))
        return cache(df)  

    def populate_MFG(self,df):
        match_udf = udf(matchManufacturer)
        df = df\
                .withColumn("MFG",lit(match_udf(split("model", " ")[0])))  
        return cache(df)
 
    def out_of_bounds_temperature(self, df):
        return cache(df.filter(df.smart_194_raw >= 18.0))

    def out_of_bounds_drive_days(self, df):
        return cache(df.filter(df.smart_9_raw > 18.0))

    def change_cols_to_float(self,df):
        list_smart_cols = get_smart_stats(df)   
        # list_smart_cols.append("failure")
        list_smart_cols.append("capacity_bytes")
        for c in list_smart_cols:
            df = df.withColumn(c,col(c).cast(FloatType()))
        df = df.withColumn("failure", col("failure").cast(IntegerType()))
        return cache(df)

    def drop_nan(self, df):
        aggregated_row = df.select([(count(when(col(c).isNull(), c))/df.count()).alias(c) for c in df.columns]).collect()
        aggregated_dict_list = [row.asDict() for row in aggregated_row]
        aggregated_dict = aggregated_dict_list[0]  
        col_null_g_90p=list({i for i in aggregated_dict if aggregated_dict[i] > 0.90})
        df = df.drop(*col_null_g_90p)
        return cache(df)

    def remove_normalised_features(self,df):
        raw_list = list({col for col in df.columns if 'normalized' in col})
        df = df.drop(*raw_list)
        return cache(df)

        
    def fill_missing_values(self, df):
        df = df.na.fill(0)
        return cache(df)