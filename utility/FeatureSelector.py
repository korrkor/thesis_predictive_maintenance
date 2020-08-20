# numpy and pandas for data manipulation
import pandas as pd
import numpy as np
# model used for feature importances
import lightgbm as lgb
# utility for early stopping with a validation set
from sklearn.model_selection import train_test_split
# visualizations
import matplotlib.pyplot as plt
import seaborn as sns
# memory management
import gc
# utilities
from itertools import chain
import os

from utility.Config import *
from utility.Spark import *
from utility.utils import *


class FeatureSelector():
    def __init__(self, labels=None):
        self.spark = get_spark_session("hdsdsdsd")
        self.filename = append_id(filename,"exploration")
        # self.features = ["smart_5_raw","smart_187_raw","smart_188_raw","smart_197_raw","smart_198_raw"]
        # Dataset and optional training labels
        self.data = read_csv(self.filename)
        print("data**********************************")
        self.data.show()
        
    