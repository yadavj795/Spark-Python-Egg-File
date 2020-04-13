# coding=utf-8 
from __future__ import print_function
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession, SQLContext



from pyspark.sql.functions import udf, col, month
from pyspark.sql.functions import split, explode
from pyspark.sql.types import StringType, IntegerType, StructType, StructField, ArrayType
from pyspark.sql import DataFrame


def read_file(path, sc):
    '''
    path: file directory path
    '''
    print("Calling inside read file ")
    #file must only contain one json object per row to work with spark
    sqlContext = SQLContext(sc)
    df = sqlContext.read.load(path,format="csv", sep=",", inferSchema="true", header="true")
    return df


