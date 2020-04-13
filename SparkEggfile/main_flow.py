# coding=utf-8
from __future__ import print_function
from pyspark.sql import SparkSession, SQLContext



from pyspark.sql.functions import udf, col, month
from pyspark.sql.types import StringType, IntegerType, StructType, StructField, ArrayType
from pyspark.sql import DataFrame


from SparkEggfile.write_movie_cast_crew import write_cast_crew
from SparkEggfile.output_data import output_df 
from SparkEggfile.read_file import read_file

spark = SparkSession.builder \
   .enableHiveSupport() \
   .getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)


print ("Init Started From main_flow")


def run():
    print('Calling inside run method')

    path="s3://bucket/credits.csv"

    #Read Input files and set dataframe
    cast_crew_df = read_file(path,sc)

    #Calling module to write dataframe into hive table
    write_cast_crew(cast_crew_df)

    #print data from hive tables
    output_df(spark)
