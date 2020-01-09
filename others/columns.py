from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys

if __name__ == '__main__':
    jobs_file = sys.argv[1]
    
    spark = (SparkSession
        .builder
        .appName(sys.argv[0])
        .getOrCreate())
        
    jobs_df = (spark.read
        .format('csv')
        .option("inferSchema","true")
        .option("header","true")
        .load(jobs_file))

    unemp_df = jobs_df.select(
        'state', 
        'county', 
        'unemprate2018', 
        'unemprate2017' 
    )

    #Find percentage change in unempolyment rate from 2017 to 2018
    unemp_df.withColumn(
        'percentage change',
        ((expr('unemprate2018')- col('unemprate2017') ) / column('unemprate2017') )
        ).show(5)