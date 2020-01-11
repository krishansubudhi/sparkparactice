# 1. Timestamp data type
# 2. Conditional aggregation

#download data
import urllib
from urllib import request

out = '../out/Accidental_Drug_Related_Deaths_2012-2018.csv'
#downlaod file
#needs manual correction replace \n( with (

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == '__main__':

    spark = (SparkSession
    .builder
    .appName('dateoperations')
    .getOrCreate())
    
    death_df = spark.read.csv(
        path = out,
        header = True,
        inferSchema = True
    )

    print(death_df.schema)

    death_df.select('Date', 'Age', 'Sex', 'DescriptionofInjury').show(5)

    #change date from string to datetime
    modified_df = death_df.select(
        to_timestamp(col('date'), 'MM/dd/yyyy').alias('date'), 
        'age', 'sex', 
        col('descriptionofinjury').alias('desc')
        ) #alias is same as withcolumnrenmaed

    modified_df.show(5)

    #analysis. Find deaths by year
    print('deaths by year')
    modified_df.groupBy(year('date').alias('year')).count().orderBy('year', ascending = False).show()

    print('avg death age by year')
    modified_df.groupBy(year('date').alias('year')).agg(
         {"sex": "count", "age":"avg"}).orderBy('year', ascending = False
        ).show()

    #conditional aggregation : sumif
    #Find total male deaths and total female deaths based on year 
    modified_df.groupBy(year('date').alias('year')).agg(
        sum(
            when(col('sex') == 'Male', 1).otherwise(0)
            ).alias('MaleDeaths'),
        sum(
            when(col('sex') == 'Female', 1).otherwise(0)
            ).alias('FemaleDeaths')
        ).orderBy('year', ascending = False
    ). show()
