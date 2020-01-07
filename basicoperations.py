from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys

INCOME_FILE = sys.argv[1]
SCHEMA = "FIPS INT,State STRING,County STRING,MedHHInc BIGINT,PerCapitaInc LONG,PovertyUnder18Pct DOUBLE, PovertyAllAgesPct DOUBLE,Deep_Pov_All DOUBLE,Deep_Pov_Children DOUBLE,PovertyUnder18Num LONG,PovertyAllAgesNum LONG"

#python basicoperations.py ..\Rural_Atlas_Update20\Income.csv    

if __name__ == '__main__':
    print ('running basic operations in spark')

    spark = (SparkSession
            .builder
            .appName('basicOperations')
            .getOrCreate())
    #Read data
    df = spark.read.csv(INCOME_FILE, header=True, schema=SCHEMA)
    # df.show(1)
    print(df.columns)

    #select coumns
    poverty_df = df.select('state', 'county', 'PovertyAllAgesPct', 'Deep_Pov_All','PovertyAllAgesNum')

    poverty_df.show(10)

    print('Find total population and add that column, drop all ages num column')
    poverty_df = poverty_df.withColumn(
        'Population', col('PovertyAllAgesNum') / col('PovertyAllAgesPct')
        ).drop('PovertyAllAgesNum')
    poverty_df.show(5)

    print('Rename columns')
    poverty_df = poverty_df.withColumnRenamed('PovertyAllAgesPct', 'PovertyPct').withColumnRenamed('Deep_Pov_All', 'DeepPovertyPct')
    poverty_df.show(5)
    
    #aggregate
    print('total population and average poverty percentage')
    state_df = poverty_df.groupBy('state')\
                .agg(
                    sum('Population').alias('population'),
                    avg('PovertyPct').alias('avgPovertyPct'),
                    avg('DeepPovertyPct')
                    )\
                .sort(col('state'),ascending = True)
    state_df.show(5)

    #filter using row value
    #collect() creates an array of row from dataframe
    print('Find state with maximum poverty')

    #not very elegant solution. How to print other columns?
    max_row = state_df.groupBy().agg(max('avgPovertyPct')).collect()[0]
    max_poverty_val = max_row[0]
    state_df.filter(col('avgPovertyPct') == max_poverty_val).show()

    #easier way
    rows = state_df.sort(col('avgPovertyPct'), ascending=False).head(1)#head does not return dataframe but list of rows. 
    spark.createDataFrame(rows, schema = state_df.schema).show()

