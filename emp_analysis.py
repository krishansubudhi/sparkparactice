import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import count,avg
import os
print(os.environ['SPARK_HOME'])

if __name__ == '__main__':
    print('inside python script for spark')

    if (len(sys.argv) <2):
        print('Provide data file path')
        sys.exit(-1)

    #build sparksession
    spark = (SparkSession
            .builder
            .appName('PythonEmpAnalysis')
            .getOrCreate())


    spark.sparkContext.setLogLevel('ERROR')
    #get Jobs.csv dataset filename from arguments
    #C:\Users\krkusuk.REDMOND\Study\spark\Rural_Atlas_Update20\Jobs.csv
    jobs_file = sys.argv[1]
    
    '''
    * Spark shell already executes everything 
    * till above. You can test the below code
    * using pyspark shell.
    '''
    
    #download data from 
    #https://www.ers.usda.gov/data-products/atlas-of-rural-and-small-town-america/download-the-data/

    #read csv
    job_df = (spark.read.format("csv")
        .option("header","true")
        .option("inferSchema","true")
        .load(jobs_file)
        )
        
    #count number of counties in states
    print('Counties per states')
    counties_count_df = (
        job_df.select('State','County')
        .groupBy('State')
        .agg(count("County").alias("Counties"))
        .orderBy("Counties", ascending = False)
        )
    
    counties_count_df.show(n = 10, truncate = False)
    
    #calculate average unemployment rate by state
    print('Unmeployement per states')
    state_unemploymentrate_df = (
        job_df.select('State', 'UnempRate2018')
        .groupBy('State')
        .agg(avg('UnempRate2018').alias('AvgUnempRate2018'))
        .orderBy('AvgUnempRate2018', ascending = False)
        )
    
    state_unemploymentrate_df.show(10,truncate = False)
    
    #calculate unemployment rate of washington state
    print('Unemployment in Washington state')
    state_unemploymentrate_df.filter(state_unemploymentrate_df.State=='WA').show()
    
    