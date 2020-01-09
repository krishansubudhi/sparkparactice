from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min
import sys

#create sample data as an array
data = [
    ['Jack', 50, 'Uber'],
    ['Emily', 24, 'Amazon'],
    ['Krishan', 30, 'Microsoft']
]

#define explicit schema
schema = 'name STRING , age INT, `company name` STRING'

if __name__ == '__main__':

    spark = (SparkSession
            .builder
            .appName(sys.argv[0])
            .getOrCreate())

    df = spark.createDataFrame(data,schema)

    df.show()

    print('Age statistics : ')
    df.agg(avg('age'), max('age'), min('age')).show()

    print('The schema is :')
    df.printSchema()