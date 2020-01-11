from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min
import sys
from pyspark.sql import Row
from pyspark.sql.types import * 


if __name__ == '__main__':

    spark = (SparkSession
            .builder
            .appName(sys.argv[0])
            .getOrCreate())


    #create sample data as an array
    data = [
        ['Jack', 50, 'Uber'],
        ['Emily', 24, 'Amazon'],
        ['Krishan', 30, 'Microsoft']
    ]

    #define explicit schema
    schema = 'name STRING , age INT, `company name` STRING'
    df = spark.createDataFrame(data,schema)

    df.show()

    print('Age statistics : ')
    df.agg(avg('age'), max('age'), min('age')).show()

    print('The schema is :')
    df.printSchema()


# Rows
    data = [
        Row('Krishan', 30, 'Microsoft'),
        Row('Ram',30,'Amazon'),
        Row('Sweta',24,'Amazon')
    ]

    data = [
        Row('Krishan', 30, 'Microsoft'),
        Row('Ram',30,'Amazon'),
        Row('Sweta',24,'Amazon')
    ]
    SCHEMA = StructType(
        [
            StructField('Name', StringType(), False),
            StructField('Age',IntegerType(), True),
            StructField('Company', StringType(), True)
        ]
    )
    df = spark.createDataFrame(data, schema = SCHEMA)

    df.show()


