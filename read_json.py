from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max, min
import sys, json

#create sample data as an array
data = [
    {'name':'Jack', 'age':50, 'company name':'Uber'},
    {'name':'Emily', 'age':24, 'company name':'Amazon'}
]

#define explicit schema
schema = 'name STRING , age INT, `company name` STRING'

if __name__ == '__main__':

    #write json data to file
    jsonstring = json.dumps(data)
    print(jsonstring)
    with open('data.json','w') as f:
        f.write(jsonstring)

    #read the json file
    with open('data.json','r') as f:
        print(json.load(f))
    spark = (SparkSession
            .builder
            .appName(sys.argv[0])
            .getOrCreate())

    df = spark.read.schema(schema).json('data.json')

    df.show()
