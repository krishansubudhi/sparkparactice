import basicoperations
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    #python write.py ..\Rural_Atlas_Update20\Income.csv
    spark = (SparkSession
        .builder
        .appName('write')
        .getOrCreate())

    income_df = spark.read.csv(basicoperations.INCOME_FILE, 
        schema = basicoperations.SCHEMA,
        header = True)

    richest_states = (income_df
            .select('State','County','PerCapitaInc')
            .sort(col('PerCapitaInc'), Ascending = False)
            )

    #write to parquet file (schema stored with data)
    richest_states.show(5)

    op_path = './out/richest_states.parquet'
    
    print(f'Writing richest state data to {op_path}')

    richest_states.write \
        .format('parquet') \
        .save(op_path)

    #read from written file again
    print(f'Reading richest state data from  {op_path}')
    richest_parquet_df = spark.read.parquet(op_path)
    print(richest_parquet_df.schema)
    richest_parquet_df.show(20)