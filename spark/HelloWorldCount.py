import os, sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.master('local[*]').getOrCreate()

class HelloWorldCount:
    def createData(self):
        return spark.read.text('C:/Users/thean/PycharmProjects/PyLeetProblems/data/helloworld.txt')

    def countWords(self, inputDf):
        # Replace each occurrence of "Hello World" with an empty string and calculate the difference in length
        pattern = "Hello World"
        count_df = inputDf.withColumn("num_hello_world", (length(col("value")) - length(regexp_replace(col("value"), pattern, ""))) / length(lit(pattern)))
        #count_df.show()

        # Sum up all the counts to get the total occurrences of "Hello World"
        total_count = count_df.agg({"num_hello_world": "sum"}).collect()[0][0]
        print(f"The phrase 'Hello World' occurs {total_count} times.")

    def countWordSql(self, inputDf):
        inputDf.createOrReplaceTempView('table')
        pattern_len = len("Hello World")
        query = f'''
            with cte as 
            (select (length(value) - length(regexp_replace(value, 'Hello World', ''))) / {pattern_len} as num from table)
            select sum(num) as Hello_world_count from cte
        '''
        return spark.sql(query)

ob = HelloWorldCount()
df = ob.createData()
#df.show(truncate=False)

ob.countWords(df)
result = ob.countWordSql(df)
result.show(truncate=False)