import os, sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.master('local[*]').getOrCreate()

class HelloWorld:
    def createData(self):
        return spark.read.text('C:/Users/thean/PycharmProjects/PyLeetProblems/data/helloworld.txt')

    def countWords(self, inputDf):
        # Use regexp_extract to find occurrences of "Hello World"
        pattern = "Hello World"
        count_df = inputDf.withColumn("num_hello_world", (length(col("value")) - length(regexp_replace(col("value"), pattern, ""))) / length(lit(pattern)))
        count_df.show()

        # Sum up all the counts to get the total occurrences of "Hello World"
        total_count = count_df.agg({"num_hello_world": "sum"}).collect()[0][0]
        print(f"The phrase 'Hello World' occurs {total_count} times.")

ob = HelloWorld()
df = ob.createData()
df.show(truncate=False)

count = ob.countWords(df)