"""
Write a SQL Query to find Min and Max values of continuous sequence in a group of elements.

Input --
|element|sequence|
+-------+--------+
|      A|       1|
|      A|       2|
|      A|       3|
|      A|       5|
|      A|       6|
|      A|       8|
|      A|       9|
|      B|      11|
|      C|      13|
|      C|      14|
|      C|      15|
+-------+--------+

Output --
|element|min_seq|max_seq|
+-------+-------+-------+
|      A|      1|      3|
|      A|      5|      6|
|      A|      8|      9|
|      B|     11|     11|
|      C|     13|     15|
+-------+-------+-------+
"""

import os, sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.master('local[*]').getOrCreate()

class SeqMaxMinValues:
    def getInputData(self):
        data = [('A','1'),
                ('A','2'),
                ('A','3'),
                ('A','5'),
                ('A','6'),
                ('A','8'),
                ('A','9'),
                ('B','11'),
                ('C','13'),
                ('C','14'),
                ('C','15')]
        return spark.createDataFrame(data, ['element', 'sequence'])

    def findSequence(self, inputDf):
        inputDf.createOrReplaceTempView('table')
        query = """
            with seq as (
            select element, sequence,
            sequence - row_number() over(partition by element order by sequence) as seqDiff
            from table
            )
            select element, min(sequence) as min_seq,
            max(sequence) as max_seq from seq
            group by element, seqDiff
            """
        return spark.sql(query)

    def seqValues(self, inputDf):
        sequenceDf = inputDf.withColumn('seqDiff', col('sequence') - row_number().over(Window.partitionBy('element').orderBy('sequence')))
        return sequenceDf.groupBy('element', 'seqDiff')\
                        .agg(min('sequence').alias('min_seq'), max('sequence').alias('max_seq'))\
                        .drop('seqDiff')


ob = SeqMaxMinValues()
inputDf = ob.getInputData()
inputDf.show()

resultDf = ob.findSequence(inputDf)
resultDf.show()

resultDf = ob.seqValues(inputDf)
resultDf.show()