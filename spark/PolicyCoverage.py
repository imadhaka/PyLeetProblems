"""
Input Table
Policy Number	Coverage No.	Premium
P1				C1				100
P1				C2				100
P2				C1				100

Output Table
Policy Number	No. of Coverages	C1	C2	Total Premium
P1				2					1	1	200
P2				1					1	0	100

Write a query to for the above output.
"""

import os, sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, col, when, lit, sum, count
from pyspark.sql.window import Window

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.master('local[*]').getOrCreate()

class PolicyCoverage:
    def getData(self):
        data = [
            ('P1', 'C1', 100),
            ('P1', 'C2', 200),
            ('P2', 'C1', 100),
            ('P3', 'C1', 100),
            ('P1', 'C3', 150)
        ]
        schema = ['Policy Number', 'Coverage No', 'Premium']
        return spark.createDataFrame(data, schema)

    def report(self, inputDf):
        pivotDf = inputDf.groupBy('Policy Number').pivot('Coverage No').count().fillna(0)
        premiumDf = inputDf.groupBy('Policy Number').agg(count('Coverage No').alias('No. of Coverages'),sum('Premium').alias('Total Premium'))
        return pivotDf.join(premiumDf, on = 'Policy Number', how = 'inner').orderBy('Policy Number')

ob = PolicyCoverage()
inputDf = ob.getData()
inputDf.show()

resultDf = ob.report(inputDf)
resultDf.show()
