'''
Find all join result for below data-
null
1
1
null
2

null
1
2
2
3
'''
import os
import sys
from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.master("local[*]").getOrCreate()

class NullJoin:
    def createData(self):
        df_a = spark.read.option('header', True).csv("C:/Users/thean/PycharmProjects/PyLeetProblems/data/DataFile.csv")
        df_b = spark.read.option('header', True).csv('C:/Users/thean/PycharmProjects/PyLeetProblems/data/DataFile2.csv')

        return df_a, df_b

    # checking left join records
    def leftJoin(self, leftDf, rightDf):
        resultDf = leftDf.join(rightDf, on="C1", how="left")
        print('left join: {}', resultDf.count())
        resultDf.show()

    # checking right join records
    def rightJoin(self, leftDf, rightDf):
        resultDf = leftDf.join(rightDf, on="C1", how="right")
        print('right join: {}', resultDf.count())
        resultDf.show()

    # checking inner join records
    def innerJoin(self, leftDf, rightDf):
        resultDf = leftDf.join(rightDf, on="C1", how="inner")
        print('inner join: {}', resultDf.count())
        resultDf.show()

    # checking left full records
    def fullJoin(self, leftDf, rightDf):
        resultDf = leftDf.join(rightDf, on="C1", how="full")
        print('full join: {}', resultDf.count())
        resultDf.show()

# Main Process starts
obj = NullJoin()
leftDf, rightdf = obj.createData()

obj.leftJoin(leftDf, rightdf)
obj.rightJoin(leftDf, rightdf)
obj.innerJoin(leftDf, rightdf)
obj.fullJoin(leftDf, rightdf)