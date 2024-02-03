'''
The Employee table holds the salary information in a year -- Id, Month, Salary.

Write a SQL to get the cumulative sum of an employee’s salary over a period of 3 months but exclude the most recent month.

The result should be displayed by ‘Id’ ascending, and then by ‘Month’ descending.

Example Input
| Id | Month | Salary |
|----|-------|--------|
| 1  | 1     | 20     |
| 2  | 1     | 20     |
| 1  | 2     | 30     |
| 2  | 2     | 30     |
| 3  | 2     | 40     |
| 1  | 3     | 40     |
| 3  | 3     | 60     |
| 1  | 4     | 60     |
| 3  | 4     | 70     |


Output
| Id | Month | Salary |
|----|-------|--------|
| 1  | 3     | 90     |
| 1  | 2     | 50     |
| 1  | 1     | 20     |
| 2  | 1     | 20     |
| 3  | 3     | 100    |
| 3  | 2     | 40     |
'''

import os, sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, row_number, desc, col
from pyspark.sql.window import Window

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.master('local[*]').getOrCreate()

class CumulativeSalary:
    def createData(self):
        data = [
            (1, 1, 20),
            (2, 1, 20),
            (1, 2, 30),
            (2, 2, 30),
            (3, 2, 40),
            (1, 3, 40),
            (3, 3, 60),
            (1, 4, 60),
            (3, 4, 70)
        ]
        schema = ['Id', 'Month', 'Salary']
        return spark.createDataFrame(data, schema)

    def getSalaryMedian(self, inputDf):
        inputDf.createOrReplaceTempView('input')
        query = """with salary as (
                select M1.Id, M1.Month,
                sum(M1.Salary) over(partition by M1.Id order by M1.Id, M1.Month) as Salary,
                row_number() over(partition by Id order by Month desc) as rnk
                from input M1 
                order by Id, Month desc )
                select Id, Month, Salary from salary where rnk != 1"""
        return spark.sql(query)

    def getSalaryMedian2(self, inputDf):
        resultDf = inputDf.withColumn('Salary', sum('Salary').over(Window.partitionBy('Id').orderBy('Id','Month')))\
                            .withColumn('rnk', row_number().over(Window.partitionBy('Id').orderBy(desc('Month'))))
        return resultDf.filter(col('rnk') != '1')\
                        .select('Id', 'Month', 'Salary')


obj = CumulativeSalary()
inputDf = obj.createData()

resultDf = obj.getSalaryMedian(inputDf)
resultDf.show()

resultDf = obj.getSalaryMedian2(inputDf)
resultDf.show()