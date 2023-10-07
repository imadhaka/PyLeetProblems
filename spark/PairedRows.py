'''
Write a query for below:

Input -
ID  NAME
1	Person1
2	Person2
3	Person3
4	Person4
5	Person5
6	Person6
7	Person7
8	Person8
9	Person9
10	Person10

Output-
RESULT
1	Person1, 2	Person2
3	Person3, 4	Person4
5	Person5, 6	Person6
7	Person7, 8	Person8
9	Person9, 10	Person10

APPROACH:
    -- Use ntile() to divide the input rows into buckets
    -- string_agg to append the data in single row

'''
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, ntile, collect_list
from pyspark.sql.window import Window

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.master("local[*]").getOrCreate()

class MaxMarks:
    def createData(self):
        data = [
            (1, 'Person1'),
            (2, 'Person2'),
            (3, 'Person3'),
            (4, 'Person4'),
            (5, 'Person5'),
            (6, 'Person6'),
            (7, 'Person7'),
            (8, 'Person8'),
            (9, 'Person9'),
            (10, 'Person10')
        ]

        column = ['ID', 'Name']
        return spark.createDataFrame(data, column)

    #----- Approach 1 -----#
    # SQL
    def getMaxMarks(self, inputDf):
        inputDf.createOrReplaceTempView('table')
        query = "with PairedRows as (" \
                "select M1.ID AS ID1, " \
                "M2.ID AS ID2, " \
                "M1.Name as Name1, " \
                "M2.Name as Name2 " \
                "from table M1 join table M2 " \
                "on (M1.ID + 1) = M2.ID)" \
                "select concat(ID1, ' ', Name1, ', ', ID2, ' ', Name2) AS RESULT " \
                "from PairedRows where ID1 % 2 == 1"
        return spark.sql(query)

    # PySpark
    def maxMarks(self, inputDf):
        df1 = inputDf.alias('df1')
        df2 = inputDf.alias('df2')
        resultDf = df1.join(df2, col('df1.ID') + 1 == col('df2.ID'), 'INNER') \
                        .filter(col('df1.ID') % 2 == 1)\
                        .select(concat_ws(' ', col('df1.ID'), col('df1.Name')).alias('r1'),
                                concat_ws(' ', col('df2.ID'), col('df2.Name')).alias('r2'))

        resultDf = resultDf.select(concat_ws(' , ', col('r1'), col('r2')).alias('RESULT'))

        return resultDf

    # ----- Approach 2 -----# RECOMMENDED
    # SQL
    def getMaxMarks_window(self, inputDf):
        inputDf.createOrReplaceTempView('table')
        query = "with cte as ( " \
                "select ID, Name, " \
                "ntile(5) over(order by ID) as rnk " \
                "from table) " \
                "select concat_ws(' , ', collect_list(concat(ID, ' ', Name))) as RESULT " \
                "from cte M1 " \
                "group by rnk order by rnk"
        return spark.sql(query)

    # PySpark
    def maxMarks_window(self, inputDf):
        resultDf = inputDf.withColumn('rnk', ntile(5).over(Window.orderBy(col('ID'))))\
                        .withColumn('res', concat_ws(' ', col('ID'), col('Name')))
        resultDf = resultDf.groupBy(col('rnk')).agg(collect_list(concat_ws(' , ', col('res'))).alias('RESULT')).drop(col('rnk'))
        return resultDf


ob = MaxMarks()
inputDf = ob.createData()
inputDf.show()

resultDf = ob.getMaxMarks(inputDf)
resultDf.show(10, False)

resultDf = ob.maxMarks(inputDf)
resultDf.show(10, False)

resultDf = ob.getMaxMarks_window(inputDf)
resultDf.show(10, False)

resultDf = ob.maxMarks_window(inputDf)
resultDf.show(10, False)