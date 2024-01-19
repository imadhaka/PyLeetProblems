'''
Table: Failed
+--------------+---------+
| Column Name  | Type    |
+--------------+---------+
| fail_date    | date    |
+--------------+---------+
Primary key for this table is fail_date.
Failed table contains the days of failed tasks.

Table: Succeeded
+--------------+---------+
| Column Name  | Type    |
+--------------+---------+
| success_date | date    |
+--------------+---------+
Primary key for this table is success_date.
Succeeded table contains the days of succeeded tasks.


A system is running one task every day. Every task is independent of the previous tasks. The tasks can fail or succeed.

Write an SQL query to generate a report of period_state for each continuous interval of days in the period from 2019-01-01 to 2019-12-31.

period_state is 'failed' if tasks in this interval failed or 'succeeded' if tasks in this interval succeeded. Interval of days are retrieved as start_date and end_date.

Order result by start_date.

The query result format is in the following example:
Failed table:
+-------------------+
| fail_date         |
+-------------------+
| 2018-12-28        |
| 2018-12-29        |
| 2019-01-04        |
| 2019-01-05        |
+-------------------+

Succeeded table:
+-------------------+
| success_date      |
+-------------------+
| 2018-12-30        |
| 2018-12-31        |
| 2019-01-01        |
| 2019-01-02        |
| 2019-01-03        |
| 2019-01-06        |
+-------------------+


Result table:
+--------------+--------------+--------------+
| period_state | start_date   | end_date     |
+--------------+--------------+--------------+
| succeeded    | 2019-01-01   | 2019-01-03   |
| failed       | 2019-01-04   | 2019-01-05   |
| succeeded    | 2019-01-06   | 2019-01-06   |
+--------------+--------------+--------------+

The report ignored the system state in 2018 as we care about the system in the period 2019-01-01 to 2019-12-31.
From 2019-01-01 to 2019-01-03 all tasks succeeded and the system state was "succeeded".
From 2019-01-04 to 2019-01-05 all tasks failed and system state was "failed".
From 2019-01-06 to 2019-01-06 all tasks succeeded and system state was "succeeded".
'''

import os, sys
from pyspark.sql import SparkSession
from datetime import date
from pyspark.sql.functions import row_number, lit, col, min, max, aggregate
from pyspark.sql.window import Window

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.master('local[*]').getOrCreate()

class Report:
    def createFailedData(self):
        data = [
            (date(2018, 12, 28),),
            (date(2018, 12, 29),),
            (date(2019, 1, 4),),
            (date(2019, 1, 5),)
        ]
        schema = ['fail_date']
        return spark.createDataFrame(data, schema)

    def createSucceededData(self):
        data = [
            (date(2018, 12, 30),),
            (date(2018, 12, 31),),
            (date(2019, 1, 1),),
            (date(2019, 1, 2),),
            (date(2019, 1, 3),),
            (date(2019, 1, 6),)
        ]
        schema = ['success_date']
        return spark.createDataFrame(data, schema)

    def periodState(self, succeedDf, failedDf):
        succeedDf.createOrReplaceTempView('success')
        failedDf.createOrReplaceTempView('fail')
        query = """
                with M1 as (
                select success_date as date, 'succeeded' as period_state from success
                union
                select fail_date as date, 'failed' as period_state from fail
                ), 
                M2 as (select 
                date, period_state,
                (date - ROW_NUMBER() OVER (PARTITION BY period_state ORDER BY date)) as rnum
                from M1) 
                SELECT period_state, 
                MIN(date) AS start_date, 
                MAX(date) AS end_date
                FROM M2
                WHERE period_state IS NOT NULL
                GROUP BY period_state, rnum
                order by start_date
                """
        return spark.sql(query)

    def periodStatePy(self, succeedDf, failedDf):
        combinedDf = succeedDf.withColumnRenamed('success_date','date').withColumn('period_state', lit('succeeded'))\
            .union(failedDf.withColumnRenamed('fail_date','date').withColumn('period_state', lit('failed')))
        window_spec = Window.partitionBy('period_state').orderBy('date')
        combinedDf = combinedDf.withColumn('rnum',col('date')-row_number().over(window_spec))
        resultDf = combinedDf.groupBy('period_state', 'rnum')\
                    .agg(min('date').alias('start_date'),max('date').alias('end_date'))\
                    .orderBy('start_date')\
                    .drop('rnum')
        return resultDf

ob = Report()

failedDf = ob.createFailedData()
succeedDf = ob.createSucceededData()

resultDf = ob.periodState(succeedDf,failedDf)
resultDf.show()

resultDf = ob.periodStatePy(succeedDf,failedDf)
resultDf.show()