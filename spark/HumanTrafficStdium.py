"""
Table: Stadium

+---------------+---------+
| Column Name   | Type    |
+---------------+---------+
| id            | int     |
| visit_date    | date    |
| people        | int     |
+---------------+---------+
visit_date is the column with unique values for this table.
Each row of this table contains the visit date and visit id to the stadium with the number of people during the visit.
As the id increases, the date increases as well.


Write a solution to display the records with three or more rows with consecutive id's, and the number of people is greater than or equal to 100 for each.

Return the result table ordered by visit_date in ascending order.

======================================================================================

Input:
Stadium table:
+------+------------+-----------+
| id   | visit_date | people    |
+------+------------+-----------+
| 1    | 2017-01-01 | 10        |
| 2    | 2017-01-02 | 109       |
| 3    | 2017-01-03 | 150       |
| 4    | 2017-01-04 | 99        |
| 5    | 2017-01-05 | 145       |
| 6    | 2017-01-06 | 1455      |
| 7    | 2017-01-07 | 199       |
| 8    | 2017-01-09 | 188       |
+------+------------+-----------+
Output:
+------+------------+-----------+
| id   | visit_date | people    |
+------+------------+-----------+
| 5    | 2017-01-05 | 145       |
| 6    | 2017-01-06 | 1455      |
| 7    | 2017-01-07 | 199       |
| 8    | 2017-01-09 | 188       |
+------+------------+-----------+
Explanation:
The four rows with ids 5, 6, 7, and 8 have consecutive ids and each of them has >= 100 people attended. Note that row 8 was included even though the visit_date was not the next day after row 7.
The rows with ids 2 and 3 are not included because we need at least three consecutive ids.
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, lead, when
from pyspark.sql.window import Window
from datetime import date

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.master("local[*]").appName("Human Traffic of Stadium").getOrCreate()

class HumanTraffic:
    def getData(self):
        data = [
            (1, date(2017, 1, 1), 10),
            (2, date(2017, 1, 2), 109),
            (3, date(2017, 1, 3), 150),
            (4, date(2017, 1, 4), 99),
            (5, date(2017, 1, 5), 145),
            (6, date(2017, 1, 6), 1455),
            (7, date(2017, 1, 7), 199),
            (8, date(2017, 1, 8), 188),
            (9, date(2017, 1, 9), 90)
        ]
        columns = ["id", "visit_date", "people"]
        return spark.createDataFrame(data, columns)

    def getConsecutiveRows(self, inputDf):
        # Define a window specification ordered by timestamp
        window_spec = Window.orderBy("id")

        inputDf = inputDf.withColumn('prev1', lag('id').over(window_spec))\
                        .withColumn('prev2', lag('id',2).over(window_spec))\
                        .withColumn('next1', lead('id').over(window_spec)) \
                        .withColumn('next2', lead('id',2).over(window_spec))

        # Define a condition for consecutive rows
        condition = (((col('id') - col('prev1') == 1) & (col('next1') - col('id') == 1))
                    | ((col('id') - col('prev2') == 2) & (col('id') - col('prev1') == 1))
                    | ((col('next2') - col('id') == 2) & (col('next1') - col('id') == 1)))

        inputDf = inputDf.select('id', 'visit_date', 'people')\
                        .filter(col('people') >= 100)\
                        .filter(condition)

        return inputDf


obj = HumanTraffic()
dataDf = obj.getData()
consRowsDf = obj.getConsecutiveRows(dataDf)
consRowsDf.show()