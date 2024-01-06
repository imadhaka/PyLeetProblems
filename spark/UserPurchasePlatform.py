'''
Table: Spending
+-------------+---------+
| Column Name | Type    |
+-------------+---------+
| user_id     | int     |
| spend_date  | date    |
| platform    | enum    |
| amount      | int     |
+-------------+---------+
The table logs the spendings history of users that make purchases from an online shopping website which has a desktop and a mobile application.
(user_id, spend_date, platform) is the primary key of this table.
The platform column is an ENUM type of ('desktop', 'mobile').


Write an SQL query to find the total number of users and the total amount spent using mobile only, desktop only and both mobile and desktop together for each date.

The query result format is in the following example:


Spending table:
+---------+------------+----------+--------+
| user_id | spend_date | platform | amount |
+---------+------------+----------+--------+
| 1       | 2019-07-01 | mobile   | 100    |
| 1       | 2019-07-01 | desktop  | 100    |
| 2       | 2019-07-01 | mobile   | 100    |
| 2       | 2019-07-02 | mobile   | 100    |
| 3       | 2019-07-01 | desktop  | 100    |
| 3       | 2019-07-02 | desktop  | 100    |
+---------+------------+----------+--------+

Result table:
+------------+----------+--------------+-------------+
| spend_date | platform | total_amount | total_users |
+------------+----------+--------------+-------------+
| 2019-07-01 | desktop  | 100          | 1           |
| 2019-07-01 | mobile   | 100          | 1           |
| 2019-07-01 | both     | 200          | 1           |
| 2019-07-02 | desktop  | 100          | 1           |
| 2019-07-02 | mobile   | 100          | 1           |
| 2019-07-02 | both     | 0            | 0           |
+------------+----------+--------------+-------------+
On 2019-07-01, user 1 purchased using both desktop and mobile, user 2 purchased using mobile only and user 3 purchased using desktop only.
On 2019-07-02, user 2 purchased using mobile only, user 3 purchased using desktop only and no one purchased using both platforms.
'''

import os, sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import date

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.master('local[*]').getOrCreate()

class UserPurchase:
    def createData(self):
        data = [
            (1, date(2019, 7, 1), 'mobile', 100),
            (1, date(2019, 7, 1), 'desktop', 100),
            (2, date(2019, 7, 1), 'mobile', 100),
            (2, date(2019, 7, 2), 'mobile', 100),
            (3, date(2019, 7, 1), 'desktop', 100),
            (3, date(2019, 7, 2), 'desktop', 100)
        ]
        schema = ['user_id', 'spend_date', 'platform', 'amount']
        return spark.createDataFrame(data, schema)

    def totalUsers(self, inputDf):
        inputDf.createOrReplaceTempView('table')

        query = """with cte as (
                            select user_id, spend_date,
                            count(distinct platform) as platformCount
                            from table group by user_id, spend_date),

                            cte2 as (
                            select M1.spend_date,
                            if(M2.platformCount > 1, 'both', M1.platform) as platform,
                            M1.amount, M1.user_id 
                            from table M1 join cte M2
                            on M1.user_id = M2.user_id 
                            and M1.spend_date = M2.spend_date), 
                            
                            cte3 as (
                            select distinct spend_date, X.platform 
                            from table join 
                            (SELECT 'desktop' AS platform UNION
                            SELECT 'mobile' AS platform UNION
                            SELECT 'both' AS platform) X )

                            select A.spend_date, A.platform,
                            coalesce(sum(amount), 0) as total_amount,
                            coalesce(count(distinct user_id), 0) as total_users
                            from cte3 A left join cte2 B
                            on A.platform = B.platform 
                            and A.spend_date = B.spend_date
                            group by A.spend_date, A.platform
                            order by A.spend_date, A.platform desc"""

        return spark.sql(query)

    def totalUserAmount(self, inputDf):
        platformData = [('desktop',), ('mobile',), ('both',)]
        schema = ['platform']
        platformCntdf = inputDf.groupBy('user_id', 'spend_date').agg(count_distinct('platform').alias('platformCount'))

        bothDf = spark.createDataFrame(platformData, schema)
        bothDf = bothDf.alias('M1').crossJoin(inputDf).select('spend_date', 'M1.platform').distinct()

        joinDf = platformCntdf.alias('M2').join(inputDf.alias('M1'), (platformCntdf['user_id']==inputDf['user_id']) & (platformCntdf['spend_date']==inputDf['spend_date']), 'inner')\
            .withColumn('platform', when(col('M2.platformCount').__gt__(1), lit('both')).otherwise(col('M1.platform')))\
            .select(col('M1.spend_date').alias('spend_date'), 'platform', 'amount', col('M1.user_id').alias('user_id'))

        joinDf = bothDf.alias('A').join(joinDf.alias('B'), (bothDf['platform']==joinDf['platform']) & (bothDf['spend_date']==joinDf['spend_date']), 'left')\
                .groupBy('A.spend_date', 'A.platform').agg(coalesce(sum('B.amount'), lit(0)).alias('total_amount'), count_distinct('B.user_id').alias('total_users'))\
                        .orderBy('A.spend_date', desc('A.platform'))

        return joinDf

ob = UserPurchase()
inputDf = ob.createData()

resultDf = ob.totalUsers(inputDf)
resultDf.show()

resultDf = ob.totalUserAmount(inputDf)
resultDf.show()