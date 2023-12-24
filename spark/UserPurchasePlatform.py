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
            (3, date(2019, 7, 2), 'desktop', 100),
        ]
        schema = ['user_id', 'spend_date', 'platform', 'amount']
        return spark.createDataFrame(data, schema)

    def totalUsers(self, inputDf):
        inputDf.createOrReplaceTempView('table')
        query = """with plat as (
                    select user_id, spend_date, 
                    concat_ws(' , ', collect_list(platform)) as platform
                    from table group by user_id, spend_date)  
                    select M1.spend_date,
                    case when M2.platform='mobile' 
                            then 'mobile' 
                        when M2.platform='desktop' 
                            then 'desktop' 
                        else 'both' 
                    end as platform,
                    sum(M1.amount) as total_amount,
                    count(M1.user_id) as total_users 
                    from table M1 join plat M2 
                    on M1.user_id = M2.user_id 
                    and M1.spend_date = M2.spend_date 
                    group by M1.spend_date, M2.platform"""

        return spark.sql(query)

    def totalUserAmount(self, inputDf):
        groupDf = inputDf.groupBy('user_id', 'spend_date').agg(concat_ws(',', collect_list('platform')).alias('platform'))
        groupDf = groupDf.alias('M2').join(inputDf.alias('M1'), (groupDf['user_id']==inputDf['user_id']) & (groupDf['spend_date']==inputDf['spend_date']), 'inner') \
                        .groupBy('M1.spend_date', 'M2.platform').agg(sum('M1.amount').alias('total_amount'), count('M1.user_id').alias('total_users'))
        return groupDf

ob = UserPurchase()
inputDf = ob.createData()

resultDf = ob.totalUsers(inputDf)
resultDf.show()

resultDf = ob.totalUserAmount(inputDf)
resultDf.show()