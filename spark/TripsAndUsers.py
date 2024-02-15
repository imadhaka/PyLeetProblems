'''
+-------------+----------+
| Column Name | Type     |
+-------------+----------+
| Id          | int      |
| Client_Id   | int      |
| Driver_Id   | int      |
| City_Id     | int      |
| Status      | enum     |
| Request_at  | date     |
+-------------+----------+
Id is the primary key for this table.
The table holds all taxi trips. Each trip has a unique Id, while Client_Id and Driver_Id are foreign keys to the Users_Id at the Users table.
Status is an ENUM type of (‘completed’, ‘cancelled_by_driver’, ‘cancelled_by_client’).

+-------------+----------+
| Column Name | Type     |
+-------------+----------+
| Users_Id    | int      |
| Banned      | enum     |
| Role        | enum     |
+-------------+----------+
Users_Id is the primary key for this table.
The table holds all users. Each user has a unique Users_Id, and Role is an ENUM type of (‘client’, ‘driver’, ‘partner’).
Status is an ENUM type of (‘Yes’, ‘No’).

Write a SQL query to find the cancellation rate of requests with unbanned users
(both client and driver must not be banned) each day between "2013-10-01" and "2013-10-03".

The cancellation rate is computed by dividing the number of canceled (by client or driver) requests with unbanned users
by the total number of requests with unbanned users on that day.

Return the result table in any order. Round Cancellation Rate to two decimal points.

The query result format is in the following example:


Trips table:
+----+-----------+-----------+---------+---------------------+------------+
| Id | Client_Id | Driver_Id | City_Id | Status              | Request_at |
+----+-----------+-----------+---------+---------------------+------------+
| 1  | 1         | 10        | 1       | completed           | 2013-10-01 |
| 2  | 2         | 11        | 1       | cancelled_by_driver | 2013-10-01 |
| 3  | 3         | 12        | 6       | completed           | 2013-10-01 |
| 4  | 4         | 13        | 6       | cancelled_by_client | 2013-10-01 |
| 5  | 1         | 10        | 1       | completed           | 2013-10-02 |
| 6  | 2         | 11        | 6       | completed           | 2013-10-02 |
| 7  | 3         | 12        | 6       | completed           | 2013-10-02 |
| 8  | 2         | 12        | 12      | completed           | 2013-10-03 |
| 9  | 3         | 10        | 12      | completed           | 2013-10-03 |
| 10 | 4         | 13        | 12      | cancelled_by_driver | 2013-10-03 |
+----+-----------+-----------+---------+---------------------+------------+

Users table:
+----------+--------+--------+
| Users_Id | Banned | Role   |
+----------+--------+--------+
| 1        | No     | client |
| 2        | Yes    | client |
| 3        | No     | client |
| 4        | No     | client |
| 10       | No     | driver |
| 11       | No     | driver |
| 12       | No     | driver |
| 13       | No     | driver |
+----------+--------+--------+

Result table:
+------------+-------------------+
| Day        | Cancellation Rate |
+------------+-------------------+
| 2013-10-01 | 0.33              |
| 2013-10-02 | 0.00              |
| 2013-10-03 | 0.50              |
+------------+-------------------+

'''

import os, sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.master('local[*]').getOrCreate()

class TripsAndUsers:
    def createTripsData(self):
        data = [
            (1, 1, 10, 1, 'completed', '2013-10-01'),
            (2, 2, 11, 1, 'cancelled_by_driver', '2013-10-01'),
            (3, 3, 12, 6, 'completed', '2013-10-01'),
            (4, 4, 13, 6, 'cancelled_by_client', '2013-10-01'),
            (5, 1, 10, 1, 'completed', '2013-10-02'),
            (6, 2, 11, 6, 'completed', '2013-10-02'),
            (7, 3, 12, 6, 'completed', '2013-10-02'),
            (8, 2, 12, 12, 'completed', '2013-10-03'),
            (9, 3, 10, 12, 'completed', '2013-10-03'),
            (10, 4, 13, 12, 'cancelled_by_driver', '2013-10-03')
        ]
        columns = ['Id', 'Client_Id', 'Driver_Id', 'City_Id', 'Status', 'Request_at']
        return spark.createDataFrame(data, columns)

    def createUsersData(self):
        data = [
            (1, 'No', 'client'),
            (2, 'Yes', 'client'),
            (3, 'No', 'client'),
            (4, 'No', 'client'),
            (10, 'No', 'driver'),
            (11, 'No', 'driver'),
            (12, 'No', 'driver'),
            (13, 'No', 'driver')
        ]
        columns = ['Users_Id', 'Banned', 'Role']
        return spark.createDataFrame(data, columns)

    # Approach - SQL
    def calculateRate(self, tripsDf, usersDf):
        tripsDf.createOrReplaceTempView('trips')
        usersDf.createOrReplaceTempView('users')
        query = """with client as (
                select count(t.Client_Id) as total_client, t.status, t.Request_at 
                from trips t inner join users u 
                on t.Client_Id = u.Users_Id 
                where t.Request_at between '2013-10-01' and '2013-10-03'
                and u.Role = 'client' and u.Banned = 'No'
                group by t.status, t.Request_at
                )
                select Request_at as Day,
                round(SUM(CASE WHEN Status LIKE 'cancelled%' THEN total_client ELSE 0 END) / sum(total_client), 2) AS Cancellation_Rate
                from client
                group by Request_at
                order by Request_at
                """
        return spark.sql(query)

     # Approach - PySpark
    def calculateRate_pyspark(self, tripsDf, usersDf):
        clientdf = tripsDf.join(usersDf, tripsDf.Client_Id == usersDf.Users_Id, 'inner')\
                            .filter(col('Request_at').between('2013-10-01', '2013-10-03') & col('Role').__eq__('client') & col('Banned').__eq__('No'))\
                            .groupBy(col('Status'), col('Request_at')).agg(count('Client_Id').alias('total_client'))\
                            .select('total_client', 'Status', 'Request_at')

        cancellation_rate_df = clientdf.groupBy('Request_at')\
                                    .agg(round(sum(when(col('Status').startswith('cancelled'), col('total_client')).otherwise(0)) / sum(col('total_client')), 2).alias('Cancellation_Rate'))\
                                    .orderBy('Request_at')
        return cancellation_rate_df

ob = TripsAndUsers()
tripsDf = ob.createTripsData()
usersDf = ob.createUsersData()

resultDf = ob.calculateRate(tripsDf, usersDf)
resultDf.show()

resultDf = ob.calculateRate_pyspark(tripsDf, usersDf)
resultDf.show()