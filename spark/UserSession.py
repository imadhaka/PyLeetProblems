'''
You are given a time series data, which is clickstream of user activity. Perform Sessionization on the data and generate the session ids.

Add an additional column with name session_id and generate the session ids based on --
1. Session expires after inactivity of 30 minutes, no clickstream will be recorded in this.
2. Session remain active for a max of 2 hours (i.e. after every 2 hours, a new session starts)

Timestamp, User_id
2021-05-01T11:00:00Z, u1
2021-05-01T13:13:00Z, u1
2021-05-01T15:00:00Z, u2
2021-05-01T11:25:00Z, u1
2021-05-01T15:15:00Z, u2
2021-05-01T02:13:00Z, u3
2021-05-03T02:15:00Z, u4
2021-05-02T11:45:00Z, u1
2021-05-02T11:00:00Z, u3
2021-05-03T12:15:00Z, u3
2021-05-03T11:00:00Z, u4
2021-05-03T21:00:00Z, u4
2021-05-04T19:00:00Z, u2
2021-05-04T09:00:00Z, u3
2021-05-01T08:15:00Z, u1

Sample Output
Timestamp, User_id, Session_id
2021-05-01T11:00:00Z, u1, u1_s1
2021-05-01T13:13:00Z, u1, u1_s2
2021-05-01T15:00:00Z, u2, u2_s1

Also, consider the following scenario:
1. Get number of sessions generated for each day
2. Total time spent by a user in a day
'''

import os, sys
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import *
from pyspark.sql.window import Window

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.master('local[*]').getOrCreate()

class UserSession:
    def createData(self):
        data = [(datetime(2021,5,1,11,00,00),'u1'),
                (datetime(2021,5,1,13,13,00),'u1'),
                (datetime(2021,5,1,15,00,00),'u2'),
                (datetime(2021,5,1,11,25,00),'u1'),
                (datetime(2021,5,1,15,15,00),'u2'),
                (datetime(2021,5,1,2,13,00),'u3'),
                (datetime(2021,5,3,2,15,00),'u4'),
                (datetime(2021,5,2,11,45,00),'u1'),
                (datetime(2021,5,2,11,00,00),'u3'),
                (datetime(2021,5,3,12,15,00),'u3'),
                (datetime(2021,5,3,11,00,00),'u4'),
                (datetime(2021,5,3,21,00,00),'u4'),
                (datetime(2021,5,4,19,00,00),'u2'),
                (datetime(2021,5,4,9,00,00),'u3'),
                (datetime(2021,5,1,8,15,00),'u1')
            ]
        schema = ['timestamp', 'user_id']
        return spark.createDataFrame(data, schema)

    def getSessionIdDf(self, df):
        df = df.withColumn("timestamp", col("timestamp").cast("timestamp"))

        # Define time thresholds in seconds
        inactivity_threshold = 30 * 60  # 30 minutes
        max_session_duration = 2 * 60 * 60  # 2 hours

        # Window specification to calculate time differences
        window_spec = Window.partitionBy("user_id").orderBy("timestamp")

        # find last session time
        df = df.withColumn('prev_click_time', lag('timestamp', 1 ).over(window_spec))
        # Calculate the time difference between current and previous activity
        df = df.withColumn("time_diff", unix_timestamp("timestamp") - unix_timestamp("prev_click_time"))
        # Determine when a new session should start
        df = df.withColumn("new_session", when(
            (col("time_diff") >= inactivity_threshold) | (col("time_diff").isNull()) | (col("time_diff") > max_session_duration),
            1).otherwise(0))

        # Generate session ID based on new sessions
        df = df.withColumn("session_id", sum("new_session").over(window_spec))
        # Generate the final session identifier
        df = df.withColumn("session_id", concat(col("user_id"), lit("_s"), col("session_id")))

        return df.select("timestamp", "user_id", "session_id")

    def count_sessions_per_day(self, df):
        df = df.withColumn("date", date_format(col("Timestamp"), "yyyy-MM-dd"))
        return df.groupBy("date").agg(countDistinct("session_id").alias("num_sessions"))

    def calculate_total_time_spent(self, df):
        df = df.withColumn("date", date_format(col("Timestamp"), "yyyy-MM-dd"))
        time_window_spec = Window.partitionBy("User_id", "date").orderBy("Timestamp")
        df = df.withColumn("next_timestamp", lead("Timestamp").over(time_window_spec))
        df = df.withColumn("session_duration", when(
            col("next_timestamp").isNotNull(),
            unix_timestamp("next_timestamp") - unix_timestamp("Timestamp")).otherwise(0))

        return df.groupBy("User_id", "date").agg(sum("session_duration").alias("total_time_spent"))


ob = UserSession()
inputDf = ob.createData()
resultDf = ob.getSessionIdDf(inputDf)
resultDf.show()

countSessionDf = ob.count_sessions_per_day(resultDf)
countSessionDf.show()

totalSpentTime = ob.calculate_total_time_spent(resultDf)
totalSpentTime.show()