'''
You are given a table named "Attendance" with columns:
StudentID, ClassDate, IsPresent (a boolean where 1 indicates presence and 0 indicates absence).

Write a SQL query to identify students who have missed at least 3 consecutive classes.

INPUT-
StudentID, ClassDate, IsPresent
(1, date(2023, 10, 1), 1),
            (2, date(2023, 10, 1), 1),
            (3, date(2023, 10, 1), 0),
            (1, date(2023, 10, 2), 1),
            (2, date(2023, 10, 2), 0),
            (3, date(2023, 10, 2), 1),
            (1, date(2023, 10, 3), 1),
            (2, date(2023, 10, 3), 0),
            (3, date(2023, 10, 3), 1),
            (1, date(2023, 10, 4), 1),
            (2, date(2023, 10, 4), 0),
            (3, date(2023, 10, 4), 1),
            (1, date(2023, 10, 5), 1),
            (2, date(2023, 10, 5), 1),
            (3, date(2023, 10, 5), 1),
            (1, date(2023, 10, 6), 1),
            (2, date(2023, 10, 6), 0),
            (3, date(2023, 10, 6), 1),
            (1, date(2023, 10, 7), 1),
            (2, date(2023, 10, 7), 0),
            (3, date(2023, 10, 7), 1),
            (1, date(2023, 10, 8), 1),
            (2, date(2023, 10, 8), 0),
            (3, date(2023, 10, 8), 1),
            (1, date(2023, 10, 9), 1),
            (2, date(2023, 10, 9), 0),
            (3, date(2023, 10, 9), 1)

OUTPUT-
StudentID,  MissingFrom,  NumberOfMissedDays
2,  2023-10-02,   3
2,  2023-10-06,   4
3,  2023-10-01,   3
'''

import os, sys
from pyspark.sql import SparkSession
from datetime import date

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.master('local[*]').getOrCreate()

class Attendance:
    def createAttendanceData(self):
        data = [
            (1, date(2023, 10, 1), 1),
            (2, date(2023, 10, 1), 1),
            (3, date(2023, 10, 1), 0),
            (1, date(2023, 10, 2), 1),
            (2, date(2023, 10, 2), 0),
            (3, date(2023, 10, 2), 0),
            (1, date(2023, 10, 3), 1),
            (2, date(2023, 10, 3), 0),
            (3, date(2023, 10, 3), 0),
            (1, date(2023, 10, 4), 1),
            (2, date(2023, 10, 4), 0),
            (3, date(2023, 10, 4), 1),
            (1, date(2023, 10, 5), 1),
            (2, date(2023, 10, 5), 1),
            (3, date(2023, 10, 5), 1),
            (1, date(2023, 10, 6), 1),
            (2, date(2023, 10, 6), 0),
            (3, date(2023, 10, 6), 1),
            (1, date(2023, 10, 7), 1),
            (2, date(2023, 10, 7), 0),
            (3, date(2023, 10, 7), 1),
            (1, date(2023, 10, 8), 1),
            (2, date(2023, 10, 8), 0),
            (3, date(2023, 10, 8), 1),
            (1, date(2023, 10, 9), 1),
            (2, date(2023, 10, 9), 0),
            (3, date(2023, 10, 9), 1)
        ]
        columns = ['StudentID', 'ClassDate', 'IsPresent']
        return spark.createDataFrame(data, columns)


    def getAttendance(self, studentDf):
        studentDf.createOrReplaceTempView('Attendance')

        query = "with cte as ( " \
                "SELECT " \
                "A.StudentID, " \
                "A.ClassDate, " \
                "A.IsPresent, " \
                "(row_number() over(PARTITION BY A.StudentID ORDER BY A.ClassDate) - row_number() over(PARTITION BY A.StudentID, A.IsPresent ORDER BY A.ClassDate)) as numDays " \
                "FROM Attendance A " \
                ") " \
                "select " \
                "M1.StudentID, " \
                "min(M1.ClassDate) as MissingFrom, " \
                "count(M1.numDays) as NumberOfMissedDays " \
                "from cte M1 " \
                "where M1.IsPresent = 0 " \
                "group by M1.StudentID, numDays"
        return spark.sql(query)

ob = Attendance()
studentDf = ob.createAttendanceData()

resultDf = ob.getAttendance(studentDf)
resultDf.show(50, False)