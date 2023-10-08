'''
Create a Comprehensive Fact Table

Given two input files:
- employee.csv with columns: employee_id, department, salary
- employee_personal.csv with columns: employee_id, first_name, last_name, DOB, state, country

Write transformations to create employee_fact with columns:
- employee_id
- employee_full_name
- department
- salary
- Salary_Diff_to_reach_highest_sal
- DOB
- state
- country
- age
'''

import os
import sys
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col, max, current_date, year

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.master("local[*]").appName("Human Traffic of Stadium").getOrCreate()

class ComprehensiveFact:
    def createEmployeeData(self):
        data = [
            (1, 'HR', 15000),
            (2, 'IT', 18000),
            (3, 'HR', 20000),
            (4, 'IT', 25000),
            (5, 'ADMIN', 12000)
        ]

        columns = ['employee_id', 'department', 'salary']
        return spark.createDataFrame(data, columns)

    def createPersonalData(self):
        data = [
            (1, 'Rohit', 'Khanna', date(1995, 12, 10), 'Delhi', 'IN'),
            (2, 'Arjun', 'Rao', date(1993, 10, 10), 'Chennai', 'IN'),
            (3, 'Kuldeep', 'Nair', date(1994, 2, 20), 'Delhi', 'IN'),
            (4, 'Viraj', 'Khaskar', date(1995, 3, 19), 'Bengalore', 'IN'),
            (5, 'Aditya', 'Paul', date(1996, 6, 12), 'Mumbai', 'IN'),
        ]

        columns = ['employee_id', 'first_name', 'last_name', 'DOB', 'state', 'country']
        return spark.createDataFrame(data, columns)

    # SQL Approach
    def getComprehensiveFactData(self, employeeDf, personalDf):
        employeeDf.createOrReplaceTempView('M1')
        personalDf.createOrReplaceTempView('M2')
        query = "select M1.employee_id, " \
                "concat(M2.first_name, ' ', M2.last_name) as employee_full_name, " \
                "M1.department, " \
                "M1.salary, " \
                "(select max(salary) from M1) - M1.salary as salary_diff, " \
                "M2.DOB, " \
                "M2.country, " \
                "DATEDIFF(YEAR, M2.DOB, NOW()) as age " \
                "from M1 join M2 " \
                "on M1.employee_id = M2.employee_id"
        return spark.sql(query)

    # PySpark Approach
    def getCompFactData(self, employeeDf, personalDf):
        # Find the highest salary in the entire company
        highest_salary = employeeDf.select(max("salary")).first()[0]

        resultDf = employeeDf.join(personalDf, on='employee_id', how='inner')\
                        .withColumn('employee_full_name', concat_ws(' ', col('first_name'), col('last_name')))\
                        .withColumn('salary_diff', highest_salary - col('salary'))\
                        .withColumn('age', year(current_date()) - year(col('DOB')))

        # Fetch required columns
        resultDf = resultDf.select(col('employee_id'),
                                   col('employee_full_name'),
                                   col('department'),
                                   col('salary'),
                                   col('salary_diff'),
                                   col('DOB'),
                                   col('country'),
                                   col('age')
                                   )
        return resultDf


ob = ComprehensiveFact()
df1 = ob.createEmployeeData()
df2 = ob.createPersonalData()

factDf = ob.getComprehensiveFactData(df1, df2)
factDf.show(10, False)

factDf = ob.getCompFactData(df1, df2)
factDf.show()