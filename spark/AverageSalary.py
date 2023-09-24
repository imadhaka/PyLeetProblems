'''
We need to obtain a list of departments with an average salary lower than the overall average salary of the company.

However, when calculating the company's average salary, you must exclude the salaries of the department you are comparing it with.

For instance, when comparing the average salary of the HR department with the company's average,
the HR department's salaries shouldn't be taken into consideration for the calculation of the company's average salary.

here is the script :

create table emp(
emp_id int,
emp_name varchar(20),
department_id int,
salary int,
manager_id int,
emp_age int);

insert into emp
values (1, 'Ankit', 100,10000, 4, 39);
insert into emp
values (2, 'Mohit', 100, 15000, 5, 48);
insert into emp
values (3, 'Vikas', 100, 10000,4,37);
insert into emp
values (4, 'Rohit', 100, 5000, 2, 16);
insert into emp
values (5, 'Mudit', 200, 12000, 6,55);
insert into emp
values (6, 'Agam', 200, 12000,2, 14);
insert into emp
values (7, 'Sanjay', 200, 9000, 2,13);
insert into emp
values (8, 'Ashish', 200,5000,2,12);
insert into emp
values (9, 'Mukesh',300,6000,6,51);
insert into emp
values (10, 'Rakesh',300,7000,6,50);
'''

import os, sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.master('local[*]').getOrCreate()

class AverageSalary:
    def createData(self):
        data = [
            (1, 'Ankit', 100, 10000, 4, 39),
            (2, 'Mohit', 100, 15000, 5, 48),
            (3, 'Vikas', 100, 10000, 4, 37),
            (4, 'Rohit', 100, 5000, 2, 16),
            (5, 'Mudit', 200, 12000, 6, 55),
            (6, 'Agam', 200, 12000, 2, 14),
            (7, 'Sanjay', 200, 9000, 2, 13),
            (8, 'Ashish', 200, 5000, 2, 12),
            (9, 'Mukesh', 300, 6000, 6, 51),
            (10, 'Rakesh', 300, 7000, 6, 50)
        ]

        column = ['emp_id', 'emp_name', 'department_id', 'salary', 'manager_id', 'emp_age']

        return spark.createDataFrame(data, column)

    # Spark SQL approach
    def getAvgSalary_SQL(self, inputDf):
        inputDf.createOrReplaceTempView('T1')
        deptAvgSalDf = spark.sql('WITH DepartmentAverages AS ('
                                 'SELECT department_id, '
                                 'AVG(salary) AS department_avg '
                                 'FROM T1 GROUP BY department_id ), '
                                 
                                 'CompanyAverages AS ( '
                                 'SELECT D.department_id,'
                                 'AVG(E.salary) AS company_avg '
                                 'FROM T1 E JOIN DepartmentAverages D     '
                                 'ON E.department_id <> D.department_id     '
                                 'GROUP BY D.department_id )  '
                                 
                                 'SELECT D.department_id, D.department_avg, C.company_avg '
                                 'FROM DepartmentAverages D JOIN CompanyAverages C '
                                 'ON D.department_id = C.department_id '
                                 'WHERE D.department_avg < C.company_avg'
                                 )
        return deptAvgSalDf

    #PySpark Approach
    def getAvgSalary(self, inputDf):
        deptAvgSalDf = inputDf.groupBy(col('department_id')).agg(avg('salary')).alias('T2')

        cmpnyAvgSalDf = inputDf.alias('T1').join(deptAvgSalDf, inputDf['department_id'] != deptAvgSalDf['department_id'], 'inner')
        cmpnyAvgSalDf = cmpnyAvgSalDf.groupBy(col('T2.department_id')).agg(avg('salary')).alias('T3')

        lowerAvgDeptDf = deptAvgSalDf.join(cmpnyAvgSalDf, on='department_id', how='inner')\
                                    .select(col('department_id'),col('T2.avg(salary)').alias('avg_dept_salary'), col('T3.avg(salary)').alias('avg_cmpny_salary'))\
                                    .filter(col('avg_dept_salary') < col('avg_cmpny_salary'))
        return lowerAvgDeptDf

obj = AverageSalary()
inputDf = obj.createData()

resultDf = obj.getAvgSalary_SQL(inputDf)
resultDf.show()

resultAvgDf = obj.getAvgSalary(inputDf)
resultAvgDf.show()