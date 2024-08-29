"""
-- Tablename: salary
-- ╒═══════╤═════╤════════╤═══════════════╕
-- │ name  │ age │ gender │ annual_salary │
-- ╞═══════╪═════╪════════╪═══════════════╡
-- │ peter │ 23  │ M      │ 100,000       │
-- ├───────┼─────┼────────┼───────────────┤
-- │ bruce │ 34  │ M      │ 20,000        │
-- ├───────┼─────┼────────┼───────────────┤
-- │ clark │ 20  │ M      │ 170,000       │
-- ├───────┼─────┼────────┼───────────────┤
-- │ bruce │ 44  │ M      │ 40,000        │
-- ├───────┼─────┼────────┼───────────────┤
-- │ tony  │ 50  │ M      │ 50,000        │
-- ├───────┼─────┼────────┼───────────────┤
-- │ carol │ 26  │ F      │ 75,000        │
-- ├───────┼─────┼────────┼───────────────┤
-- │ steve │ 170 │ M      │ 10,000        │
-- ╘═══════╧═════╧════════╧═══════════════╛

-- 1. What can you tell me about the data in the Salary table

-- Tablename: employee
-- ╒══════╤════════════╤═══════════╤══════════════════════════════╕
-- │ id   │ first_name │ last_name │ email                        │
-- ╞══════╪════════════╪═══════════╪══════════════════════════════╡
-- │ 1245 │ peter      │ parker    │ peter.parker@carcompany.com  │
-- ├──────┼────────────┼───────────┼──────────────────────────────┤
-- │ 6487 │ bruce      │ banner    │ bruce.banner@carcompany.com  │
-- ├──────┼────────────┼───────────┼──────────────────────────────┤
-- │ 1576 │ clark      │ kent      │ clark .kent@carcompany.com   │
-- ├──────┼────────────┼───────────┼──────────────────────────────┤
-- │ 1897 │ bruce      │ wayne     │ bruce.wayne@carcompany.com   │
-- ├──────┼────────────┼───────────┼──────────────────────────────┤
-- │ 1575 │ tony       │ stark     │ tony.stark@carcompany.com    │
-- ├──────┼────────────┼───────────┼──────────────────────────────┤
-- │ 1154 │ carol      │ danvers   │ carol.danvers@carcompany.com │
-- ├──────┼────────────┼───────────┼──────────────────────────────┤
-- │ 2345 │ steve      │ rogers    │ steve.rogers@carcompany.com  │
-- ╘══════╧════════════╧═══════════╧══════════════════════════════╛

-- Tablename: sales
-- ╒══════╤════════════╤═══════════╤══════════════════════╕
-- │ id   │ date_sold  │ time_sold │ car_model            │
-- ╞══════╪════════════╪═══════════╪══════════════════════╡
-- │ 1245 │ 2022-08-23 │ 12:43     │ 2022 Acura Integra   │
-- ├──────┼────────────┼───────────┼──────────────────────┤
-- │ 6487 │ 2022-08-23 │ 15:55     │ 2021 Audi e-tron     │
-- ├──────┼────────────┼───────────┼──────────────────────┤
-- │ 1576 │ 2022-08-23 │ 9:36      │ 2022 Cadillac        │
-- ├──────┼────────────┼───────────┼──────────────────────┤
-- │ 1897 │ 2022-08-23 │ 18:13     │ 2022 Genesis         │
-- ├──────┼────────────┼───────────┼──────────────────────┤
-- │ 1897 │ 2022-08-22 │ 18:00     │ 2018 Ford Maverick   │
-- ├──────┼────────────┼───────────┼──────────────────────┤
-- │ 1897 │ 2022-08-21 │ 19:23     │ 2017 Genesis         │
-- ├──────┼────────────┼───────────┼──────────────────────┤
-- │ 1575 │ 2022-08-19 │ 18:43     │ 2022 Hyundai Elantra │
-- ├──────┼────────────┼───────────┼──────────────────────┤
-- │ 2345 │ 2022-08-17 │ 18:33     │ 2022 Hyundai Elantra │
-- ├──────┼────────────┼───────────┼──────────────────────┤
-- │ 1154 │ 2022-08-17 │ 16:23     │ 2021 Cadillac        │
-- ├──────┼────────────┼───────────┼──────────────────────┤
-- │ 1154 │ 2022-08-19 │ 18:24     │ 2022 Genesis         │
-- ├──────┼────────────┼───────────┼──────────────────────┤
-- │ 2345 │ 2022-08-21 │ 19:23     │ 2018 Ford Maverick   │
-- ╘══════╧════════════╧═══════════╧══════════════════════╛



-- 2. Write a query that would generate the following headers.
-- We are trying to get the first + last name of sales associate and how many cars they sold between a start_date and end_date
-- ╒════════════╤═══════════╤══════════╕
-- │ first_name │ last_name │ car_sold │
-- ╘════════════╧═══════════╧══════════╛


-- 3. How would you select the sales associate who sold the second highest amount of sales

"""

import os, sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, col, when, lit
from pyspark.sql.window import Window
from pyspark.sql.types import StructType

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.master('local[*]').getOrCreate()

class EmployeeDetails:
    def createSalaryDf(self):
        data = [('peter', 23, 'M', 100000),
                ('bruce', 34, 'M', 20000),
                ('clark', 20, 'M', 170000),
                ('bruce', 44, 'M', 40000),
                ('tony ', 50, 'M', 50000),
                ('carol', 26, 'F', 75000),
                ('steve', 37, 'M', 10000)]
        schema = ['name', 'age', 'gender', 'salary']
        return spark.createDataFrame(data, schema)

    def createEmployeeDf(self):
        data = [(1245, 'peter', 'parker', 'peter.parker@carcompany.com'),
                (6487, 'bruce', 'banner', 'bruce.banner@carcompany.com'),
                (1576, 'clark', 'kent', 'clark .kent@carcompany.com'),
                (1897, 'bruce', 'wayne', 'bruce.wayne@carcompany.com'),
                (1575, 'tony', 'stark', 'tony.stark@carcompany.com'),
                (1154, 'carol', 'danvers', 'carol.danvers@carcompany.com'),
                (2345, 'steve', 'rogers', 'steve.rogers@carcompany.com')]
        schema = ['id', 'first_name', 'last_name', 'email']
        return spark.createDataFrame(data, schema)

    def createSalesDf(self):
        data = [
            (1245, '2022-08-23', '12:43', '2022 Acura Integra'),
            (6487, '2022-08-23', '15:55', '2021 Audi e-tron'),
            (1576, '2022-08-23', '9:36', '2022 Cadillac'),
            (1897, '2022-08-23', '18:13', '2022 Genesis'),
            (1897, '2022-08-22', '18:00', '2018 Ford Maverick  '),
            (1897, '2022-08-21', '19:23', '2017 Genesis'),
            (1575, '2022-08-19', '18:43', '2022 Hyundai Elantra'),
            (2345, '2022-08-17', '18:33', '2022 Hyundai Elantra'),
            (1154, '2022-08-17', '16:23', '2021 Cadillac'),
            (1154, '2022-08-19', '18:24', '2022 Genesis'),
            (2345, '2022-08-21', '19:23', '2018 Ford Maverick')
        ]
        schema = ['id', 'date_sold', 'time_sold', 'car_model']
        return spark.createDataFrame(data, schema)

    def empSalesDetails(self, empDf, salesDf):
        empDf.registerTempTable('emp')
        salesDf.registerTempTable('sales')

        query = '''
        with cte as (
        select id, count(id) as car_sold
        from sales s
        group by id
        )
        select
        e.first_name,
        e.last_name,
        s.car_sold
        from emp e join cte s
        on e.id = s.id
        '''
        return spark.sql(query)

    def secondSalesEmp(self, resDf):
        resDf.registerTempTable('car')
        query = '''
        with cte as (
        select 
        c.first_name, c.last_name,
        dense_rank() over(order by car_sold desc) as rnk
        from car c
        )
        select 
        c.first_name, c.last_name
        from cte c
        where rnk = 2
        '''
        return spark.sql(query)

ob = EmployeeDetails()
salaryDf = ob.createSalaryDf()
empDf = ob.createEmployeeDf()
salesDf = ob.createSalesDf()

#salaryDf.show()
#empDf.show()
#salesDf.show()

res=ob.empSalesDetails(empDf, salesDf)
res.show()
res2 = ob.secondSalesEmp(res)
res2.show()