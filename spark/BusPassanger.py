"""
Table 1:
Buses (id, origin, destination, time)

Table2:
Passengers (id, origin, destination, time)

There could be multiple busses for same origin, destination. Passenger will board bus the earliest bus as per their schedule.
write query that returns the number of passenger boarding for each bus.
"""

import os, sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.master('local[*]').getOrCreate()

class BusPassanger:
    def createBusesData(self):
        data = [(1, 'Delhi', 'Mumbai', 7.0),
                (2, 'Delhi', 'Jaipur', 8.0),
                (3, 'Jaipur', 'Mumbai', 7.0),
                (4, 'Mumbai', 'Pune', 8.0),
                (5, 'Delhi', 'Mumbai', 9.0),
                (6, 'Delhi', 'Jaipur', 9.0),
                (7, 'Delhi', 'Jaipur', 7.3)]
        schema = ['id', 'origin', 'destination', 'time']
        return spark.createDataFrame(data, schema)

    def createPassangerData(self):
        data = [(1, 'Delhi', 'Mumbai', 8.0),
                (2, 'Delhi', 'Jaipur', 8.15),
                (3, 'Jaipur', 'Mumbai', 7.0),
                (4, 'Mumbai', 'Pune', 7.0),
                (5, 'Delhi', 'Mumbai', 8.0),
                (6, 'Delhi', 'Jaipur', 8.30),
                (7, 'Delhi', 'Jaipur', 7.45)]
        schema = ['id', 'origin', 'destination', 'time']
        return spark.createDataFrame(data, schema)

    def numPassanger(self, busDf, passDf):
        busDf.createOrReplaceTempView('buses')
        passDf.createOrReplaceTempView('passanger')

        query = '''
        with cte as (
        select b.id,
        p.id as p_id,
        p.time as p_time,
        row_number() over(partition by p.id order by b.time) as rn
        from buses b join passanger p
        on b.origin = p.origin and b.destination = p.destination and b.time >= p.time
        )
        select b.id, count(p_id) as num_passanger
        from buses b
        left join cte
        on cte.id = b.id
        and rn = 1
        group by b.id
        order by b.id
        '''
        return spark.sql(query)

    def passList(self, busDf, passDf):
        # Define the window specification for row_number
        windowSpec = Window.partitionBy("p.id").orderBy("b.time")

        # Step 1: Join buses and passengers on origin, destination, and time conditions
        joined_df = busDf.alias("b").join(
            passDf.alias("p"),
            (col("b.origin") == col("p.origin")) &
            (col("b.destination") == col("p.destination")) &
            (col("b.time") >= col("p.time"))
        ).select(
            col("b.id").alias("b_id"),
            col("p.id").alias("p_id"),
            col("p.time").alias("p_time"),
            row_number().over(windowSpec).alias("rn"))


        # Step 3: Filter to get only the first bus (earliest) for each passenger
        filtered_cte = joined_df.filter(col("rn") == 1)

        # Step 4: Left join with the original buses DataFrame and count passengers
        result_df = busDf.alias("b").join(
            filtered_cte.alias("cte"),
            col("b.id") == col("cte.b_id"),
            how="left"
        ).groupBy("b.id").agg(
            count("cte.p_id").alias("num_passenger")
        ).orderBy("b.id")

        return result_df

ob = BusPassanger()
busDf = ob.createBusesData()
passDf = ob.createPassangerData()

resultDf = ob.numPassanger(busDf, passDf)
resultDf.show()

resultDf = ob.passList(busDf, passDf)
resultDf.show()