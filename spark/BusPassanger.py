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
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DateType

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

ob = BusPassanger()
busDf = ob.createBusesData()
passDf = ob.createPassangerData()

resultDf = ob.numPassanger(busDf, passDf)
resultDf.show()