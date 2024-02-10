'''
 Find the origin and the destination of each customer.
Note : There can be more than 1 stops for the same customer journey.

(1, 'Flight2', 'Goa', 'Kochi'),
(1, 'Flight1', 'Delhi', 'Goa'),
(1, 'Flight3', 'Kochi', 'Hyderabad'),
(2, 'Flight1', 'Pune', 'Chennai'),
(2, 'Flight2', 'Chennai', 'Pune'),
(3, 'Flight1', 'Mumbai', 'Bangalore'),
(3, 'Flight2', 'Bangalore', 'Ayodhya'),
(4, 'Flight1', 'Ahmedabad', 'Indore'),
(4, 'Flight2', 'Indore', 'Kolkata'),
(4, 'Flight3', 'Ranchi', 'Delhi'),
(4, 'Flight4', 'Delhi', 'Mumbai')

Output-
|cust_id|   origin|destination|
+-------+---------+-----------+
|      1|    Delhi|  Hyderabad|
|      2|     Pune|       Pune|
|      3|   Mumbai|    Ayodhya|
|      4|Ahmedabad|     Kolkata|
|      4|Ranchi|     Mumbai|
'''

import os, sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.master('local[*]').getOrCreate()

class CustomerLocation:
    def createData(self):
        flights_data = [(1, 'Flight2', 'Goa', 'Kochi'),
                        (1, 'Flight1', 'Delhi', 'Goa'),
                        (1, 'Flight3', 'Kochi', 'Hyderabad'),
                        (2, 'Flight1', 'Pune', 'Chennai'),
                        (2, 'Flight2', 'Chennai', 'Pune'),
                        (3, 'Flight1', 'Mumbai', 'Bangalore'),
                        (3, 'Flight2', 'Bangalore', 'Ayodhya'),
                        (4, 'Flight1', 'Ahmedabad', 'Indore'),
                        (4, 'Flight2', 'Indore', 'Kolkata'),
                        (4, 'Flight3', 'Ranchi', 'Delhi'),
                        (4, 'Flight4', 'Kashmir', 'Mumbai')
                        ]
        schema = ['cust_id', 'flight_id', 'origin', 'destination']
        return spark.createDataFrame(flights_data, schema)

    def getResult(self, inputDf):
        inputDf.createOrReplaceTempView('table')
        query = """with prevDest as (
                    select *, 
                    LAG(destination) OVER(PARTITION BY cust_id ORDER BY flight_id) as prev_dest
                    from table
                    ),
                    partCol as (
                    select cust_id, origin, destination, 
                    sum(case when origin != prev_dest then 1 else 0 end) over(partition by cust_id order by flight_id) as value
                    from prevDest
                    )
                    select cust_id, 
                    first(origin) as origin, 
                    last(destination) as destination
                    from partCol
                    group by cust_id, value
                    """
        return spark.sql(query)

    def getPyResult(self, inputDf):
        prevDestDf = inputDf.withColumn('prev_dest', F.lag('destination').over(Window.partitionBy('cust_id').orderBy('flight_id')))
        partColDf = prevDestDf.withColumn('value', F.sum(F.when(F.col('origin') != F.col('prev_dest'), 1).otherwise(0)).over(Window.partitionBy('cust_id').orderBy('flight_id')))

        resultDf = partColDf.groupBy('cust_id', 'value')\
                            .agg(F.first('origin').alias('origin'),F.last('destination').alias('destination'))\
                            .drop('value')
        return resultDf

ob = CustomerLocation()

inputDf = ob.createData()
resultDf = ob.getResult(inputDf)
resultDf.show()
resultDf = ob.getPyResult(inputDf)
resultDf.show()