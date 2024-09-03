"""
You have a table exp_items which has 2 columns fruit and price. fruit is a primary key.
There is another table val_items which has the same columns fruit and price.

exp_items
fruit  |  price
-------|---------
apple  |  1.00
orange |  1.50
banana |  2.00
grape  |  1.00

val_items
fruit  |  price
-------|---------
apple  |  1.00
orange |  1.50
pear   |  3.00
grape  |  2.00


1. In the exp_items table, write a query to select all fruits cheaper than 2.00.
2. Determine which items exist in val_items but not in exp_items.
3. Write a query to determine which fruits have a different price in exp_items than in val_items and display the price in exp_items, price in val_items, and the difference.
4. Without using LEFT JOIN, how to determine which items exist in val_items but not in exp_items?
"""

import os, sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, col, when, lit

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.master('local[*]').getOrCreate()

class FruitsItems:
    def createData(self):
        exp_data = [('apple', 1.00),
                    ('orange', 1.50),
                    ('banana', 2.00),
                    ('grape', 1.00)]

        val_data = [('apple', 1.00),
                    ('orange', 1.50),
                    ('pear', 3.00),
                    ('grape', 2.00)]

        schema = ['fruit', 'price']
        return spark.createDataFrame(exp_data, schema), spark.createDataFrame(val_data, schema)

    #select all fruits cheaper than 2.00
    def fruits_less_than_2(self, exp_items):
        exp_items.registerTempTable('exp')
        query = '''
        select * from exp where price < 2.0
        '''
        return spark.sql(query)

    #which items exist in val_items but not in exp_items
    def items_not_in_exp(self, exp_items, val_items):
        exp_items.registerTempTable('exp')
        val_items.registerTempTable('val')
        query = '''
        select fruit from val where fruit not in (select fruit from exp)
        '''
        query2 = '''
        select v.fruit from val v left join exp e on v.fruit = e.fruit where e.fruit is null
        '''
        return spark.sql(query2)

    #determine which fruits have a different price in exp_items than in val_items and display the price in exp_items, price in val_items, and the difference.
    def price_diff(self, exp_items, val_items):
        exp_items.registerTempTable('exp')
        val_items.registerTempTable('val')

        query = '''select e.fruit, e.price as exp_price, v.price as v_price, (v.price - e.price) as difference
        from exp e inner join val v
        on e.fruit = v.fruit
        '''
        return spark.sql(query)

ob = FruitsItems()
exp_items, val_items = ob.createData()

res1_df = ob.fruits_less_than_2(exp_items)
res1_df.show()

res2_df = ob.items_not_in_exp(exp_items, val_items)
res2_df.show()

res3_df = ob.price_diff(exp_items, val_items)
res3_df.show()