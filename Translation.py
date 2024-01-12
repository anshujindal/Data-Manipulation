from googletrans import Translator, constants
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import trim
from pyspark.sql import functions as F
from datetime import datetime
import time

translator = Translator()

spark = SparkSession.builder.master("yarn").appName('SparkByExamples.com').getOrCreate()

df = spark.sql("""
select
*
from 
DB.TB
""")
df.createOrReplaceTempView("df")

pandas_df = df.toPandas()
df = pandas_df.drop_duplicates(subset=['commodity','energy_balance'])

df = df[['commodity','energy_balance']]


df['commodity_translated']=df['commodity'].apply(lambda x:translator.translate(x,dest='en').text)

df['energy_balance_translated']=df['energy_balance'].apply(lambda x:translator.translate(x,dest='en').text)

df_spark = spark.createDataFrame(df)
df_spark.createOrReplaceTempView("df2")

joindf = spark.sql(
"""
select a.source,
a.table_name,
a.commodity,
a.commodity_translated,
case when 
trim(c.commodity_hierarchy) is null then ""
else trim(c.commodity_hierarchy)
end as commodity_hierarchy,
a.quote_date,
a.`location`,
a.uom,
a.frequency,
a.energy_balance,
a.energy_balance_translated,
a.quantity,
a.updated_date
from (select a.source,
a.table_name,
a.commodity,
case when 
a.commodity = "Nafta" then "Naphtha"
when a.commodity = "E85/ED95" then "E85/ED95" 
when a.commodity = "1.4 Summa rent biodrivmedel" then "1.4 Total clean biofuel"
when a.commodity = "Ren annan bio-bensin" then "Pure other bio-gasoline"
when a.commodity = "Ren annan biodiesel" then "Pure other biodiesel"
when a.commodity = "Ren FAME" then "Pure FAME"
else b.commodity_translated end as commodity_translated,
a.quote_date,
a.`location`,
a.uom,
a.frequency,
a.energy_balance,
b.energy_balance_translated,
a.quantity,
a.updated_date
from 
df a
left join
(select * from df2) b
on 
(a.commodity = b.commodity
and 
a.energy_balance = b.energy_balance)) a
left join DB.TB c on a.commodity_translated = c.commodity_translated

"""
)
joindf.createOrReplaceTempView("df3")

dfexcept = spark.sql("""SELECT 
source,
table_name,
commodity,
commodity_translated,
commodity_hierarchy,
quote_date,
`location`,
uom,
frequency,
energy_balance,
energy_balance_translated,
quantity,
updated_date
FROM 
(select * From 
(select *, ROW_NUMBER() OVER (PARTITION BY source,table_name,commodity,commodity_translated,commodity_hierarchy,
`location`,uom,frequency,energy_balance,energy_balance_translated, quote_date ORDER BY updated_date desc) as Row_num3
from 
(select *, ROW_NUMBER() OVER (PARTITION BY source,table_name,commodity,commodity_translated,commodity_hierarchy,
`location`,uom,frequency,energy_balance,energy_balance_translated, 
quote_date, quantity ORDER BY updated_date asc) as Row_num2 from 
(select *, 
ROW_NUMBER() OVER (PARTITION BY source,table_name,commodity,commodity_translated,commodity_hierarchy, quote_date,`location`,uom,frequency,energy_balance,energy_balance_translated,quantity, updated_date ORDER BY updated_date) as Row_num
FROM df3) f
where Row_num = 1) g where row_num2 = 1) h where row_num3 = 1) a""")
dfexcept.createOrReplaceTempView("dfexcept")

dfexcept.write.mode('append').insertInto("DB.TB")
