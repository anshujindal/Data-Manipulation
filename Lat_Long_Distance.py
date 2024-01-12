#pip install geopy


# importing geopy library and Nominatim class
from geopy.geocoders import Nominatim

import pandas as pd
from pandas import melt
import numpy as np
import requests
import json

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col,lit,split
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import trim
from pyspark.sql import functions as F
from datetime import datetime
import time

import geopy.distance
from math import sin, cos, sqrt, atan2, radians

import geopy.distance
from geopy.distance import geodesic as GD
from math import sin, cos, sqrt, atan2, radians
from typing import Tuple

#from pandasql import sqldf
from pandas.tseries.offsets import MonthBegin
import datetime
import traceback
error = traceback.format_exc() 
import csv
from io import StringIO
    
##sending error mail
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

#------------------------------------------------------------------

spark = SparkSession.builder.master("yarn").appName('SparkByExamples.com').getOrCreate()

def lat_from_address(a):
    try:
        t= requests.get(
            "https://singlesearch.alk.com/NA/api/search",
            params={
                "authToken": '157BB7B3527D7345813ADB21F429877E',
                'query': a
            }).text
        coords= json.loads(t)['Locations'][0]['Coords']
        lat = float(coords['Lat'])
        return lat
    except Exception:
        return np.nan
      
def long_from_address(a):
    try:
        t= requests.get(
            "https://singlesearch.alk.com/NA/api/search",
            params={
                "authToken": '157BB7B3527D7345813ADB21F429877E',
                'query': a
            }).text
        coords= json.loads(t)['Locations'][0]['Coords']
        lon = float(coords['Lon'])
        return lon
    except Exception:
        return np.nan
      

df = spark.sql("""
select 
customer_id, customer_parent_id, customer_child_id, 
customer_address
from DB-NAME.TABLE-NAME
""")
df.createOrReplaceTempView("df")
pandas_df = df.toPandas()

pandas_df["lat"] = pandas_df.apply(lambda x: lat_from_address(x['customer_address']),axis = 1)
pandas_df["long"] = pandas_df.apply(lambda x: long_from_address(x['customer_address']),axis = 1)

sparkDF=spark.createDataFrame(pandas_df)

sparkDF.write.mode('append').insertInto("DB-NAME.TABLE-NAME")

#-------------------------------------------------------------------

df = spark.sql("""
select 
customer_id, customer_parent_id, customer_child_id, 
billing_address
from DB-NAME.TABLE-NAME
""")
df.createOrReplaceTempView("df")
pandas_df = df.toPandas()

pandas_df["lat"] = pandas_df.apply(lambda x: lat_from_address(x['billing_address']),axis = 1)
pandas_df["long"] = pandas_df.apply(lambda x: long_from_address(x['billing_address']),axis = 1)

sparkDF=spark.createDataFrame(pandas_df)

sparkDF.write.mode('append').insertInto("DB-NAME.TABLE-NAME")

#-------------------------------------------------------------------

df = spark.sql("""
select 
cast(customer_id as string) as customer_id, customer_parent_id, customer_child_id, 
shipping_address
from DB-NAME.TABLE-NAME
""")
df.createOrReplaceTempView("df")
pandas_df = df.toPandas()
print(pandas_df)

pandas_df["lat"] = pandas_df.apply(lambda x: lat_from_address(x['shipping_address']),axis = 1)
pandas_df["long"] = pandas_df.apply(lambda x: long_from_address(x['shipping_address']),axis = 1)

sparkDF=spark.createDataFrame(pandas_df)

sparkDF.write.mode('append').insertInto("DB-NAME.TABLE-NAME")

#-------------------------------------------------------------------

def dist(a,b,c,d):
    R = 6373.0

    lat1 = radians(a)
    lon1 = radians(b)

    lat2 = radians(c)
    lon2 = radians(d)

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c * 0.6213711922

    return distance

df = spark.sql("""
select 
cast(customer_id as string) as customer_id,
customer_parent_id, 
customer_child_id, 
lynx_id,
customer_name, 
customer_street, 
customer_city, 
customer_postalcode, 
customer_state_province,
customer_address, 
customer_latitude, 
customer_longitude, 
billingstreet,
billingcity,
billingpostalcode,
billingstate,
billing_address,
billinglatitude, 
billinglongitude, 
shippingstreet,
shippingcity,
shippingpostalcode,
shippingstate,
shipping_address,
shippinglatitude,
shippinglongitude,
location_company,
contract_price_basis_place,
location_name,
location_code,
location_city,
location_postal_code,
location_state,
location_country,
location_latitude,
location_longitude
From DB-NAME.TABLE-NAME
""")

df.createOrReplaceTempView("df")
pandas_df = df.toPandas()

pandas_df["billing_distance_in_miles"] = pandas_df.apply(lambda x: dist(x['billinglatitude'], x['billinglongitude'],x['location_latitude'], x['location_longitude']),axis = 1)

pandas_df["customer_distance_in_miles"] = pandas_df.apply(lambda x: dist(x['customer_latitude'], x['customer_longitude'],x['location_latitude'], x['location_longitude']),axis = 1)

pandas_df["shipping_distance_in_miles"] = pandas_df.apply(lambda x: dist(x['shippinglatitude'], x['shippinglongitude'],x['location_latitude'], x['location_longitude']),axis = 1)

pandas_df = pandas_df.where(pandas_df.notna(), None)

pandas_df1 = pd.melt(pandas_df, id_vars = ("customer_id",
"customer_parent_id", 
"customer_child_id", 
"lynx_id",
"customer_name", 
"customer_street", 
"customer_city", 
"customer_postalcode", 
"customer_state_province",
"customer_address", 
"customer_latitude", 
"customer_longitude", 
"billingstreet",
"billingcity",
"billingpostalcode",
"billingstate",
"billing_address",
"billinglatitude", 
"billinglongitude", 
"shippingstreet",
"shippingcity",
"shippingpostalcode",
"shippingstate",
"shipping_address",
"shippinglatitude",
"shippinglongitude",
"location_company",
"contract_price_basis_place",
"location_name",
"location_code",
"location_city",
"location_postal_code",
"location_state",
"location_country",
"location_latitude",
"location_longitude"), value_vars = ("billing_distance_in_miles"))

pandas_df2 = pd.melt(pandas_df, id_vars = ("customer_id",
"customer_parent_id", 
"customer_child_id", 
"lynx_id",
"customer_name", 
"customer_street", 
"customer_city", 
"customer_postalcode", 
"customer_state_province",
"customer_address", 
"customer_latitude", 
"customer_longitude", 
"billingstreet",
"billingcity",
"billingpostalcode",
"billingstate",
"billing_address",
"billinglatitude", 
"billinglongitude", 
"shippingstreet",
"shippingcity",
"shippingpostalcode",
"shippingstate",
"shipping_address",
"shippinglatitude",
"shippinglongitude",
"location_company",
"contract_price_basis_place",
"location_name",
"location_code",
"location_city",
"location_postal_code",
"location_state",
"location_country",
"location_latitude",
"location_longitude"), value_vars = ("customer_distance_in_miles"))

pandas_df3 = pd.melt(pandas_df, id_vars = ("customer_id",
"customer_parent_id", 
"customer_child_id", 
"lynx_id",
"customer_name", 
"customer_street", 
"customer_city", 
"customer_postalcode", 
"customer_state_province",
"customer_address", 
"customer_latitude", 
"customer_longitude", 
"billingstreet",
"billingcity",
"billingpostalcode",
"billingstate",
"billing_address",
"billinglatitude", 
"billinglongitude", 
"shippingstreet",
"shippingcity",
"shippingpostalcode",
"shippingstate",
"shipping_address",
"shippinglatitude",
"shippinglongitude",
"location_company",
"contract_price_basis_place",
"location_name",
"location_code",
"location_city",
"location_postal_code",
"location_state",
"location_country",
"location_latitude",
"location_longitude"), value_vars = ("shipping_distance_in_miles"))

sparkDF1=spark.createDataFrame(pandas_df1)
sparkDF2=spark.createDataFrame(pandas_df2)
sparkDF3=spark.createDataFrame(pandas_df3)
sparkDF=spark.createDataFrame(pandas_df)

sparkDF1.write.mode('append').insertInto("DB-NAME.TABLE-NAME")
sparkDF2.write.mode('append').insertInto("DB-NAME.TABLE-NAME")
sparkDF3.write.mode('append').insertInto("DB-NAME.TABLE-NAME")
sparkDF.write.mode('append').insertInto("DB-NAME.TABLE-NAME")

---------------------------------------

#a = lat_from_address('3727 Creek Road,43074,Sunbury,Ohio')
#b = long_from_address('3727 Creek Road,43074,Sunbury,Ohio')
#print(a,b)
#
#c = lat_from_address('45233, Cincinati river road, Cincinati, Ohio')
#d= long_from_address('45233, Cincinati river road, Cincinati, Ohio')
#print(c,d)
#
#d = dist(a,b,c,d)
#print(d)
#
#def distance(
#    lat: float, lon: float, fixed_coords: Tuple[float] = (39.117467, -84.674514)
#) -> float:
#    return geopy.distance.distance((lat, lon), fixed_coords).km
#  
#distance (40.278497,-82.8216)
