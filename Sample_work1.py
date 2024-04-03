from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,ArrayType,IntegerType,StringType
from pyspark.sql.functions import *
import getpass

username = getpass.getuser()
spark = SparkSession. \
    builder. \
    config('spark.ui.port','0'). \
    config("spark.sql.warehouse.dir","/user/itv010130/warehouse"). \
    enableHiveSupport(). \
    master('yarn'). \
    getOrCreate()

#creating schema for reading the json file
    
user_schema = StructType([
    StructField("user_id",IntegerType(),nullable=False),
    StructField("user_first_name",StringType(),nullable=False),
    StructField("user_last_name",StringType(),nullable=False),
    StructField("user_email",StringType(),nullable=False),
    StructField("user_gender",StringType(),nullable=False),
    StructField("user_phone_numbers",ArrayType(StringType()),nullable=True),
    StructField("user_address",StructType([
        StructField("street",StringType(),nullable=False),
        StructField("city",StringType(),nullable=False),
        StructField("state",StringType(),nullable=False),
        StructField("postal_code",StringType(),nullable=False),
    ]),nullable=False)
])

#reading the files
user_df = spark.read \
.format("json") \
.schema(user_schema) \
.load("/public/sms/users/")

#number of partitions
user_df.rdd.getNumPartitions()

#Count of records
user_df.count()

#extracting columns from nested json and creating views
user_df.createOrReplaceTempView("user_data")

#finding distinct user count for New York State
spark.sql("select count(distinct (user_id)) from user_data where user_address.state = 'New York'")

#State with max postal codes
spark.sql("select user_address.state,count(distinct user_address.postal_code) as max_postal_code from user_data group by user_address.state order by max_postal_code desc limit 1")

#City with maximum users
spark.sql("select user_address.city,count(distinct user_id) as city_max_users from user_data where user_address.city is not null group by user_address.city order by city_max_users desc limit 1")

#users having email domain as bizjournals.com
spark.sql("select count(distinct user_id) as num_user_id from user_data where user_email like '%bizjournals.com%'")

#users with 4 phone numbers
spark.sql("select count(distinct user_id) as num_users from (select user_id,size(user_phone_numbers) as count_phone_numbers from user_data) where count_phone_numbers = 4")

#users with no phone number
spark.sql("select count(distinct user_id) as num_users from user_data where user_phone_numbers is null")


user_df.write \
.format('parquet') \
.mode("overwrite") \
.option("path","/user/itv010130/data") \
.save()

spark.stop()