from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,ArrayType,IntegerType,StringType
from pyspark.sql.functions import *
from pyspark.sql.functions import size,col
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

user_schema = StructType([StructField("user_id",IntegerType(),nullable=False),StructField("user_first_name",StringType(),nullable=False),StructField("user_last_name",StringType(),nullable=False),StructField("user_email",StringType(),nullable=False),StructField("user_gender",StringType(),nullable=False),StructField("user_phone_numbers",ArrayType(StringType()),nullable=True),StructField("user_address",StructType([StructField("street",StringType(),nullable=False),StructField("city",StringType(),nullable=False),StructField("state",StringType(),nullable=False),StructField("postal_code",StringType(),nullable=False),]),nullable=False)])

#reading the files
user_df=spark.read \
.format("json") \
.schema(user_schema) \
.load("/public/sms/users/")

#creating new columns and temp view
user_df.withColumn("user_street",col("user_address.street")).withColumn("user_city",col("user_address.city")).withColumn("user_state",col("user_address.state")).withColumn("user_postal_code",col("user_address.postal_code")).withColumn("u_phone_numbers",size(col("user_phone_numbers"))).createOrReplaceTempView("users_data")

#pivot table with state and gender
city_gender_pivot = spark.sql("select * from users_data where user_phone_numbers is not null and user_gender is not null").groupBy("user_state").pivot("user_gender").count().show()


spark.write \
.format("parquet") \
.mode("overwrite") \
.option("path","/user/itv010130/data") \
.save()

spark.stop()

