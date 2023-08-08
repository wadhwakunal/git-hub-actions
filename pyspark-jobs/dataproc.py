from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession \
.builder \
.appName("dataproc") \
.getOrCreate()

spark.conf.set("temporaryGcsBucket","kunal-bucket/tmp/")

schema = StructType([
    StructField('EMPLOYEE_ID',
                IntegerType(), True),
    StructField('FIRST_NAME',
                StringType(), True),
    StructField('LAST_NAME',
                StringType(), True),
    StructField('EMAIL',
                StringType(), True),
    StructField('PHONE_NUMBER',
                StringType(), True),
    StructField('HIRE_DATE',
                StringType(), True),
    StructField('JOB_ID',
                StringType(), True),
    StructField('SALARY',
                IntegerType(), True),
    StructField('COMMISSION_PCT',
                StringType(), True),
    StructField('MANAGER_ID',
                StringType(), True),
    StructField('DEPARTMENT_ID',
                IntegerType(), True)
])

df = spark \
.read \
.format("csv") \
.schema(schema) \
.option("header",True) \
.load("gs://kunal-bucket/data/employees.csv")

df \
.write \
.format("bigquery") \
.mode("overwrite") \
.option("table","poc.employees_composer") \
.save()