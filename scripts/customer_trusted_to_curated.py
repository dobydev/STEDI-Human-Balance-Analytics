import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1791028128256 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1791028128256")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1791028158998 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1791028158998")

# Script generated for node SQL Query
SqlQuery1278 = '''
SELECT DISTINCT
    c.serialnumber,
    c.sharewithpublicasofdate,
    c.birthday,
    c.registrationdate,
    c.sharewithresearchasofdate,
    c.customername,
    c.email,
    c.lastupdatedate,
    c.phone,
    c.sharewithfriendsasofdate
FROM c
JOIN a
    ON c.email = a.user
'''
SQLQuery_node1791028196296 = sparkSqlQuery(glueContext, query = SqlQuery1278, mapping = {"c":CustomerTrusted_node1791028128256, "a":AccelerometerTrusted_node1791028158998}, transformation_ctx = "SQLQuery_node1791028196296")

# Script generated for node Amazon S3
AmazonS3_node1791028258831 = glueContext.getSink(path="s3://stedi-human-balance-gdoby/customer_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1791028258831")
AmazonS3_node1791028258831.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
AmazonS3_node1791028258831.setFormat("json")
AmazonS3_node1791028258831.writeFrame(SQLQuery_node1791028196296)
job.commit()