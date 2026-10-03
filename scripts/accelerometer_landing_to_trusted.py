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
CustomerTrusted_node1791027360159 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1791027360159")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1791027390370 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1791027390370")

# Script generated for node SQL Query
SqlQuery1339 = '''
SELECT
    a.timestamp,
    a.user,
    a.x,
    a.y,
    a.z
FROM a
JOIN c
    ON a.user = c.email
'''
SQLQuery_node1791027414497 = sparkSqlQuery(glueContext, query = SqlQuery1339, mapping = {"a":AccelerometerLanding_node1791027390370, "c":CustomerTrusted_node1791027360159}, transformation_ctx = "SQLQuery_node1791027414497")

# Script generated for node Amazon S3
AmazonS3_node1791027733583 = glueContext.getSink(path="s3://stedi-human-balance-gdoby/accelerometer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1791027733583")
AmazonS3_node1791027733583.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AmazonS3_node1791027733583.setFormat("json")
AmazonS3_node1791027733583.writeFrame(SQLQuery_node1791027414497)
job.commit()