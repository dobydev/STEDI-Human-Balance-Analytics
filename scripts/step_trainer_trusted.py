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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1791028755912 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1791028755912")

# Script generated for node Customer Curated
CustomerCurated_node1791028771767 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCurated_node1791028771767")

# Script generated for node SQL Query
SqlQuery1273 = '''
SELECT DISTINCT
    s.sensorreadingtime,
    s.serialnumber,
    s.distancefromobject
FROM s
WHERE s.serialnumber IN (
    SELECT c.serialnumber
    FROM c
)
'''
SQLQuery_node1791028787534 = sparkSqlQuery(glueContext, query = SqlQuery1273, mapping = {"c":CustomerCurated_node1791028771767, "s":StepTrainerLanding_node1791028755912}, transformation_ctx = "SQLQuery_node1791028787534")

# Script generated for node Amazon S3
AmazonS3_node1791028856501 = glueContext.getSink(path="s3://stedi-human-balance-gdoby/step_trainer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1791028856501")
AmazonS3_node1791028856501.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
AmazonS3_node1791028856501.setFormat("json")
AmazonS3_node1791028856501.writeFrame(SQLQuery_node1791028787534)
job.commit()