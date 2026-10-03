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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1791029261339 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1791029261339")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1791029275871 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1791029275871")

# Script generated for node SQL Query
SqlQuery1274 = '''
SELECT
    s.sensorreadingtime,
    s.serialnumber,
    s.distancefromobject,
    a.user,
    a.x,
    a.y,
    a.z
FROM s
JOIN a
    ON s.sensorreadingtime = a.timestamp
'''
SQLQuery_node1791029295277 = sparkSqlQuery(glueContext, query = SqlQuery1274, mapping = {"a":AccelerometerTrusted_node1791029275871, "s":StepTrainerTrusted_node1791029261339}, transformation_ctx = "SQLQuery_node1791029295277")

# Script generated for node Amazon S3
AmazonS3_node1791029361515 = glueContext.getSink(path="s3://stedi-human-balance-gdoby/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1791029361515")
AmazonS3_node1791029361515.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
AmazonS3_node1791029361515.setFormat("json")
AmazonS3_node1791029361515.writeFrame(SQLQuery_node1791029295277)
job.commit()