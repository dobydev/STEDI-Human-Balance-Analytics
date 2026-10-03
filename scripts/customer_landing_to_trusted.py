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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1791026246142 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing", transformation_ctx="AWSGlueDataCatalog_node1791026246142")

# Script generated for node SQL Query
SqlQuery1243 = '''
select * from myDataSource
WHERE sharewithresearchasofdate IS NOT NULL
'''
SQLQuery_node1791026728448 = sparkSqlQuery(glueContext, query = SqlQuery1243, mapping = {"myDataSource":AWSGlueDataCatalog_node1791026246142}, transformation_ctx = "SQLQuery_node1791026728448")

# Script generated for node Amazon S3
AmazonS3_node1791026807480 = glueContext.getSink(path="s3://stedi-human-balance-gdoby/customer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1791026807480")
AmazonS3_node1791026807480.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
AmazonS3_node1791026807480.setFormat("json")
AmazonS3_node1791026807480.writeFrame(SQLQuery_node1791026728448)
job.commit()