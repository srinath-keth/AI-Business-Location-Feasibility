import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lower, col
from awsglue.dynamicframe import DynamicFrame

# Initialize Glue context
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define the Glue database and table
db_name = "etl-capstone-project-db"  # Replace with your Glue catalog database name
tbl_name = "capstone_property_data_csv"  # Replace with your Glue catalog table name

# Load data from the Glue Data Catalog
property_data = glueContext.create_dynamic_frame.from_catalog(
    database=db_name,
    table_name=tbl_name
)

# Convert DynamicFrame to DataFrame for transformations
df = property_data.toDF()

# Step 1: Extract unique categories and convert them to lowercase
unique_categories = df.select(lower(col("category")).alias("category_name")).distinct()

# Convert DataFrame back to DynamicFrame for Glue to handle
categories_dynamic_frame = DynamicFrame.fromDF(unique_categories, glueContext, "categories_dynamic_frame")

# Step 2: Write unique categories to dim_categories table in PostgreSQL
glueContext.write_dynamic_frame.from_options(
    frame=categories_dynamic_frame,
    connection_type="postgresql",
    connection_options={
        "url": "url",
        "dbtable": "categories",
        "user": "postgres",
        "password": "your-password",
        "sslmode": "prefer",
        "connectTimeout": "10",
        "connectionName": "aws-rds"  # Ensure this connection is set up in Glue
    },
    transformation_ctx="write_categories"
)

# Commit the job
job.commit()
