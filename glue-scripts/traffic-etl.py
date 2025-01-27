import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, lit

# Initialize Glue context and job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load the traffic data from the Glue Data Catalog
db_name = "etl-capstone-project-db"  # Glue catalog database
tbl_name = "traffic_data_csv"  # Glue catalog table

# Load data into a DynamicFrame and convert to DataFrame for transformations
traffic_dyf = glueContext.create_dynamic_frame.from_catalog(database=db_name, table_name=tbl_name)
traffic_df = traffic_dyf.toDF()

# Step 1: Clean up column names for ease of use
traffic_df = traffic_df \
    .withColumnRenamed("Nearest_Station", "nearest_station") \
    .withColumnRenamed("Distance_to_Station_m", "distance_to_station_m") \
    .withColumnRenamed("Distance to Nearest Traffic Point (m)", "distance_to_nearest_traffic_point_m") \
    .withColumnRenamed("Nearest Traffic BACK_AADT", "nearest_traffic_back_aadt") \
    .withColumnRenamed("Nearest Traffic AHEAD_AADT", "nearest_traffic_ahead_aadt") \
    .withColumnRenamed("Property_ID", "property_id")  # Ensure property_id is included

# Step 2: Convert distances from meters to kilometers
traffic_df = traffic_df.withColumn("distance_to_station_km", col("distance_to_station_m") / 1000)
traffic_df = traffic_df.withColumn("distance_to_nearest_traffic_point_km", col("distance_to_nearest_traffic_point_m") / 1000)

# Step 3: Handle missing values
traffic_df = traffic_df.fillna({"nearest_station": "Unknown"})

# Fill numerical columns with average values to prevent null issues
traffic_df = traffic_df.fillna({
    "distance_to_station_km": traffic_df.agg({"distance_to_station_km": "mean"}).first()[0],
    "nearest_traffic_back_aadt": traffic_df.agg({"nearest_traffic_back_aadt": "mean"}).first()[0],
    "nearest_traffic_ahead_aadt": traffic_df.agg({"nearest_traffic_ahead_aadt": "mean"}).first()[0]
})

# Step 4: Calculate necessary fields as per target schema
traffic_df = traffic_df.withColumn("total_aadt", col("nearest_traffic_back_aadt") + col("nearest_traffic_ahead_aadt"))
traffic_df = traffic_df.withColumn("foot_traffic_proxy", lit(1) / (col("distance_to_station_km") + 1))
traffic_df = traffic_df.withColumn("combined_traffic_accessibility", (col("nearest_traffic_back_aadt") + col("nearest_traffic_ahead_aadt")) / 2)

# Drop unnecessary columns to match dim_traffic schema
traffic_df = traffic_df.select(
    col("property_id"),  # Include property_id for target schema
    col("nearest_station"),
    col("distance_to_station_km"),  # Use kilometers
    col("total_aadt"),
    col("foot_traffic_proxy"),
    col("combined_traffic_accessibility")
)

# Convert DataFrame back to DynamicFrame for Glue compatibility
transformed_traffic_dyf = DynamicFrame.fromDF(traffic_df, glueContext, "transformed_traffic_dyf")

# Write transformed data to AWS RDS PostgreSQL (dim_traffic table)
glueContext.write_dynamic_frame.from_options(
    frame=transformed_traffic_dyf,
    connection_type="postgresql",
    connection_options={
        "url": "jdbc:postgresql://mdbinstanceproject.clmkmooaa6hz.us-east-1.rds.amazonaws.com:5432/capstone-project.db",
        "dbtable": "dim_traffic",
        "user": "postgres",
        "password": "your-password",
        "sslmode": "prefer"
    },
    transformation_ctx="write_dim_traffic"
)

# Commit job
job.commit()
