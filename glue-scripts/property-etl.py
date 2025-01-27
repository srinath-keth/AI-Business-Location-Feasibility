import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import regexp_replace, col, when, round, abs, lower
from awsglue.dynamicframe import DynamicFrame

# Initialize Glue context and job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define data sources
db_name = "etl-capstone-project-db"  # Glue catalog database name
property_tbl_name = "capstone_property_data_csv"  # Property table name in Glue catalog

# PostgreSQL connection details
postgres_url = "jdbc:postgresql://mdbinstanceproject.clmkmooaa6hz.us-east-1.rds.amazonaws.com:5432/capstone-project.db"
postgres_properties = {
    "user": "postgres",
    "password": "your-password",
    "sslmode": "prefer"
}

# Load property data from Glue Data Catalog
property_data = glueContext.create_dynamic_frame.from_catalog(
    database=db_name,
    table_name=property_tbl_name
)

# Convert DynamicFrame to DataFrame for transformations
property_df = property_data.toDF()

# Transformations on property data
# 1. Clean price field (remove $ and commas), set 'Unpriced' to None, convert to float
property_df = property_df.withColumn('price', regexp_replace(col('price'), '[$,]', ''))
property_df = property_df.withColumn('price', when(col('price') == 'Unpriced', None).otherwise(col('price').cast('float')))

# 2. Ensure square_foot is positive by using abs() function
property_df = property_df.withColumn('square_foot', abs(col('square_foot')).cast('int'))

# 3. Convert latitude and longitude to float
property_df = property_df.withColumn('latitude', col('latitude').cast('float'))
property_df = property_df.withColumn('longitude', col('longitude').cast('float'))

# 4. Standardize the category column to lowercase
property_df = property_df.withColumn('category', lower(col('category')))

# Load category data from PostgreSQL
category_df = spark.read \
    .format("jdbc") \
    .option("url", postgres_url) \
    .option("dbtable", "categories") \
    .option("user", postgres_properties["user"]) \
    .option("password", postgres_properties["password"]) \
    .option("sslmode", postgres_properties["sslmode"]) \
    .load()

# Standardize the category_name column to lowercase
category_df = category_df.withColumn('category_name', lower(col('category_name')))

# Join property data with category data on category name
property_df = property_df.join(
    category_df,
    property_df["category"] == category_df["category_name"],
    "left_outer"
)

# Replace category names with category_id
property_df = property_df.drop('category', 'category_name').withColumnRenamed('category_id', 'category_id')

# 5. Categorize properties based on size into size_category
property_df = property_df.withColumn('size_category',
                                     when(col('square_foot') > 5000, 'Large')
                                     .when((col('square_foot') <= 5000) & (col('square_foot') > 3000), 'Medium')
                                     .otherwise('Small'))

# 6. Calculate price per square foot for non-null price and valid square_foot
property_df = property_df.withColumn('price_per_sqft',
                                     when((col('price').isNotNull()) & (col('square_foot') > 0),
                                          round(col('price') / col('square_foot'), 2))
                                     .otherwise(None))

# 7. Fill missing values where needed with default values
property_df = property_df.fillna({'price': 0, 'square_foot': 0, 'latitude': 0, 'longitude': 0})

# Add census_track from demographics CSV to property data
demographics_tbl_path = "s3://capstone-project-etl/raw-data-initial-data/demographic_data.csv"

demographic_data = glueContext.create_dynamic_frame.from_options(
    format="csv",
    connection_type="s3",
    connection_options={"paths": [demographics_tbl_path], "recurse": True},
    format_options={"withHeader": True}
)

# Convert demographic data to DataFrame
demographics_df = demographic_data.toDF()

# Select necessary columns from demographics CSV
demographics_df = demographics_df.select(
    col("Property_id").alias("demographic_property_id"),
    col("Census Tract").alias("census_track")
)

# Add census_track to the properties table
property_df = property_df.join(
    demographics_df,
    property_df["property_id"] == demographics_df["demographic_property_id"],
    "left_outer"
).drop("demographic_property_id")  # Add census_track to properties

# Load dim_demographics from PostgreSQL to map demographic_id
dim_demographics_df = spark.read \
    .format("jdbc") \
    .option("url", postgres_url) \
    .option("dbtable", "dim_demographics") \
    .option("user", postgres_properties["user"]) \
    .option("password", postgres_properties["password"]) \
    .option("sslmode", postgres_properties["sslmode"]) \
    .load()

# Map demographic_id to the properties table using census_track
property_df = property_df.join(
    dim_demographics_df,
    property_df["census_track"] == dim_demographics_df["census_track"],
    "left_outer"
).drop(dim_demographics_df["census_track"])  # Add demographic_id to properties

# Select only the columns needed for dim_properties table
final_df = property_df.select(
    col('property_id'),  # Include property_id as the primary key
    col('price'),
    col('category_id'),  # Ensuring category_id is the foreign key
    col('square_foot'),
    col('price_per_sqft'),
    col('address'),
    col('city'),
    col('state'),
    col('zip code').alias('zip_code'),
    col('property_link'),
    col('latitude'),
    col('longitude'),
    col('size_category'),
    col('demographic_id'),  # Include demographic_id as a foreign key
    col('census_track').cast("bigint")  # Include census_track and cast to BIGINT
)

# Convert DataFrame back to DynamicFrame for Glue to handle
transformed_property_data = DynamicFrame.fromDF(final_df, glueContext, "transformed_property_data")

# Write the transformed data to AWS RDS PostgreSQL (dim_properties table)
glueContext.write_dynamic_frame.from_options(
    frame=transformed_property_data,
    connection_type="postgresql",
    connection_options={
        "url": postgres_url,
        "dbtable": "dim_properties",
        "user": postgres_properties["user"],
        "password": postgres_properties["password"],
        "sslmode": postgres_properties["sslmode"]
    },
    transformation_ctx="write_dim_properties"
)

# Commit the job
job.commit()
