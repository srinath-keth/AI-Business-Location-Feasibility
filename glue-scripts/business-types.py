import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import Row
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import lower, col

# Initialize Glue context
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define the Glue database and table
db_name = "etl-capstone-project-db"  # Replace with your Glue catalog database name
tbl_name = "capstone_competitors_data_csv"  # Replace with your Glue catalog table name (assuming business types come from competitors)

# Load competitor data from Glue Data Catalog
competitor_data = glueContext.create_dynamic_frame.from_catalog(
    database=db_name,
    table_name=tbl_name
)

# Convert DynamicFrame to DataFrame
df = competitor_data.toDF()

# Step 1: Standardize business type names and extract unique values
# Convert type column to lowercase for consistency
df = df.withColumn("business_type_name", lower(col("type")))

# Define the business type to category mapping (fill in all mappings as needed)
business_type_to_category_mapping = [
    ("department_store", "retail"), ("pharmacy", "retail"), ("electronics_store", "retail"),
    ("grocery_or_supermarket", "retail"), ("health", "services"), ("pet_store", "retail"),
    ("moving_company", "services"), ("store", "retail"), ("home_goods_store", "retail"),
    ("jewelry_store", "retail"), ("liquor_store", "retail"), ("convenience_store", "retail"),
    ("beauty_salon", "services"), ("furniture_store", "retail"), ("hair_care", "services"),
    ("cafe", "restaurant"), ("car_repair", "services"), ("clothing_store", "retail"),
    ("locksmith", "services"), ("painter", "services"), ("meal_takeaway", "restaurant"),
    ("restaurant", "restaurant"), ("meal_delivery", "restaurant"), ("movie_theater", "mixed use"),
    ("bakery", "restaurant"), ("bar", "restaurant"), ("florist", "retail"),
    ("general_contractor", "services"), ("gas_station", "services"), ("drugstore", "retail"),
    ("food", "restaurant"), ("movie_rental", "mixed use"), ("night_club", "mixed use"),
    ("gym", "services"), ("supermarket", "retail"), ("veterinary_care", "services"),
    ("storage", "services"), ("shoe_store", "retail"), ("point_of_interest", "mixed use"),
    ("art_gallery", "mixed use"), ("car_dealer", "retail"), ("atm", "services"),
    ("finance", "office"), ("hardware_store", "retail"), ("bowling_alley", "mixed use"),
    ("secondary_school", "services"), ("real_estate_agency", "office"), ("bank", "office"),
    ("rv_park", "services"), ("courthouse", "office"), ("roofing_contractor", "services"),
    ("dentist", "services"), ("car_wash", "services"), ("spa", "services"),
    ("parking", "services"), ("book_store", "retail"), ("bicycle_store", "retail"),
    ("lodging", "services"), ("tourist_attraction", "mixed use"), ("doctor", "services"),
    ("school", "services"), ("physiotherapist", "services"), ("electrician", "services"),
    ("primary_school", "services"), ("lawyer", "office"), ("shopping_mall", "mixed use"),
    ("insurance_agency", "office"), ("post_office", "office"), ("laundry", "services"),
    ("church", "mixed use"), ("museum", "mixed use"), ("plumber", "services"),
    ("park", "mixed use"), ("car_rental", "services"), ("amusement_park", "mixed use"),
    ("travel_agency", "office"), ("local_government_office", "office"), ("university", "services"),
    ("accounting", "office"), ("library", "mixed use"), ("police", "services"),
    ("aquarium", "mixed use"), ("casino", "mixed use"), ("synagogue", "mixed use"),
    ("cemetery", "mixed use"), ("zoo", "mixed use"), ("fire_station", "services"),
    ("subway_station", "services"), ("mosque", "mixed use"), ("hospital", "services"),
    ("funeral_home", "services"), ("stadium", "mixed use"), ("transit_station", "services"),
    ("train_station", "services"), ("airport", "services"), ("place_of_worship", "mixed use"),
    ("city_hall", "office")
]

# Convert the mapping into a DataFrame
mapping_df = spark.createDataFrame(business_type_to_category_mapping, ["business_type_name", "category_name"])

# Step 2: Join competitor data with the mapping to get category names
# This join adds the `category_name` based on `business_type_name`
business_types_with_category = df.join(mapping_df, "business_type_name", "left").select("business_type_name", "category_name").distinct()

# Step 3: Load dim_categories table to get `category_id` for each `category_name`
categories_with_id = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://mdbinstanceproject.clmkmooaa6hz.us-east-1.rds.amazonaws.com:5432/capstone-project.db") \
    .option("dbtable", "categories") \
    .option("user", "postgres") \
    .option("password", "your-password") \
    .option("sslmode", "prefer") \
    .load()

# Join business types with categories to get category_id for each business type
business_types_with_id = business_types_with_category.join(categories_with_id, "category_name", "left").select("business_type_name", "category_id").distinct()

# Convert to DynamicFrame for Glue
business_types_dynamic_frame = DynamicFrame.fromDF(business_types_with_id, glueContext, "business_types_dynamic_frame")

# Step 4: Write business types with category IDs to `dim_business_types`
# This write operation will ensure each business type is unique in `dim_business_types`
glueContext.write_dynamic_frame.from_options(
    frame=business_types_dynamic_frame,
    connection_type="postgresql",
    connection_options={
        "url": "jdbc:postgresql://mdbinstanceproject.clmkmooaa6hz.us-east-1.rds.amazonaws.com:5432/capstone-project.db",
        "dbtable": "business_types",
        "user": "postgres",
        "password": "123456789",
        "sslmode": "prefer"
    },
    transformation_ctx="write_business_types"
)

# Commit the job
job.commit()
