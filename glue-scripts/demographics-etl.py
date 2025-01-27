import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, IntegerType
from awsglue.dynamicframe import DynamicFrame

# Initialize Glue context and job
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Load demographics data from Glue Data Catalog
demographics_df = glueContext.create_dynamic_frame.from_catalog(
    database="etl-capstone-project-db",
    table_name="demographic_data_csv"
).toDF()

# Standardize column names in demographics_df
demographics_df = demographics_df.withColumnRenamed("Census Tract", "census_track") \
    .withColumnRenamed("Total Population", "total_population") \
    .withColumnRenamed("Median Household Income", "median_household_income") \
    .withColumnRenamed("White alone", "white_alone") \
    .withColumnRenamed("Black or African American alone", "black_or_african_american_alone") \
    .withColumnRenamed("American Indian and Alaska Native alone", "american_indian_and_alaska_native_alone") \
    .withColumnRenamed("Asian alone", "asian_alone") \
    .withColumnRenamed("Native Hawaiian and Other Pacific Islander alone", "native_hawaiian_and_other_pacific_islander_alone") \
    .withColumnRenamed("Some other race alone", "some_other_race_alone") \
    .withColumnRenamed("Two or more races", "two_or_more_races") \
    .withColumnRenamed("Age 0-17", "age_0_17") \
    .withColumnRenamed("Age 18-34", "age_18_34") \
    .withColumnRenamed("Age 35-64", "age_35_64") \
    .withColumnRenamed("Age 65+", "age_65_plus")

# Load area data directly from S3
area_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://capstone-project-etl/raw-data-initial-data/census_tract_area.csv"]},
    format="csv",
    format_options={"withHeader": True}
).toDF()

# Standardize column names in area_df
area_df = area_df.withColumnRenamed("GEOID", "census_track") \
    .withColumnRenamed("Area (sq miles)", "area_sq_miles")

# Merge demographic data with area data on 'census_track'
merged_df = demographics_df.join(area_df, on="census_track", how="left")

# Step 1: Scale the area and calculate Population Density
scaling_factor = 1e15  # Ensure the area doesn't lose precision
merged_df = merged_df.withColumn('scaled_area_sq_miles', F.col('area_sq_miles') * F.lit(scaling_factor))
merged_df = merged_df.withColumn('population_density', 
    (F.col('total_population') / F.col('scaled_area_sq_miles') * F.lit(1000)).cast(DoubleType()))

# Step 2: Map Race/Ethnicity Fields
merged_df = merged_df.withColumn("american", F.col("white_alone").cast(IntegerType())) \
    .withColumn("african", F.col("black_or_african_american_alone").cast(IntegerType())) \
    .withColumn("asian", (F.col("asian_alone") + F.col("native_hawaiian_and_other_pacific_islander_alone")).cast(IntegerType())) \
    .withColumn("indian", F.col("american_indian_and_alaska_native_alone").cast(IntegerType())) \
    .withColumn("pacific_islander", F.col("native_hawaiian_and_other_pacific_islander_alone").cast(IntegerType())) \
    .withColumn("other_race", F.col("some_other_race_alone").cast(IntegerType())) \
    .withColumn("mixed_race", F.col("two_or_more_races").cast(IntegerType()))

# Step 3: Map Age Group Fields
merged_df = merged_df.withColumn("teen", F.col("age_0_17").cast(IntegerType())) \
    .withColumn("young_adult", F.col("age_18_34").cast(IntegerType())) \
    .withColumn("adult", F.col("age_35_64").cast(IntegerType())) \
    .withColumn("senior", F.col("age_65_plus").cast(IntegerType()))

# Step 4: Income-Adjusted Spending Calculation
income_mean = merged_df.select(F.mean('median_household_income').alias('mean_income')).collect()[0]['mean_income']

spending_config = {
    'total_spending_adjusted': 64835,
    'housing_adjusted': 12188,
    'healthcare_adjusted': 10373,
    'food_and_beverages_adjusted': 4708,
    'gas_and_energy_adjusted': 1320,
    'other_spending_adjusted': 36245
}

for col_name, per_capita_spending in spending_config.items():
    merged_df = merged_df.withColumn(col_name, 
        (F.col('median_household_income') / F.lit(income_mean) * F.lit(per_capita_spending) * F.col('total_population')).cast(DoubleType()))

# Step 5: Handle Missing Values for Remaining Numeric Columns
numeric_cols = [col for col, dtype in merged_df.dtypes if dtype in ['double', 'int']]
for col in numeric_cols:
    median_value = merged_df.select(F.percentile_approx(col, 0.5).alias('median')).collect()[0]['median']
    merged_df = merged_df.withColumn(col, F.coalesce(F.col(col), F.lit(median_value)))

# Step 6: Select Final Columns
final_columns = [
    'census_track', 'total_population', 'median_household_income', 'population_density',
    'american', 'african', 'asian', 'indian', 'pacific_islander', 'other_race', 'mixed_race',
    'teen', 'young_adult', 'adult', 'senior',
    'total_spending_adjusted', 'housing_adjusted', 'healthcare_adjusted',
    'food_and_beverages_adjusted', 'gas_and_energy_adjusted', 'other_spending_adjusted'
]
final_df = merged_df.select(*final_columns).dropDuplicates(['census_track'])

# Convert to DynamicFrame and write to PostgreSQL
final_dynamic_df = DynamicFrame.fromDF(final_df, glueContext, "final_dynamic_df")

glueContext.write_dynamic_frame.from_options(
    frame=final_dynamic_df,
    connection_type="postgresql",
    connection_options={
        "url": "jdbc:postgresql://mdbinstanceproject.clmkmooaa6hz.us-east-1.rds.amazonaws.com:5432/capstone-project.db",
        "dbtable": "dim_demographics",
        "user": "postgres",
        "password": "your-password",
        "sslmode": "prefer",
        "connectTimeout": "10"
    },
    transformation_ctx="write_dim_demographics"
)

# Commit job
job.commit()
