import sys
import json
import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

# Initialize Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

class EligibilityPipeline:
    def __init__(self, bucket_name, config_key):
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=bucket_name, Key=config_key)
        self.configs = json.loads(response['Body'].read().decode('utf-8'))

    def _apply_transformations(self, df, partner_cfg):
        """Logic for formatting names, dates, and phones."""
        clean_phone = F.regexp_replace(F.col("phone"), r'\D', '')
        formatted_phone = F.concat_ws('-', 
            F.substring(clean_phone, 1, 3), 
            F.substring(clean_phone, 4, 3), 
            F.substring(clean_phone, 7, 4)
        )

        return df.select(
            F.trim(F.col("external_id")).alias("external_id"),
            F.initcap(F.trim(F.col("first_name"))).alias("first_name"),
            F.initcap(F.trim(F.col("last_name"))).alias("last_name"),
            F.to_date(F.trim(F.col("dob")), partner_cfg['date_format']).alias("dob"),
            F.lower(F.trim(F.col("email"))).alias("email"),
            formatted_phone.alias("phone"),
            F.lit(partner_cfg['partner_code']).alias("partner_code")
        )

    def process_partner(self, partner_name):
        cfg = self.configs[partner_name]
        
        # 1. READ WITH TRIM OPTIONS: This handles the " MBI " vs "MBI" issue
        raw_df = spark.read \
            .option("header", "true") \
            .option("sep", cfg['delimiter']) \
            .option("ignoreLeadingWhiteSpace", "true") \
            .option("ignoreTrailingWhiteSpace", "true") \
            .csv(cfg['file_path'])

        # 2. RENAME WITH STRIP: We strip the keys from the config just in case
        for partner_col, std_col in cfg['mappings'].items():
            # .strip() handles any accidental spaces in the JSON or the file headers
            raw_df = raw_df.withColumnRenamed(partner_col.strip(), std_col.strip())

        # 3. TRANSFORM
        # Now 'external_id' will definitely exist
        return self._apply_transformations(raw_df, cfg)

    def run(self):
        all_dfs = [self.process_partner(p) for p in self.configs.keys()]
        
        final_df = all_dfs[0]
        for next_df in all_dfs[1:]:
            final_df = final_df.unionByName(next_df)
        
        # Bonus Requirement: Filter out rows with missing IDs
        return final_df.filter(F.col("external_id").isNotNull())

if __name__ == "__main__":
    BUCKET = "pyspark-healthcare-pipeline-amogh" 
    CONFIG_PATH = "config.json"
    
    pipeline = EligibilityPipeline(BUCKET, CONFIG_PATH)
    final_output = pipeline.run()
    
    # Print results to logs
    final_output.show(truncate=False)
    
    # Save to S3
    final_output.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"s3://{BUCKET}/output_standardized/")
    
    job.commit()
