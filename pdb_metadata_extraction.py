import os
import logging
import json
import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, input_file_name, first, when, trim

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def extract_metadata():
    """
    Extract metadata from PDB files (Resolution, Experiment Type, R-Free, R-Factor, Date, Organism).
    """
    # Initialize Spark Session
    spark = SparkSession.builder.appName("PDB_Metadata_Extract").getOrCreate()

    # Use glob to find all PDB files
    pdb_files = glob.glob("data/*.pdb")

    if not pdb_files:
        logging.error("No PDB files found in data/ directory.")
        exit(1)  # Exit script if no files found

    input_path = pdb_files  # Provide actual list of files
    output_path = "output/metadata_extract"
    os.makedirs(output_path, exist_ok=True)

    logging.info(f"Reading {len(pdb_files)} PDB files from: data/")

    # Read PDB Files
    df = spark.read.text(input_path)

    # Extract PDB ID from filename
    df = df.withColumn("PDB_ID", input_file_name())
    df = df.withColumn("PDB_ID", col("PDB_ID").substr(-8, 4))  # Extract PDB ID

    # Filter metadata lines (HEADER, REMARK 2, REMARK 3, EXPDTA, SOURCE)
    metadata_df = df.filter(df.value.startswith("HEADER") |
                            df.value.startswith("REMARK   2 RESOLUTION.") |
                            df.value.startswith("REMARK   3") |
                            df.value.startswith("EXPDTA") |
                            df.value.startswith("SOURCE"))

    # Extract Resolution (REMARK 2)
    metadata_df = metadata_df.withColumn("Resolution",
        when(df.value.startswith("REMARK   2 RESOLUTION."), trim(col("value").substr(27, 4)))  # Extract from column 24-30
    )

    # Extract Experiment Type (EXPDTA)
    metadata_df = metadata_df.withColumn("Experiment_Type",
        when(df.value.startswith("EXPDTA"), trim(col("value").substr(11, 50)))  # Extract after "EXPDTA"
    )

    # Extract Deposition Date (HEADER)
    metadata_df = metadata_df.withColumn("Date",
        when(df.value.startswith("HEADER"), trim(col("value").substr(51, 9)))  # Extract from column 51-59
    )

    # Extract Organism (SOURCE)
    metadata_df = metadata_df.withColumn("Organism",
        when(df.value.contains("ORGANISM_SCIENTIFIC"), trim(col("value").substr(33, 50)))  # Extract from column 36-86
    )

    # Group by PDB_ID and collect first non-null values
    grouped_df = metadata_df.groupBy("PDB_ID").agg(
        first("Resolution", ignorenulls=True).alias("Resolution"),
        first("Experiment_Type", ignorenulls=True).alias("Experiment_Type"),
        first("Date", ignorenulls=True).alias("Deposition_Date"),
        first("Organism", ignorenulls=True).alias("Organism")
    )

    # Convert to JSON format
    metadata_list = grouped_df.toJSON().collect()
    metadata_json = [json.loads(entry) for entry in metadata_list]

    # Save metadata as a JSON file
    output_json_path = os.path.join(output_path, "pdb_metadata.json")
    with open(output_json_path, "w") as json_file:
        json.dump(metadata_json, json_file, indent=4)

    logging.info(f"Metadata saved in {output_json_path}")

if __name__ == "__main__":
    extract_metadata()
