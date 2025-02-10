import os
import shutil
import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, input_file_name, substring, when

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def extract_pdb_data():
    """
    Extract atomic data from PDB files using PySpark and save as individual CSV files.
    """
    # Initialize Spark Session
    spark = SparkSession.builder.appName("PDB_Coord_Extract").getOrCreate()

    input_path = r"data/*.pdb"
    output_path = r"output/PDB_Coord_Extract"
    os.makedirs(output_path, exist_ok=True)

    logging.info(f"Reading PDB files from: {input_path}")

    # Read PDB Files
    df = spark.read.text(input_path)

    # Filter Only ATOM and HETATM Records
    atom_df = df.filter(df.value.startswith("ATOM") | df.value.startswith("HETATM"))

    # Extract PDB ID from Filename
    atom_df = atom_df.withColumn("PDB_ID", input_file_name())
    atom_df = atom_df.withColumn("PDB_ID", col("PDB_ID").substr(-8, 4))  # Extract PDB ID

    # Extract Atomic Data
    atom_df = atom_df.withColumn("Record_Type", col("value").substr(1, 6)) \
        .withColumn("Atom_ID", col("value").substr(7, 5).cast("int")) \
        .withColumn("Atom_Type", col("value").substr(14, 3)) \
        .withColumn("Alt_Confmn", col("value").substr(17, 1)) \
        .withColumn("Alt_Confmn", when(col("Alt_Confmn") == " ", None).otherwise(col("Alt_Confmn"))) \
        .withColumn("Residue", col("value").substr(18, 3)) \
        .withColumn("Chain", col("value").substr(22, 1)) \
        .withColumn("Residue_ID", col("value").substr(23, 4).cast("int")) \
        .withColumn("X_Coord", col("value").substr(31, 8).cast("float")) \
        .withColumn("Y_Coord", col("value").substr(39, 8).cast("float")) \
        .withColumn("Z_Coord", col("value").substr(47, 8).cast("float")) \
        .drop("value")

    # Ensure Output Directory Exists
    os.makedirs(output_path, exist_ok=True)

    # Define Temporary Output Path
    temp_output = os.path.join(output_path, "temp_pdb_output")

    logging.info(f"Writing PDB atom data to temporary partitioned directory: {temp_output}")

    # Write CSVs using Partitioning (Parallel Processing)
    atom_df.repartition("PDB_ID").write.partitionBy("PDB_ID").csv(temp_output, header=True, mode="overwrite")

    # Move & Rename Files to Avoid Subdirectories
    for pdb_folder in os.listdir(temp_output):
        pdb_path = os.path.join(temp_output, pdb_folder)

        if os.path.isdir(pdb_path):  # Process folders (PDB_ID partitions)
            part_files = [f for f in os.listdir(pdb_path) if f.startswith("part-") and f.endswith(".csv")]
            if part_files:
                temp_csv_file = os.path.join(pdb_path, part_files[0])  # Get first part file
                final_output = os.path.join(output_path, f"{pdb_folder[-4:]}.csv")  # Rename to pdb_id.csv

                shutil.move(temp_csv_file, final_output)
                logging.info(f"Saved {final_output}")

    # Remove Temporary Partitioned Output
    shutil.rmtree(temp_output, ignore_errors=True)
    logging.info("All PDB atom data has been saved successfully!")


if __name__ == "__main__":
    extract_pdb_data()
