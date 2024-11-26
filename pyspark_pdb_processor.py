from pyspark.sql import SparkSession
from pyspark.sql import Row
import os

def parse_pdb_line(line):
    """
    Parses a single line from a PDB file to extract relevant fields.
    
    Args:
        line (str): A line from the PDB file.
        
    Returns:
        Row: A PySpark Row object containing extracted fields.
    """
    return Row(
        record_type=line[0:6].strip(),
        atom_id=int(line[6:11].strip()),
        atom_name=line[12:16].strip(),
        residue=line[17:20].strip(),
        chain_id=line[21:22].strip(),
        residue_seq=int(line[22:26].strip()),
        x=float(line[30:38].strip()),
        y=float(line[38:46].strip()),
        z=float(line[46:54].strip())
    )


def process_pdb_file(spark, file_path, output_dir):
    """
    Process a single PDB file and save its ATOM and HETATM data to a CSV file.
    
    Args:
        spark (SparkSession): The active SparkSession.
        file_path (str): Path to the PDB file.
        output_dir (str): Directory where the output CSV file will be saved.
    """
    file_name = os.path.basename(file_path).replace(".pdb", ".txt")
    output_file_path = os.path.join(output_dir, file_name)

    # Read the PDB file into an RDD
    rdd = spark.sparkContext.textFile(file_path)

    # Filter and parse lines starting with "ATOM" or "HETATM"
    parsed_rdd = (
        rdd.filter(lambda line: line.startswith("ATOM") or line.startswith("HETATM"))
           .map(parse_pdb_line)
    )

    # Convert RDD to DataFrame
    df = spark.createDataFrame(parsed_rdd)

    # Write DataFrame to CSV
    df.write.csv(output_file_path, header=True, mode="overwrite")
    print(f"Processed: {file_path} -> {output_file_path}")


def process_pdb_directory(spark, input_dir, output_dir):
    """
    Process all PDB files in a directory using PySpark.
    
    Args:
        spark (SparkSession): The active SparkSession.
        input_dir (str): Directory containing PDB files.
        output_dir (str): Directory to save the CSV files.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    pdb_files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith(".pdb")]

    for pdb_file in pdb_files:
        process_pdb_file(spark, pdb_file, output_dir)


if __name__ == "__main__":
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("PDB Processing with PySpark") \
        .master("local[*]") \
        .getOrCreate()

    # Define input and output directories
    input_directory = "./example_pdbs"  # Replace with the path to your PDB files
    output_directory = "./output"

    # Process all PDB files in the directory
    process_pdb_directory(spark, input_directory, output_directory)

    # Stop SparkSession
    spark.stop()