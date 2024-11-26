**PDB Processor with PySpark**

This Python project uses PySpark to process PDB (Protein Data Bank) files. It extracts ATOM and HETATM records, converts the data into tabular format, and saves the processed results as CSV files. The script is designed for scalable and efficient handling of large datasets.

Features
Efficient processing of large PDB datasets using PySpark's distributed computing capabilities.
Extracts fields such as:
record_type (e.g., ATOM, HETATM)
atom_id, atom_name, residue
chain_id, residue_seq, and 3D coordinates (x, y, z)
Generates one CSV file per PDB file with a structured format.
Easily extendable for additional analysis and processing tasks.
Requirements
Python 3.8+
Libraries:
PySpark (pip install pyspark)
Pandas (pip install pandas)
Java 8+ installed and properly configured (required for PySpark).

**Setup**
**1. Clone the Repository**
git clone https://github.com/<your-username>/PDB_Processor_PySpark.git
cd PDB_Processor_PySpark

**2. Place PDB Files**
Add your PDB files to the example_pdbs/ directory.

**3. Run the Script**
python pyspark_pdb_processor.py
The script will process all PDB files in the example_pdbs/ directory and save the output CSV files in the output/ directory.
