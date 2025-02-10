# PDB Processor with PySpark & Docker   

[![PySpark](https://img.shields.io/badge/PySpark-3.3.0-orange)](https://spark.apache.org/docs/latest/api/python/getting_started/index.html)
[![Docker](https://img.shields.io/badge/Docker-✔️-blue)](https://www.docker.com/)
[![Python](https://img.shields.io/badge/Python-3.9%2B-brightgreen)](https://www.python.org/)

**This program is designed to efficiently process large-scale PDB datasets on a local cluster using Apache PySpark. Given that the Protein Data Bank (PDB) dataset spans approximately 4–5 TB, traditional sequential processing methods are computationally expensive and inefficient. By leveraging PySpark’s distributed computing capabilities, this solution enables fast and scalable data processing across multiple cores or nodes.**

**The PDB dataset is systematically partitioned into two distinct data categories:**

**Atomic Coordinate Data (CSV format):**

**Contains detailed 3D atomic coordinates of biomolecular structures.
Primarily used for structural analysis, computational modeling, and machine learning applications in structural bioinformatics.
Experimental Metadata (JSON format):**

**Includes key information such as resolution, R-free, R-factor, deposition date, and experimental method.
Designed for efficient querying, facilitating large-scale statistical analysis, metadata filtering, and integration with external biological databases.**

**By preprocessing and structuring PDB data in this format, the system enhances data accessibility, supports downstream ML model training, and enables rapid querying of experimental conditions. This architecture is particularly suited for high-throughput structural bioinformatics pipelines and large-scale machine learning applications in computational biology and cheminformatics.**

---

## Installation

Follow these steps to set up and run the project.

### **1. Clone the Repository**
```bash
git clone git@github.com:SanketSatishShelke/PDB_Processor_PySpark.git
cd PDB_Processor_PySpark
```

### **2. Install Dependencies**
```bash
pip install -r requirements.txt
```

---

## Usage

### 1. Run without Docker
```bash
python pdb_coordinate_data_extraction.py
python pdb_metadata_extraction.py
```

### 2. Run With Docker
```bash
docker build -t pdb_extract
docker run -v $(pwd)/data:/app/data -v $(pwd)/output:/app/output pdb_extract
```
