# Use Python base image with Java (needed for PySpark)
FROM openjdk:11

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    pip3 install --no-cache-dir pyspark

# Copy only the Python scripts (NOT data)
COPY pdb_coordinate_data_extraction.py .
COPY pdb_metadata_extraction.py .

# Set environment variables (relative paths inside the container)
ENV input_path=/app/data/*.pdb
ENV output_path=/app/output

# Run both scripts sequentially
CMD ["bash", "-c", "python3 pdb_coordinate_data_extraction.py && python3 pdb_metadata_extraction.py"]
