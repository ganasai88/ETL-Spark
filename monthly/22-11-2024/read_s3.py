import s3fs
import json

def read_from_s3(file_path: str):
    try:
        if file_path.startswith("s3://"):
            print(f"Reading JSON from S3: {file_path}")
            fs = s3fs.S3FileSystem()
            with fs.open(file_path, 'r') as file:
                return json.load(file)
        else:
            print(f"Reading JSON from local file: {file_path}")
            with open(file_path) as file:
                return json.load(file)
    except Exception as e:
        print(f"Error reading JSON from {file_path}: {e}")
        raise  # Reraise the exception for higher-level handling