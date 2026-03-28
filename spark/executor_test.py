#!/usr/bin/env python3
import sys
import os

print("=" * 50)
print("Executor Test Script")
print("=" * 50)
print(f"Python version: {sys.version}")
print(f"PYTHONPATH: {os.environ.get('PYTHONPATH', 'Not set')}")
print(f"PYSPARK_PYTHON: {os.environ.get('PYSPARK_PYTHON', 'Not set')}")

try:
    import pyspark
    print(f"PySpark version: {pyspark.__version__}")
except ImportError as e:
    print(f"ERROR: Cannot import pyspark: {e}")
    sys.exit(1)

print("SUCCESS: PySpark is available!")
sys.exit(0)
