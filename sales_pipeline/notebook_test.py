# Databricks notebook source
!pip install -e .

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import pytest
import sys
import os

current_dir = os.getcwd()
if current_dir not in sys.path:
    sys.path.append(current_dir)
os.environ["PYTHONPATH"] = current_dir

sys.dont_write_bytecode = True

args = [
    "tests/",
    "-v",
    "-p", "no:cacheprovider",
    "-p", "no:warnings"
]

retcode = pytest.main(args)

if retcode != 0:
    raise Exception(f"pytest failed with exit code {retcode}")
else:
    print("All tests passed")
