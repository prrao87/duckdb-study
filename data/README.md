# Data

The data we will be working with in this repo is [this dataset of 7+ million companies](https://www.kaggle.com/datasets/peopledatalabssf/free-7-million-company-dataset) from Kaggle. The data is in CSV format, and is quite large when unzipped (1.09 GB). As a result, it's not stored here -- but it's easy to sign up for a Kaggle account and download it to your local machine.

To demonstrate the power of embedded databases, the data is converted to parquet format via `polars` as shown below.

1. [Download the dataset](https://www.kaggle.com/datasets/peopledatalabssf/free-7-million-company-dataset/download?datasetVersionNumber=1) from Kaggle and unzip the CSV file
2. Make sure to run `pip install polars` prior to running the Python code snippet below.

```py
import polars as pl

df = pl.read_csv("companies_sorted.csv")
df.write_parquet("companies_sorted.parquet")
```

This saves the data to a parquet file, whose size is roughly the same as the zipped data downloaded (~25% of the raw CSV).
