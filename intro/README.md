# Generate dataset

This section highlights how to use any one of `pandas`, `polars`, or `duckdb` to generate an artificial dataset of persons, the companies they held work positions in, and their locations. The input dataset is the [7+ million companies dataset](https://www.kaggle.com/datasets/peopledatalabssf/free-7-million-company-dataset) from Kaggle, processed into a parquet file.

* `pandas` is a popular Python DataFrame library for data manipulation and analysis, whose internals are in C++.
* `polars` is a newer DataFrame library for data manipulation and analysis, whose internals are in Rust, and is far more amenable to multi-threading DataFrame ops than `pandas`.
* `duckdb` is a high-performance embedded database that can be queried via a rich SQL dialect. Its core is written in C++, and is designed to be fast, reliable and easy to use, and very amenable to transformation to either `pandas` or `polars`


## Goals


The aim of this section is to show how `pandas`, despite its maturity and importance to the Python data science ecosystem, is **far** slower than either `polars` or `duckdb`. This isn't a criticism of `pandas` -- it's had its time in the sun, and now faces a host of constraints related to backward compatibility that prevent it from modernizing its API.

Both `polars` and `duckdb` (being written in Rust and C++ respectively) have had [ample opportunity to learn from](https://twitter.com/datapythonista/status/1692452584785580111) the pain points of `pandas` and the general PyData ecosystem over the years, allowing them to leverage lessons from modern database theory and the power of the Apache Arrow ecosystem.

## Benchmark

Each person can have multiple positions, held at different companies/locations, and hence, the ratio of persons and positions need not be the same. The example results and timing benchmarks to generate the full dataset are shown in this section.

🚧 WIP
