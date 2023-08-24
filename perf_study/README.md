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

The benchmarks are run using the `pytest-benchmark` library which itself depends on `pytest`, and the results are shown below. The full benchmark code is available in `./perf_study/benchmark.py`.

### Conditions

* Raw dataset being generated contains 1M persons and 10M positions at companies that these 1M persons have held
* Macbook Pro M2, 16 GB RAM
* Average of 3 runs (for each of `pandas`, `polars` and `duckdb`)
* Garbage collector timing disabled during benchmark

### Results

```sh
$ pytest benchmark.py --benchmark-min-rounds=3 --benchmark-disable-gc

==================================================================================================== test session starts ====================================================================================================
platform darwin -- Python 3.11.2, pytest-7.4.0, pluggy-1.2.0
benchmark: 4.0.0 (defaults: timer=time.perf_counter disable_gc=True min_rounds=3 min_time=0.000005 max_time=1.0 calibration_precision=10 warmup=False warmup_iterations=100000)
rootdir: /code/embedded-dbs/perf_study
plugins: benchmark-4.0.0
collected 3 items                                                                                                                                                                                                           

benchmark.py ...                                                                                                                                                                                                      [100%]


---------------------------------------------------------------------------------- benchmark: 3 tests ----------------------------------------------------------------------------------
Name (time in s)              Min                Max               Mean            StdDev             Median               IQR            Outliers     OPS            Rounds  Iterations
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
test_benchmark_duckdb      3.6492 (1.0)       4.0477 (1.0)       3.8405 (1.0)      0.1998 (1.0)       3.8245 (1.0)      0.2989 (1.0)           1;0  0.2604 (1.0)           3           1
test_benchmark_polars      5.4167 (1.48)      6.4685 (1.60)      5.7704 (1.50)     0.6046 (3.03)      5.4260 (1.42)     0.7888 (2.64)          1;0  0.1733 (0.67)          3           1
test_benchmark_pandas     19.3788 (5.31)     19.8074 (4.89)     19.5716 (5.10)     0.2175 (1.09)     19.5287 (5.11)     0.3215 (1.08)          1;0  0.0511 (0.20)          3           1
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Legend:
  Outliers: 1 Standard Deviation from Mean; 1.5 IQR (InterQuartile Range) from 1st Quartile and 3rd Quartile.
  OPS: Operations Per Second, computed as 1 / Mean
=============================================================================================== 3 passed in 144.33s (0:02:24) ===============================================================================================
```

The results are ordered by average run time. As can be seen, `duckdb` is the fastest, followed by `polars`, and then `pandas`. The difference between `polars` and `pandas` is quite stark, with `polars` being 3.5x faster than `pandas`, and `duckdb` being ~1.5x faster than `polars`.
