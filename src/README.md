# Generate dataset

In this section, we show how to use either Polars or DuckDB to generate an artificial dataset of persons, the companies they held work positions in, and their locations.

## Summary

DuckDB is ~26% faster than Polars for generating a dataset of 1M persons 10M positions at companies. Each person can have multiple positions, held at different companies/locations, and hence, the ratio of persons and positions need not be the same. The example results and timing are shown below (both methods return identical DataFrames).

### DuckDB

```sh
$ time python generate_data_polars.py --num_persons 1000000 --num_positions 10000000

Obtained persons, companies and locations DataFrame of shape: (9999986, 5)
shape: (10, 5)
┌───────────┬────────────┬───────────────────────────────────┬────────────────┬─────┐
│ person_id ┆ company_id ┆ locality                          ┆ country        ┆ age │
│ ---       ┆ ---        ┆ ---                               ┆ ---            ┆ --- │
│ i64       ┆ i64        ┆ str                               ┆ str            ┆ i64 │
╞═══════════╪════════════╪═══════════════════════════════════╪════════════════╪═════╡
│ 1         ┆ 342506     ┆ toronto, ontario, canada          ┆ canada         ┆ 40  │
│ 1         ┆ 3035395    ┆ dallas, texas, united states      ┆ united states  ┆ 40  │
│ 1         ┆ 4251382    ┆ ilford, redbridge, united kingdo… ┆ united kingdom ┆ 40  │
│ 1         ┆ 4982842    ┆ madrid, madrid, spain             ┆ spain          ┆ 40  │
│ …         ┆ …          ┆ …                                 ┆ …              ┆ …   │
│ 1         ┆ 6512043    ┆ altrincham, trafford, united kin… ┆ united kingdom ┆ 40  │
│ 1         ┆ 7134420    ┆ new lenox, illinois, united stat… ┆ united states  ┆ 40  │
│ 2         ┆ 1375651    ┆ hove, brighton and hove, united … ┆ united kingdom ┆ 37  │
│ 2         ┆ 3017119    ┆ weymouth, dorset, united kingdom  ┆ united kingdom ┆ 37  │
└───────────┴────────────┴───────────────────────────────────┴────────────────┴─────┘
python generate_data_duckdb.py --num_persons 1000000 --num_positions 10000000  17.63s user 3.22s system 341% cpu 6.105 total
```

### Polars

```sh
$ time python generate_data_polars.py --num_persons 1000000 --num_positions 10000000

Obtained persons, companies and locations DataFrame of shape: (9999986, 5)
shape: (10, 5)
┌───────────┬────────────┬───────────────────────────────────┬────────────────┬─────┐
│ person_id ┆ company_id ┆ locality                          ┆ country        ┆ age │
│ ---       ┆ ---        ┆ ---                               ┆ ---            ┆ --- │
│ i64       ┆ i64        ┆ str                               ┆ str            ┆ i64 │
╞═══════════╪════════════╪═══════════════════════════════════╪════════════════╪═════╡
│ 1         ┆ 342506     ┆ toronto, ontario, canada          ┆ canada         ┆ 40  │
│ 1         ┆ 3035395    ┆ dallas, texas, united states      ┆ united states  ┆ 40  │
│ 1         ┆ 4251382    ┆ ilford, redbridge, united kingdo… ┆ united kingdom ┆ 40  │
│ 1         ┆ 4982842    ┆ madrid, madrid, spain             ┆ spain          ┆ 40  │
│ …         ┆ …          ┆ …                                 ┆ …              ┆ …   │
│ 1         ┆ 6512043    ┆ altrincham, trafford, united kin… ┆ united kingdom ┆ 40  │
│ 1         ┆ 7134420    ┆ new lenox, illinois, united stat… ┆ united states  ┆ 40  │
│ 2         ┆ 1375651    ┆ hove, brighton and hove, united … ┆ united kingdom ┆ 37  │
│ 2         ┆ 3017119    ┆ weymouth, dorset, united kingdom  ┆ united kingdom ┆ 37  │
└───────────┴────────────┴───────────────────────────────────┴────────────────┴─────┘
python generate_data_polars.py --num_persons 1000000 --num_positions 10000000  23.64s user 5.73s system 356% cpu 8.242 total
```
