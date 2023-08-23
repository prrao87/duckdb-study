# Embedded databases

This repo showcases the power of using embedded databases to assist in data-generation and data-processing workflows. The following embedded databases and their associated data structures are considered in the provided examples:

- [DuckDB](https://github.com/duckdb/duckdb) (table)
- [KùzuDB](https://github.com/kuzudb/kuzu) (graph)
- [LanceDB](https://github.com/lancedb/lancedb) (vector)

## Setup

Install a Python virtual environment and then install the dependencies via `requirements.txt` as follows.

```sh
python -m venv .venv  # python -> python 3.10+
source .venv/bin/activate
python -m pip install -U pip wheel  # Upgrade pip and install the wheel package first
python -m pip install -r requirements.txt
```


## What are embedded databases?

An embedded database is a DBMS (database management system) that is tightly integrated with the application software. They are often referred to as serverless databases, because they do not require a client-server architecture (as is common in large-scale software systems). Another characteristic of embedded databases is that they typically offer a choice between in-memory storage, or serverless on-disk storage for larger-than-memory datasets.

It is this combination of features that allows embedded databases to be the go-to choice for OLAP (online analytical processing) workflows, i.e., when the consumer of the data wants to run analytical queries on the data.


### DuckDB

DuckDB is a high-performance embedded database that can be queried via a rich SQL dialect. Its core is written in C++, and is designed to be fast, reliable and easy to set up and use. DuckDB is optimized for OLAP query workloads, which are typically characterized by complex, relatively long-running queries that process significant portions of the stored dataset, for example, aggregations over entire tables or joins between several large tables. Changes to the data are expected to be rather large-scale as well, with several rows being appended, or large portions of tables being changed or added at the same time.

### Kùzu

Kùzu is an embedded property graph database management system (GDBMS) written in C++, built for query speed and scalability. Just like DuckDB, it is optimized for OLAP query workloads, but on graphs, and performs very well on complex join-heavy workloads involving aggregations involving many-to-many relationships.

### LanceDB

LanceDB is an open-source embedded database for vector-search built with persistent storage, which greatly simplifies retrieval, filtering and management of embeddings. LanceDB's core is written in Rust 🦀 and is built using [Lance](https://github.com/lancedb/lance), an open-source columnar format designed for performant ML workloads that's [100x faster than parquet](https://blog.eto.ai/benchmarking-random-access-in-lance-ed690757a826) for random access. It can be queried in a number of ways, including via SQL, full-text search, and vector search.
