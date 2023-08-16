import argparse
from pathlib import Path

import duckdb
import numpy as np
import polars as pl
import pyarrow as pa
from duckdb import DuckDBPyConnection, DuckDBPyRelation


def get_companies_table(conn: DuckDBPyConnection, parquet_file: str) -> DuckDBPyRelation:
    companies = conn.sql(f"SELECT * FROM '{parquet_file}'")
    # Obtain column names and replace spaces with underscores
    names = companies.columns
    names = [(name, name.replace(" ", "_").lower()) for name in names]
    # Rename first column to "company_id"
    names[0] = ("C0", "company_id")
    projection = ", ".join([f'"{old}" AS {new}' for old, new in names])
    companies = companies.project(projection)
    if LIMIT > 0:
        companies = companies.limit(LIMIT)
    return companies


def get_top_10_countries(conn: DuckDBPyConnection, companies: DuckDBPyRelation) -> DuckDBPyRelation:
    top_10_countries = conn.sql(
        """
        SELECT country, COUNT(company_id) as counts FROM companies
        WHERE country IS NOT NULL
        GROUP BY country
        ORDER BY counts DESC
        """
    ).limit(10)
    return top_10_countries


def get_final_companies(
    conn: DuckDBPyConnection, companies: DuckDBPyRelation, top_10_countries: DuckDBPyRelation
) -> DuckDBPyRelation:
    final_companies = conn.sql(
        """
            SELECT * FROM companies
            WHERE country IN (SELECT country FROM top_10_countries)
            AND locality IS NOT NULL
            ORDER BY company_id
            """
    )
    return final_companies


def get_person_companies(
    conn: DuckDBPyConnection,
    num_persons: int,
    num_positions: int,
    final_companies: DuckDBPyRelation,
) -> tuple[pa.Table, pa.Table]:
    persons = np.arange(start=1, stop=num_persons + 1)
    # Generate ages for persons between a specified range
    ages = np.random.randint(low=25, high=65, size=num_persons)
    # Construct person-ages pyarrow table
    person_ages_tbl = pa.Table.from_arrays(
        [
            pa.array(persons),
            pa.array(ages),
        ],
        names=["person_id", "age"],
    )
    # Create a list of persons with repetition
    person_ids = np.random.choice(persons, size=num_positions, replace=True)
    sorted_persons = np.sort(person_ids)
    # Pick company ids at random with repetition
    final_companies_list = conn.sql("SELECT company_id FROM final_companies").fetchall()
    final_companies_list = [x[0] for x in final_companies_list]
    companies_list = np.random.choice(final_companies_list, size=num_positions, replace=True)
    # Construct a pyarrow table to read into DuckDB
    person_companies_tbl = pa.Table.from_arrays(
        [
            pa.array(sorted_persons),
            pa.array(companies_list),
        ],
        names=["person_id", "company_id"],
    )
    return person_ages_tbl, person_companies_tbl


def get_person_companies_locations(
    conn: DuckDBPyConnection,
    companies: DuckDBPyRelation,
    person_ages_tbl: pa.Table,
    person_companies_tbl: DuckDBPyRelation,
) -> pl.DataFrame:
    person_companies = conn.sql("SELECT * FROM person_companies_tbl ORDER BY person_id")
    result = conn.sql(
        """
        SELECT person_companies.person_id, companies.company_id, locality, country, age
        FROM person_companies
        JOIN companies ON (person_companies.company_id = companies.company_id)
        JOIN person_ages_tbl ON (person_companies.person_id = person_ages_tbl.person_id)
        ORDER BY person_companies.person_id, companies.company_id
        """
    )
    return result.pl()


def main(conn: DuckDBPyConnection, num_persons: int, num_positions: int) -> pl.DataFrame:
    companies = get_companies_table(conn, INPUT_FILE)
    top_10_countries = get_top_10_countries(conn, companies)
    final_companies = get_final_companies(conn, companies, top_10_countries)
    person_ages_tbl, person_companies_tbl = get_person_companies(
        conn, num_persons, num_positions, final_companies
    )
    result = get_person_companies_locations(conn, companies, person_ages_tbl, person_companies_tbl)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num_persons", type=int, default=200)
    parser.add_argument("--num_positions", type=int, default=300)
    parser.add_argument("--input_file", type=str, default="companies_sorted.parquet")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    LIMIT = args.limit
    NUM_PERSONS = args.num_persons
    NUM_POSITIONS = args.num_positions
    INPUT_FILE = Path(__file__).resolve().parents[1] / "data" / args.input_file

    np.random.seed(37)

    # Obtain duckdb connection
    CONNECTION = duckdb.connect()

    result = main(CONNECTION, NUM_PERSONS, NUM_POSITIONS)
    print(f"Obtained persons, companies and locations table")
    print(result.head(10))
