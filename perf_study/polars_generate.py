import argparse
from pathlib import Path

import numpy as np
import polars as pl
from codetiming import Timer


def get_companies(filename: str, limit: int) -> pl.DataFrame:
    """Reads the companies data from a csv file and returns a polars DataFrame."""
    df = pl.read_parquet(filename, use_pyarrow=True)
    # Replace spaces with underscores in column names
    df.columns = list(map(lambda x: x.replace(" ", "_"), df.columns))
    # Cast year_founded as int
    df = (
        df.with_columns(pl.col("year_founded").cast(pl.Int32, strict=True))
        .rename({"column_0": "company_id"})
        .filter(pl.col("country").is_not_null() & pl.col("year_founded").is_not_null())
    )
    if limit > 0:
        df = df.limit(limit)
    return df


def get_top_country_counts(companies: pl.DataFrame) -> pl.DataFrame:
    companies_count = (
        companies.groupby("country")
        .agg(pl.count("company_id").alias("counts"))
        .sort("counts", descending=True)
    )
    return companies_count


def construct_person_company_df(
    num_persons: int, num_positions: int, final_companies: pl.DataFrame
) -> tuple[pl.DataFrame, pl.DataFrame]:
    persons = np.arange(start=1, stop=num_persons + 1)
    # Generate ages for persons between a specified range
    ages = np.random.randint(low=25, high=65, size=num_persons)
    person_ages = pl.DataFrame((persons, ages), schema=["person_id", "age"])
    # Create a list of persons with repetition
    person_ids = np.random.choice(persons, size=num_positions, replace=True)
    sorted_persons = np.sort(person_ids)
    # Pick company ids at random with repetition
    companies_list = np.random.choice(
        final_companies["company_id"].to_list(), size=num_positions, replace=True
    )
    # Combine persons and companies columns into a DataFrame
    person_company_df = pl.DataFrame(
        (sorted_persons, companies_list), schema=["person_id", "company_id"]
    )
    person_company_df = person_company_df.sort(by="person_id")
    return person_ages, person_company_df


def construct_person_company_and_location_df(
    person_ages: pl.DataFrame,
    person_company_df: pl.DataFrame,
    companies: pl.DataFrame,
) -> pl.DataFrame:
    person_company_and_location_df = (
        person_company_df.join(companies, on="company_id")
        .select(
            [
                "person_id",
                "company_id",
                "locality",
                "country",
            ]
        )
        .join(person_ages, on="person_id")
        .sort(["person_id", "company_id"])
    )
    return person_company_and_location_df


def main(input_file: Path, num_persons: int, num_positions: int, limit: int) -> pl.DataFrame:
    with Timer(name="read file", text="Read input file in {:.4f}s"):
        companies = get_companies(input_file, limit)
    top_10_countries = get_top_country_counts(companies)["country"][:10].to_list()

    # Filter companies by top 10 countries
    final_companies = companies.filter(
        (pl.col("country").is_in(top_10_countries) & pl.col("locality").is_not_null())
    ).sort("company_id")
    person_ages, person_company_df = construct_person_company_df(
        num_persons, num_positions, final_companies
    )
    person_company_and_location_df = construct_person_company_and_location_df(
        person_ages, person_company_df, final_companies
    )
    return person_company_and_location_df


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

    with Timer(name="generation", text="Generating data completed in {:.4f}s"):
        result = main(INPUT_FILE, NUM_PERSONS, NUM_POSITIONS, LIMIT)
        print(f"Obtained persons, companies and locations DataFrame of shape: {result.shape}")
        print(result.head(10))
