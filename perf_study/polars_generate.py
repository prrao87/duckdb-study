import argparse
from pathlib import Path

import numpy as np
import polars as pl
from codetiming import Timer


def get_companies(filename: str) -> pl.DataFrame:
    """Reads the companies data from a csv file and returns a polars DataFrame."""
    lazy_df = pl.scan_parquet(filename)
    # Cast year_founded as int
    lazy_df = (
        lazy_df.with_columns(pl.col("year founded").cast(pl.Int32, strict=True))
        .filter(pl.col("country").is_not_null() & pl.col("year founded").is_not_null())
        .rename({"": "company_id"})
    )
    return lazy_df


def get_top_country_counts(companies: pl.DataFrame) -> list[int]:
    companies_count = (
        companies.groupby("country")
        .agg(pl.count("company_id").alias("counts"))
        .sort("counts", descending=True)
    )
    companies_count = companies_count.collect().get_column("country").to_list()[:10]
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
    company_ids = final_companies.get_column("company_id").to_list()
    companies_list = np.random.choice(company_ids, size=num_positions, replace=True)
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


def main(input_file: Path, num_persons: int, num_positions: int) -> pl.DataFrame:
    with Timer(name="scan file", text="Scanned input file in {:.4f}s"):
        companies = get_companies(input_file)
    top_10_countries = get_top_country_counts(companies)

    # Filter companies by top 10 countries
    final_companies = companies.filter(
        (pl.col("country").is_in(top_10_countries) & pl.col("locality").is_not_null())
    )
    final_companies = final_companies.sort("company_id").collect()
    person_ages, person_company_df = construct_person_company_df(
        num_persons, num_positions, final_companies
    )
    person_company_and_location_df = construct_person_company_and_location_df(
        person_ages, person_company_df, final_companies
    )
    return person_company_and_location_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num_persons", type=int, default=int(1e6))
    parser.add_argument("--num_positions", type=int, default=int(1e7))
    parser.add_argument("--input_file", type=str, default="companies_sorted.parquet")
    args = parser.parse_args()

    NUM_PERSONS = args.num_persons
    NUM_POSITIONS = args.num_positions
    INPUT_FILE = Path(__file__).resolve().parents[1] / "data" / args.input_file

    np.random.seed(37)

    with Timer(name="generation", text="Generating data completed in {:.4f}s"):
        result = main(INPUT_FILE, NUM_PERSONS, NUM_POSITIONS)
        print(f"Obtained persons, companies and locations DataFrame of shape: {result.shape}")
        print(result.head(10))
