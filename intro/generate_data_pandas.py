import argparse
from pathlib import Path

import numpy as np
import pandas as pd
from codetiming import Timer


def get_companies(filename: str) -> pd.DataFrame:
    """Reads the companies data from a csv f
    ile and returns a pandas DataFrame."""
    df = pd.read_parquet(filename).rename(columns={"": "company_id"})
    # Replace spaces with underscores in column names
    df.columns = df.columns.str.replace(" ", "_")
    # Ensure no null values are present in `year_founded` column
    df = df[df.year_founded.notnull() & df.country.notnull()]
    # Cast year_founded as int
    df.year_founded = df.year_founded.astype(int)
    if LIMIT > 0:
        df = df.iloc[:LIMIT, :]
    return df


def get_top_country_counts(companies: pd.DataFrame) -> pd.DataFrame:
    companies_count = (
        companies.groupby("country")
        .agg({"company_id": "count"})
        .sort_values(by="company_id", ascending=False)
    )
    return companies_count


def construct_person_company_df(
    num_persons: int, num_positions: int, final_companies: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    persons = np.arange(start=1, stop=num_persons + 1)
    # Generate ages for persons between a specified range
    ages = np.random.randint(low=25, high=65, size=num_persons)
    person_ages = pd.DataFrame(
        {
            "person_id": persons,
            "age": ages,
        }
    )
    # Create a list of persons with repetition
    person_ids = np.random.choice(persons, size=num_positions, replace=True)
    sorted_persons = np.sort(person_ids)
    # Pick company ids at random with repetition
    companies_list = np.random.choice(
        final_companies["company_id"].to_list(), size=num_positions, replace=True
    )
    # Combine persons and companies columns into a DataFrame
    person_company_df = pd.DataFrame(
        {
            "person_id": sorted_persons,
            "company_id": companies_list,
        }
    ).reset_index(drop=True)
    person_company_df = person_company_df.drop_duplicates().sort_values(by="person_id")
    return person_ages, person_company_df


def construct_person_company_and_location_df(
    person_ages: pd.DataFrame,
    person_company_df: pd.DataFrame,
    companies: pd.DataFrame,
) -> pd.DataFrame:
    person_and_companies = (
        person_company_df.join(companies.set_index("company_id"), how="inner", on="company_id")
    ).reset_index(drop=True)
    temp_df = person_and_companies[["person_id", "company_id", "locality", "country"]]
    person_company_and_location_df = (
        temp_df.join(person_ages.set_index("person_id"), on="person_id").sort_values(
            by=["person_id", "company_id"]
        )
    ).reset_index(drop=True)
    return person_company_and_location_df


def main(num_persons: int, num_positions: int) -> None:
    with Timer(name="read file", text="Read input file in {:.4f}s"):
        companies = get_companies(INPUT_FILE)
    companies_count = get_top_country_counts(companies)
    # Top 10 countries
    top_10_countries = companies_count.iloc[:10, :].index.to_list()
    # Choose only those companies that are in the top 10 countries
    final_companies = (
        (companies[companies.country.isin(top_10_countries) & companies.locality.notnull()])
        .sort_values(by="company_id")
        .reset_index(drop=True)
    )
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
        result = main(NUM_PERSONS, NUM_POSITIONS)
        print(f"Obtained persons, companies and locations DataFrame of shape: {result.shape}")
        print(result.head(10))
