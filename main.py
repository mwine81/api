from fastapi import FastAPI, Query
import polars as pl
from polars import col as c
import polars.selectors as cs
from typing import List, Annotated, Literal
import duckdb
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

def create_remote_connection():
    token = os.getenv("TOKEN_RW")
    return duckdb.connect(f"md:benchmarks?motherduck_token={token}")

def read_database(product_regex: str | None = None, ndc: str | None = None, benchmark: list[str] | None = None):
    query = "SELECT * FROM benchmarks"
    filters = []
    params = []

    if product_regex is not None:
        filters.append("regexp_matches(product, ?, 'i')")
        params.append(product_regex)

    if ndc is not None:
        filters.append("ndc = ?")
        params.append(ndc)

    if benchmark is not None:
        placeholders = ", ".join("?" * len(benchmark))
        filters.append(f"benchmark IN ({placeholders})")
        params.extend(benchmark)

    if filters:
        query += " WHERE " + " AND ".join(filters)

    with create_remote_connection() as conn:
        return conn.execute(query, params).pl()

@app.get("/item/")
def get_item(product: str| None = None, ndc: str| None = None, benchmark: Annotated[list[Literal['al-aac', 'mccpdc','big4','fss','nadac','ia-aac']] | None, Query()] = None):
    data = read_database(product_regex=product, ndc=ndc, benchmark=benchmark)
    
    return data.to_dicts()
if __name__ == "__main__":
    pass
    # read_database(product_regex='(?i)acetaminophen',ndc='0000000', benchmark=['al-aac', 'mccpdc'])

