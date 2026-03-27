from fastapi import FastAPI, Query
import polars as pl
from polars import col as c
import polars.selectors as cs
from typing import List, Annotated, Literal

def read_data() -> pl.LazyFrame:
    return pl.scan_parquet('data/benchmarks.parquet')

app = FastAPI()

@app.get("/item/")
def get_item(product: str| None = None, ndc: str| None = None, benchmark: Annotated[list[Literal['al-aac', 'awp','big4','fss','nadac','ia-aac']] | None, Query()] = None):
    df = (read_data())
    if product:
        print(product)
        df = df.filter(c.product.str.contains(f'(?i){product}'))    
    if ndc:
        print(ndc)
        df = df.filter(c.ndc.str.contains(f'(?i){ndc}'))
    if benchmark:
        print(benchmark)
        df = df.filter(c.benchmark.is_in(benchmark))
    
    return df.collect(engine='streaming').to_dicts()


