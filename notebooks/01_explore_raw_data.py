import marimo

__generated_with = "0.17.8"
app = marimo.App(width="medium")


@app.cell
def _():
    # Imports
    import sys
    from pathlib import Path
    import polars as pl
    import marimo as mo

    # Get project root (parent of notebooks directory)
    project_root = Path(__file__).parent.parent
    sys.path.append(str(project_root / "src"))

    from utils.config import get_insee_urls, load_config, get_data_paths
    from ingestion import download_data

    mo.md("# Data Exploration")
    return Path, download_data, get_data_paths, load_config, mo, pl


@app.cell
def _(mo):
    mo.md("""
    ## Config
    """)
    return


@app.cell
def _(pl):
    pl.Config.set_tbl_cols(-1)  # Show all columns for Polars
    pl.Config.set_tbl_width_chars(1000)  # Increase width for better display
    SAMPLE_SIZE = 10**5 # Files are very large (>10m rows) : sample size will be used to analyze them
    return (SAMPLE_SIZE,)


@app.cell
def _(Path, get_data_paths):
    DATA_PATH = Path(get_data_paths()["bronze"])
    return (DATA_PATH,)


@app.cell
def _(mo):
    mo.md("""
    ## Download files
    """)
    return


@app.cell
def _(DATA_PATH, download_data, load_config):
    download_data.download_insee_files(
        config=load_config(),
        output_dir=DATA_PATH
    )
    return


@app.cell
def _(mo):
    mo.md("""
    ## Quick exploration

    First let's have a look to the downloaded files to understand data structure.
    """)
    return


@app.cell
def _(DATA_PATH, SAMPLE_SIZE, pl):
    files = {}
    file_stats = {}
    for file in list(DATA_PATH.glob("*.parquet")):
        print(f"\n{'='*80}")
        print(file)

        # Store lazy reference
        lazy_df = pl.scan_parquet(file)
        files[file.name] = lazy_df

        # Get total row count
        total_rows = lazy_df.select(pl.len()).collect().item()
        print(f"Total rows: {total_rows:,}")
    
        # Show sample data
        print("\nSample data (first 10 rows):")
        print(pl.read_parquet(file, n_rows=10))

        # Read sample for completion analysis
        df_sample = pl.read_parquet(file, n_rows=SAMPLE_SIZE)
    
        print(f"\nCompletion rate (based on {SAMPLE_SIZE:,} sample size rows)")
        print(f"\n{'Column':<50} {'Type':<15} {'Completion %':<15}")
        print('-'*80)
    
        # Calculate completion % for each column
        completion_stats = []
        for col in df_sample.columns:
            null_count = df_sample[col].null_count()
            completion_pct = ((SAMPLE_SIZE - null_count) / SAMPLE_SIZE) * 100
            dtype = str(df_sample[col].dtype)
        
            print(f"{col:<50} {dtype:<15} {completion_pct:>6.2f}%")
        
            completion_stats.append({
                'column': col,
                'type': dtype,
                'completion_pct': completion_pct,
                'null_count': null_count
            })
    
        # Store stats
        file_stats[file.name] = {
            'total_rows': total_rows,
            'sample_size': SAMPLE_SIZE,
            'num_columns': len(df_sample.columns),
            'completion_stats': completion_stats
        }
    return


@app.cell
def _(mo):
    mo.md(r"""
    Files are huge (93m rows for StockEtablissementHistorique_utf8 !) and cannot be load into memory. Pandas cannot be used, and we are forced to use lazy evaluation in Polars (scan_parquet).
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    We want to predict when there is a change in the etatAdministratifUniteLegale field : from A value (the company is operating) to C value (the company has been closed). Changes on this field are tracked in the StockUniteLegaleHistorique_utf8 file.

    We will need in later stages to transform this log tracking to a be the target value of our ML model : 1 for churn and 0 for non churn, and build features around this one which will take into account the time window.

    But before that, I am going to clean this data and store it in silver tables for easier management. I am going as well to set some filter to focus only on the scope I am interested in (legal entities only, no foreign subsidiary, ...)
    """)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
