"""Idempotent Delta writes.

NOTE: these use Delta's ``replaceWhere`` and need a Delta-enabled Spark (Databricks).
They could not be run in the local test environment, so they are untested until the
first run on Databricks.
"""

from pyspark.sql import DataFrame


def write_replace_where(df: DataFrame, table: str, predicate: str) -> None:
    """Overwrite only the rows matching ``predicate`` so re-running a month never duplicates it.

    Every row in ``df`` must satisfy ``predicate`` (Delta enforces this). On the very first
    run, when the table does not exist yet, it is simply created.
    """
    spark = df.sparkSession
    if not spark.catalog.tableExists(table):
        df.write.saveAsTable(table)
        return
    (
        df.write.mode("overwrite")
        .option("replaceWhere", predicate)
        .option("mergeSchema", "true")  # lets tables gain new columns (e.g. tip_amount)
        .saveAsTable(table)
    )
