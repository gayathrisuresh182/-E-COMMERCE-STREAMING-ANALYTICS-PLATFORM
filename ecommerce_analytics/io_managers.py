"""
Dagster I/O managers for BigQuery and other storage.
Phase 1D: BigQuery I/O manager - writes pandas DataFrames to BigQuery.
"""

# Use dagster-gcp's BigQueryIOManager - configure with project and datasets.
# Example usage in Definitions:
#
# from dagster_gcp import BigQueryIOManager
#
# bigquery_io_manager = BigQueryIOManager(
#     project="your-gcp-project",
#     gcp_credentials=None,  # Uses GOOGLE_APPLICATION_CREDENTIALS if None
# )
#
# definitions = Definitions(
#     assets=[...],
#     resources={
#         "io_manager": bigquery_io_manager,
#     },
# )
#
# For dataset-specific routing, use BigQueryIOManager with dataset parameter
# or a custom I/O manager that selects dataset per asset.
