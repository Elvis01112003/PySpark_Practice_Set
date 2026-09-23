from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr


# ============================================================
# STEP 1: Create the target table
# ============================================================

dp.create_streaming_table(
    "customer_history"
)


# ============================================================
# STEP 2: Create AUTO CDC flow
# ============================================================

dp.create_auto_cdc_flow(

    # Target table
    target="customer_history",

    # Source CDC table
    source="workspace.default.customer_cdc",

    # Business key
    keys=["customer_id"],

    # Column used to determine the order of changes
    sequence_by=col("sequence_num"),

    # Treat records with operation = DELETE as deletes
    apply_as_deletes=expr("operation = 'DELETE'"),

    # Don't copy CDC control columns to the target
    except_column_list=[
        "operation",
        "sequence_num"
    ],

    # Keep history
    stored_as_scd_type=2
)