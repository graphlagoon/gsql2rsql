# Query Examples

Welcome to the gsql2rsql query examples gallery!

This section demonstrates the transpiler's capabilities across different domains.
Each example shows the original OpenCypher query alongside the generated Databricks SQL.

## Available Categories

### [Fraud](fraud.md)

- **Total Queries**: 17
- **Successful**: 17
- **Failed**: 0

### [Credit](credit.md)

- **Total Queries**: 14
- **Successful**: 14
- **Failed**: 0

### [Features](features.md)

- **Total Queries**: 54
- **Successful**: 54
- **Failed**: 0

## About These Examples

All queries are sourced from real-world use cases in:

- **Fraud Detection**: Graph-based fraud ring detection, anomaly identification
- **Credit Analysis**: Relationship-based credit risk assessment
- **Simple examples**: Simple examples

!!! tip "Try These Yourself"
    You can run any of these queries through the transpiler using:
    ```bash
    gsql2rsql translate --schema examples/fraud_queries.yaml "<your-query>"
    ```
