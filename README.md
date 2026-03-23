# enterprise-cloud-data-pipeline

These decisions are fixed to keep scope controlled and ensure the pipeline ships by next Monday.

1) Transformation Engine
- Choice: Python + DuckDB
- Why: Fast local execution, SQL-friendly transformations, minimal setup overhead.

2) Fact Table Grain (Gold Layer)
- Choice: 1 row per `order_item`
- Why: More flexible analytics (product-level insights, basket analysis) and aligns with enterprise modeling.

3) Dataset Domain
- Choice: Retail sales (structured)
- Core entities:
  - orders
  - order_items
  - customers
  - products
  - stores (or regions)
