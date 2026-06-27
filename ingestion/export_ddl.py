"""
export_ddl.py
Script sekali run untuk mengekstrak DDL dari semua layer DuckDB.
Jalankan dari dalam container Airflow:

    python3 /opt/airflow/ingestion/export_ddl.py

Output: /opt/airflow/duckdb/ddl_export.sql
"""

import duckdb

DUCKDB_PATH = "/opt/airflow/duckdb/weather.duckdb"
OUTPUT_PATH = "/opt/airflow/duckdb/ddl_export.sql"

SCHEMAS = ["staging", "silver", "gold"]

con = duckdb.connect(DUCKDB_PATH, read_only=True)

lines = []
lines.append("-- ============================================================")
lines.append("-- DDL Export — Weather Madiun Pipeline")
lines.append("-- DuckDB file: weather.duckdb")
lines.append("-- ============================================================")
lines.append("")

for schema in SCHEMAS:
    lines.append(f"-- ── Schema: {schema} {'─' * (50 - len(schema))}")
    lines.append(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    lines.append("")

    # Tables
    tables = con.execute(f"""
        SELECT table_name, sql
        FROM duckdb_tables()
        WHERE schema_name = '{schema}'
        ORDER BY table_name
    """).fetchall()

    for table_name, sql in tables:
        lines.append(f"-- {schema}.{table_name}")
        lines.append(sql.strip() + ";")
        lines.append("")

    # Views
    views = con.execute(f"""
        SELECT view_name, sql
        FROM duckdb_views()
        WHERE schema_name = '{schema}'
          AND internal = false
        ORDER BY view_name
    """).fetchall()

    for view_name, sql in views:
        lines.append(f"-- {schema}.{view_name} (VIEW)")
        lines.append(sql.strip() + ";")
        lines.append("")

    # Sequences
    sequences = con.execute(f"""
        SELECT sequence_name, start_value, increment_by
        FROM duckdb_sequences()
        WHERE schema_name = '{schema}'
        ORDER BY sequence_name
    """).fetchall()

    for seq_name, start_value, increment_by in sequences:
        lines.append(f"-- {schema}.{seq_name} (SEQUENCE)")
        lines.append(
            f"CREATE SEQUENCE IF NOT EXISTS {schema}.{seq_name} "
            f"START {start_value} INCREMENT {increment_by};"
        )
        lines.append("")

con.close()

output = "\n".join(lines)
with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
    f.write(output)

print(f"DDL berhasil diekspor ke: {OUTPUT_PATH}")
print(f"Total baris: {len(lines)}")
