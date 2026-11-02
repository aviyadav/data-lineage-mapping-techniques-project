import re
from pathlib import Path
from typing import Any, Dict, List, Optional

import sqlglot as sg
from sqlglot import exp

from config import BASE_DIR


SENSITIVE_COLUMNS = {
    "email",
    "customer_id",
    "ssn",
    "phone",
    "address",
    "payment_method",
}


def _relative_path(path: Path) -> str:
    try:
        return str(path.relative_to(BASE_DIR))
    except ValueError:
        return str(path)


def _table_full_name(table: exp.Table) -> str:
    parts = []
    if table.catalog:
        parts.append(table.catalog)
    if table.db:
        parts.append(table.db)
    if table.name:
        parts.append(table.name)
    return ".".join(parts)


def _build_alias_map(select: exp.Select) -> Dict[str, str]:
    alias_map: Dict[str, str] = {}

    for table in select.find_all(exp.Table):
        full_name = _table_full_name(table)
        alias = table.alias

        if full_name:
            alias_map[full_name] = full_name

        if alias:
            alias_map[alias] = full_name

    return alias_map


def _classify_column(column_node: exp.Column, projection_node: exp.Expression) -> str:
    node = column_node

    while node is not None and node is not projection_node:
        if isinstance(node, exp.AggFunc):
            return "AGGREGATION"
        if isinstance(node, exp.Cast):
            return "CAST"
        if isinstance(node, exp.Case):
            return "CONDITIONAL"
        if isinstance(node, exp.Func):
            return "FUNCTION"
        node = node.parent

    return "DIRECT_COPY"


def _extract_column_lineage(
    select: exp.Select,
    target_table: str,
    source_tables: List[str],
    alias_map: Dict[str, str],
) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    seen = set()

    default_source = source_tables[0] if len(source_tables) == 1 else None

    for projection in select.expressions:
        target_column = projection.alias_or_name

        if not target_column or target_column == "*":
            continue

        for column_node in projection.find_all(exp.Column):
            source_column = column_node.name

            if column_node.table:
                source_table = alias_map.get(column_node.table, column_node.table)
            else:
                source_table = default_source or "unknown"

            if source_table == target_table:
                continue

            transformation_type = _classify_column(column_node, projection)

            if column_node.table and column_node.table in alias_map:
                confidence = 0.95
            elif source_table != "unknown":
                confidence = 0.85
            else:
                confidence = 0.60

            sensitive = (
                source_column.lower() in SENSITIVE_COLUMNS
                or target_column.lower() in SENSITIVE_COLUMNS
            )

            key = (
                source_table,
                source_column,
                target_table,
                target_column,
                transformation_type,
            )

            if key in seen:
                continue

            seen.add(key)

            entries.append(
                {
                    "source_table": source_table,
                    "source_column": source_column,
                    "target_table": target_table,
                    "target_column": target_column,
                    "transformation_type": transformation_type,
                    "confidence": confidence,
                    "sensitive": sensitive,
                }
            )

    return entries


def parse_sql_file(path: Path) -> Dict[str, Any]:
    """
    Parses a SQL file containing CREATE TABLE ... AS SELECT ...
    and extracts table-level and column-level lineage.
    """
    sql = path.read_text(encoding="utf-8")

    target_match = re.search(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([\w\.]+)\s+AS",
        sql,
        re.IGNORECASE,
    )

    if not target_match:
        return {}

    target_table = target_match.group(1)

    statements = sg.parse(sql, read="postgres")

    sources: List[str] = []
    column_lineage: List[Dict[str, Any]] = []

    for statement in statements:
        if not isinstance(statement, exp.Create):
            continue

        select = statement.find(exp.Select)
        if not select:
            continue

        alias_map = _build_alias_map(select)

        statement_sources = set()
        for table in select.find_all(exp.Table):
            full_name = _table_full_name(table)
            if full_name and full_name != target_table:
                statement_sources.add(full_name)

        sources.extend(sorted(statement_sources))
        column_lineage.extend(
            _extract_column_lineage(
                select=select,
                target_table=target_table,
                source_tables=sorted(statement_sources),
                alias_map=alias_map,
            )
        )

    # Deduplicate sources while preserving order.
    deduped_sources = sorted(set(sources))

    return {
        "target": target_table,
        "sources": deduped_sources,
        "column_lineage": column_lineage,
        "source_file": _relative_path(path),
    }


def persist_lineage_for_file(
    conn,
    parsed: Dict[str, Any],
    run_id: Optional[str] = None,
) -> None:
    target_table = parsed.get("target")
    if not target_table:
        return

    source_file = parsed.get("source_file")

    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM lineage.table_lineage
            WHERE target_table = %s AND source_file = %s
            """,
            (target_table, source_file),
        )

        cur.execute(
            """
            DELETE FROM lineage.column_lineage
            WHERE target_table = %s AND source_file = %s
            """,
            (target_table, source_file),
        )

        for source_table in parsed.get("sources", []):
            cur.execute(
                """
                INSERT INTO lineage.table_lineage (
                    source_table,
                    target_table,
                    transformation_type,
                    confidence,
                    source_file,
                    run_id
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    source_table,
                    target_table,
                    "SQL_TRANSFORM",
                    0.95,
                    source_file,
                    run_id,
                ),
            )

        for entry in parsed.get("column_lineage", []):
            cur.execute(
                """
                INSERT INTO lineage.column_lineage (
                    source_table,
                    source_column,
                    target_table,
                    target_column,
                    transformation_type,
                    confidence,
                    sensitive,
                    source_file,
                    run_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    entry["source_table"],
                    entry["source_column"],
                    entry["target_table"],
                    entry["target_column"],
                    entry["transformation_type"],
                    entry["confidence"],
                    entry["sensitive"],
                    source_file,
                    run_id,
                ),
            )

    conn.commit()