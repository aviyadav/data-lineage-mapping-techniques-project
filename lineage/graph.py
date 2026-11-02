from typing import Dict, List


def get_downstream(conn, table_name: str, max_hops: int = 10) -> List[Dict]:
    query = """
    WITH RECURSIVE descendants AS (
        SELECT
            target_table AS node,
            1 AS hop
        FROM lineage.table_lineage
        WHERE source_table = %(table)s

        UNION

        SELECT
            tl.target_table AS node,
            d.hop + 1 AS hop
        FROM lineage.table_lineage tl
        JOIN descendants d
          ON tl.source_table = d.node
        WHERE d.hop < %(max_hops)s
    )
    SELECT
        node,
        MIN(hop) AS hop
    FROM descendants
    GROUP BY node
    ORDER BY hop ASC, node ASC
    """

    with conn.cursor() as cur:
        cur.execute(query, {"table": table_name, "max_hops": max_hops})
        return [dict(row) for row in cur.fetchall()]


def get_upstream(conn, table_name: str, max_hops: int = 10) -> List[Dict]:
    query = """
    WITH RECURSIVE ancestors AS (
        SELECT
            source_table AS node,
            1 AS hop
        FROM lineage.table_lineage
        WHERE target_table = %(table)s

        UNION

        SELECT
            tl.source_table AS node,
            a.hop + 1 AS hop
        FROM lineage.table_lineage tl
        JOIN ancestors a
          ON tl.target_table = a.node
        WHERE a.hop < %(max_hops)s
    )
    SELECT
        node,
        MIN(hop) AS hop
    FROM ancestors
    GROUP BY node
    ORDER BY hop ASC, node ASC
    """

    with conn.cursor() as cur:
        cur.execute(query, {"table": table_name, "max_hops": max_hops})
        return [dict(row) for row in cur.fetchall()]


def find_common_ancestors(conn, table_a: str, table_b: str, max_hops: int = 10) -> List[str]:
    query = """
    WITH RECURSIVE anc_a AS (
        SELECT source_table AS node, 1 AS hop
        FROM lineage.table_lineage
        WHERE target_table = %(a)s

        UNION

        SELECT tl.source_table, a.hop + 1
        FROM lineage.table_lineage tl
        JOIN anc_a a ON tl.target_table = a.node
        WHERE a.hop < %(max_hops)s
    ),
    anc_b AS (
        SELECT source_table AS node, 1 AS hop
        FROM lineage.table_lineage
        WHERE target_table = %(b)s

        UNION

        SELECT tl.source_table, b.hop + 1
        FROM lineage.table_lineage tl
        JOIN anc_b b ON tl.target_table = b.node
        WHERE b.hop < %(max_hops)s
    )
    SELECT DISTINCT a.node AS shared_source
    FROM anc_a a
    JOIN anc_b b ON a.node = b.node
    ORDER BY shared_source
    """

    with conn.cursor() as cur:
        cur.execute(query, {"a": table_a, "b": table_b, "max_hops": max_hops})
        return [row["shared_source"] for row in cur.fetchall()]