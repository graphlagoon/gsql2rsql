# Procedural BFS Examples

This page shows queries with **variable-length paths** (VLP) transpiled
using the **procedural BFS** rendering mode.

Procedural BFS uses SQL scripting (`BEGIN...END`, `WHILE`) instead of
`WITH RECURSIVE` CTEs. This enables a **global visited set** that
prevents re-visiting nodes (shortest-path semantics).

Two materialization strategies are shown:

- **Databricks** (`temp_tables`): Uses `CREATE TEMPORARY TABLE` + `INSERT INTO`. Fixed table names, O(1) visited reads per level.
- **PySpark 4.2** (`numbered_views`): Uses `EXECUTE IMMEDIATE` + numbered views. Dynamic names, UNION chain for visited.

---

## Quick Start with GraphContext

The simplest way to use procedural BFS is via `GraphContext`:

!!! warning "PySpark: enable SQL scripting"
    Procedural BFS generates `BEGIN...END` blocks with `DECLARE`, `WHILE`, etc. PySpark requires SQL scripting to be enabled:

    ```python
    spark.conf.set("spark.sql.scripting.enabled", "true")
    ```

    Databricks has SQL scripting enabled by default.

```python
from gsql2rsql import GraphContext

graph = GraphContext(
    spark=spark,
    nodes_table="catalog.schema.nodes",
    edges_table="catalog.schema.edges",
)

# Procedural BFS for Databricks (default)
sql = graph.transpile(
    """
    MATCH (root {node_id: 'Alice'})
    MATCH p = (root)-[*1..4]-()
    UNWIND relationships(p) AS r
    RETURN r
    """,
    vlp_rendering_mode="procedural",
    materialization_strategy="temp_tables",
)

# Procedural BFS for PySpark 4.2+
sql_pyspark = graph.transpile(
    """
    MATCH (root {node_id: 'Alice'})
    MATCH p = (root)-[*1..4]-()
    UNWIND relationships(p) AS r
    RETURN r
    """,
    vlp_rendering_mode="procedural",
    materialization_strategy="numbered_views",
)
```

!!! info "Edge collection support"
    Procedural BFS supports `UNWIND relationships(path) AS r` to access edge properties. Each BFS result row is one edge, so `UNWIND` produces one row per traversed edge. `nodes(path)` is **not** supported (requires full path reconstruction). Use `vlp_rendering_mode='cte'` if you need it.

---

## 1. Analyze transaction chains to assess liquidity patterns

**Source**: Credit

???+ note "OpenCypher Query"
    ```cypher
    MATCH path = (source:Account)-[:TRANSFER*1..3]->(sink:Account)
    WHERE source.customer_id = sink.customer_id
      AND ALL(rel IN relationships(path) WHERE rel.timestamp > TIMESTAMP() - DURATION('P30D'))
    WITH source.customer_id AS customer_id,
         COUNT(DISTINCT path) AS transfer_chains,
         AVG(LENGTH(path)) AS avg_chain_length
    RETURN customer_id, transfer_chains, avg_chain_length
    ORDER BY transfer_chains DESC
    LIMIT 20
    ```

??? example "Procedural BFS — Databricks (temp_tables)"
    ```sql
    BEGIN
      DECLARE current_depth_1 INT DEFAULT 0;
      DECLARE rows_in_frontier_1 BIGINT DEFAULT 1;
    
      DROP TEMPORARY TABLE IF EXISTS bfs_visited_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_frontier_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_result_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_frontier_1_init;
      CREATE TEMPORARY TABLE bfs_visited_1 (node STRING);
      CREATE TEMPORARY TABLE bfs_frontier_1 AS
      SELECT n.id AS node
      FROM catalog.credit.Account n;
      INSERT INTO bfs_visited_1
      SELECT node FROM bfs_frontier_1;
      CREATE TEMPORARY TABLE bfs_result_1 (source_account_id STRING, target_account_id STRING, amount STRING, timestamp STRING, _next_node STRING, _bfs_depth INT);
      CREATE TEMPORARY TABLE bfs_frontier_1_init AS
      SELECT node FROM bfs_frontier_1;
      
      WHILE rows_in_frontier_1 > 0 AND current_depth_1 < 3 DO
        SET current_depth_1 = current_depth_1 + 1;
      
        DROP TEMPORARY TABLE IF EXISTS bfs_edges_1;
        CREATE TEMPORARY TABLE bfs_edges_1 AS
        SELECT e.source_account_id, e.target_account_id, e.amount, e.timestamp, e.target_account_id AS _next_node
      FROM catalog.credit.Transfer e
      INNER JOIN bfs_frontier_1 f ON e.source_account_id = f.node
      WHERE e.target_account_id NOT IN (SELECT node FROM bfs_visited_1) AND (e.timestamp) > ((CURRENT_TIMESTAMP()) - (INTERVAL 30 DAY));
      
        SET rows_in_frontier_1 = (SELECT COUNT(1) FROM bfs_edges_1);
      
        IF rows_in_frontier_1 > 0 THEN
          INSERT INTO bfs_visited_1
          SELECT DISTINCT _next_node FROM bfs_edges_1;
          DROP TEMPORARY TABLE bfs_frontier_1;
          CREATE TEMPORARY TABLE bfs_frontier_1 AS
          SELECT DISTINCT _next_node AS node FROM bfs_edges_1;
          INSERT INTO bfs_result_1
          SELECT *, current_depth_1 AS _bfs_depth FROM bfs_edges_1;
        END IF;
      END WHILE;
      
      CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
      SELECT f0.node AS start_node, r._next_node AS end_node, r._bfs_depth AS depth,
             r.source_account_id, r.target_account_id, r.amount, r.timestamp,
             ARRAY(NAMED_STRUCT('source_account_id', r.source_account_id, 'target_account_id', r.target_account_id, 'amount', r.amount, 'timestamp', r.timestamp)) AS path_edges,
             CAST(NULL AS ARRAY<STRING>) AS path
      FROM bfs_result_1 r
      CROSS JOIN bfs_frontier_1_init f0;
    
      SELECT 
       customer_id AS customer_id
      ,transfer_chains AS transfer_chains
      ,avg_chain_length AS avg_chain_length
    FROM (
      SELECT 
         _gsql2rsql_source_customer_id AS customer_id
        ,COUNT(DISTINCT _gsql2rsql_path_id) AS transfer_chains
        ,AVG(CAST((SIZE(_gsql2rsql_path_id) - 1) AS DOUBLE)) AS avg_chain_length
      FROM (
        SELECT
           sink.id AS _gsql2rsql_sink_id
          ,sink.balance AS _gsql2rsql_sink_balance
          ,sink.customer_id AS _gsql2rsql_sink_customer_id
          ,source.id AS _gsql2rsql_source_id
          ,source.balance AS _gsql2rsql_source_balance
          ,source.customer_id AS _gsql2rsql_source_customer_id
          ,p.start_node
          ,p.end_node
          ,p.depth
          ,p.path AS _gsql2rsql_path_id
          ,p.path_edges AS _gsql2rsql_path_edges
        FROM paths_1 p
        JOIN catalog.credit.Account sink
          ON sink.id = p.end_node
        JOIN catalog.credit.Account source
          ON source.id = p.start_node
        WHERE p.depth >= 1 AND p.depth <= 3
      ) AS _proj
      WHERE (_gsql2rsql_source_customer_id) = (_gsql2rsql_sink_customer_id)
      GROUP BY _gsql2rsql_source_customer_id
    ) AS _proj
    ORDER BY transfer_chains DESC
    LIMIT 20;
    END
    ```

??? example "Procedural BFS — PySpark 4.2 (numbered_views)"
    ```sql
    BEGIN
      DECLARE bfs_depth_1 INT DEFAULT 0;
      DECLARE bfs_frontier_count_1 BIGINT DEFAULT 1;
      DECLARE bfs_union_sql_1 STRING DEFAULT '';
    
      CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_1_0 AS
      SELECT n.id AS node
      FROM catalog.credit.Account n;
      
      CREATE OR REPLACE TEMPORARY VIEW bfs_visited_1_0 AS
      SELECT node FROM bfs_frontier_1_0;
      
      WHILE bfs_frontier_count_1 > 0 AND bfs_depth_1 < 3 DO
        SET bfs_depth_1 = bfs_depth_1 + 1;
      
        EXECUTE IMMEDIATE
          'CREATE OR REPLACE TEMPORARY VIEW bfs_edges_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
           SELECT e.source_account_id, e.target_account_id, e.amount, e.timestamp, e.target_account_id AS _next_node, ' || CAST(bfs_depth_1 AS STRING) || ' AS _bfs_depth FROM catalog.credit.Transfer e INNER JOIN bfs_frontier_1_' || CAST(bfs_depth_1 - 1 AS STRING) || ' f ON e.source_account_id = f.node WHERE e.target_account_id NOT IN (SELECT node FROM bfs_visited_1_' || CAST(bfs_depth_1 - 1 AS STRING) || ') AND (e.timestamp) > ((CURRENT_TIMESTAMP()) - (INTERVAL 30 DAY))';
      
        EXECUTE IMMEDIATE
          'SET bfs_frontier_count_1 = (SELECT COUNT(1) FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING) || ')';
      
        IF bfs_frontier_count_1 > 0 THEN
      
          EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY VIEW bfs_visited_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
             SELECT node FROM bfs_visited_1_' || CAST(bfs_depth_1 - 1 AS STRING) || '
             UNION
             SELECT DISTINCT _next_node AS node
             FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
      
          EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
             SELECT DISTINCT _next_node AS node
             FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
      
          IF bfs_depth_1 >= 1 THEN
            IF bfs_union_sql_1 = '' THEN
              SET bfs_union_sql_1 =
                'SELECT source_account_id, target_account_id, amount, timestamp, _next_node AS end_node, _bfs_depth AS depth FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
            ELSE
              SET bfs_union_sql_1 = bfs_union_sql_1
                || ' UNION ALL SELECT source_account_id, target_account_id, amount, timestamp, _next_node AS end_node, _bfs_depth AS depth FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
            END IF;
          END IF;
      
        END IF;
      END WHILE;
      
      IF bfs_union_sql_1 != '' THEN
        EXECUTE IMMEDIATE
          'CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
           SELECT f0.node AS start_node, r.end_node, r.depth,
                  r.source_account_id, r.target_account_id, r.amount, r.timestamp,
                  ARRAY(NAMED_STRUCT(''source_account_id'', r.source_account_id, ''target_account_id'', r.target_account_id, ''amount'', r.amount, ''timestamp'', r.timestamp)) AS path_edges,
                  CAST(NULL AS ARRAY<STRING>) AS path
           FROM (' || bfs_union_sql_1 || ') r
           CROSS JOIN bfs_frontier_1_0 f0';
      ELSE
        CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
        SELECT
          CAST(NULL AS STRING) AS start_node,
          CAST(NULL AS STRING) AS end_node,
          CAST(NULL AS INT) AS depth,
          CAST(NULL AS STRING) AS source_account_id,
          CAST(NULL AS STRING) AS target_account_id,
          CAST(NULL AS STRING) AS amount,
          CAST(NULL AS STRING) AS timestamp,
          CAST(NULL AS ARRAY<STRUCT<source_account_id: STRING, target_account_id: STRING, amount: STRING, timestamp: STRING>>) AS path_edges,
          CAST(NULL AS ARRAY<STRING>) AS path
        WHERE 1 = 0;
      END IF;
    
      SELECT 
       customer_id AS customer_id
      ,transfer_chains AS transfer_chains
      ,avg_chain_length AS avg_chain_length
    FROM (
      SELECT 
         _gsql2rsql_source_customer_id AS customer_id
        ,COUNT(DISTINCT _gsql2rsql_path_id) AS transfer_chains
        ,AVG(CAST((SIZE(_gsql2rsql_path_id) - 1) AS DOUBLE)) AS avg_chain_length
      FROM (
        SELECT
           sink.id AS _gsql2rsql_sink_id
          ,sink.balance AS _gsql2rsql_sink_balance
          ,sink.customer_id AS _gsql2rsql_sink_customer_id
          ,source.id AS _gsql2rsql_source_id
          ,source.balance AS _gsql2rsql_source_balance
          ,source.customer_id AS _gsql2rsql_source_customer_id
          ,p.start_node
          ,p.end_node
          ,p.depth
          ,p.path AS _gsql2rsql_path_id
          ,p.path_edges AS _gsql2rsql_path_edges
        FROM paths_1 p
        JOIN catalog.credit.Account sink
          ON sink.id = p.end_node
        JOIN catalog.credit.Account source
          ON source.id = p.start_node
        WHERE p.depth >= 1 AND p.depth <= 3
      ) AS _proj
      WHERE (_gsql2rsql_source_customer_id) = (_gsql2rsql_sink_customer_id)
      GROUP BY _gsql2rsql_source_customer_id
    ) AS _proj
    ORDER BY transfer_chains DESC
    LIMIT 20;
    END
    ```

---

## 2. Assess creditworthiness via social network analysis

**Source**: Credit

???+ note "OpenCypher Query"
    ```cypher
    MATCH (c:Customer)-[:KNOWS*1..2]-(peer:Customer)-[:HAS_LOAN]->(l:Loan)
    WHERE l.status = 'defaulted'
    WITH c, COUNT(DISTINCT peer) AS defaulted_peers, COUNT(DISTINCT l) AS defaulted_loans
    WHERE defaulted_peers > 0
    RETURN c.id, c.name, defaulted_peers, defaulted_loans,
           (defaulted_peers * 1.0) AS network_risk_score
    ORDER BY network_risk_score DESC
    ```

??? example "Procedural BFS — Databricks (temp_tables)"
    ```sql
    BEGIN
      DECLARE current_depth_1 INT DEFAULT 0;
      DECLARE rows_in_frontier_1 BIGINT DEFAULT 1;
    
      DROP TEMPORARY TABLE IF EXISTS bfs_visited_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_frontier_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_result_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_frontier_1_init;
      CREATE TEMPORARY TABLE bfs_visited_1 (node STRING);
      CREATE TEMPORARY TABLE bfs_frontier_1 AS
      SELECT n.id AS node
      FROM catalog.credit.Customer n;
      INSERT INTO bfs_visited_1
      SELECT node FROM bfs_frontier_1;
      CREATE TEMPORARY TABLE bfs_result_1 (customer_id STRING, knows_customer_id STRING, _next_node STRING, _bfs_depth INT);
      CREATE TEMPORARY TABLE bfs_frontier_1_init AS
      SELECT node FROM bfs_frontier_1;
      
      WHILE rows_in_frontier_1 > 0 AND current_depth_1 < 2 DO
        SET current_depth_1 = current_depth_1 + 1;
      
        DROP TEMPORARY TABLE IF EXISTS bfs_edges_1;
        CREATE TEMPORARY TABLE bfs_edges_1 AS
        SELECT e.customer_id, e.knows_customer_id, CASE WHEN f.node = e.customer_id THEN e.knows_customer_id ELSE e.customer_id END AS _next_node
      FROM catalog.credit.CustomerKnows e
      INNER JOIN bfs_frontier_1 f ON (e.customer_id = f.node OR e.knows_customer_id = f.node)
      WHERE CASE WHEN f.node = e.customer_id THEN e.knows_customer_id ELSE e.customer_id END NOT IN (SELECT node FROM bfs_visited_1);
      
        SET rows_in_frontier_1 = (SELECT COUNT(1) FROM bfs_edges_1);
      
        IF rows_in_frontier_1 > 0 THEN
          INSERT INTO bfs_visited_1
          SELECT DISTINCT _next_node FROM bfs_edges_1;
          DROP TEMPORARY TABLE bfs_frontier_1;
          CREATE TEMPORARY TABLE bfs_frontier_1 AS
          SELECT DISTINCT _next_node AS node FROM bfs_edges_1;
          INSERT INTO bfs_result_1
          SELECT *, current_depth_1 AS _bfs_depth FROM bfs_edges_1;
        END IF;
      END WHILE;
      
      CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
      SELECT f0.node AS start_node, r._next_node AS end_node, r._bfs_depth AS depth,
             r.customer_id, r.knows_customer_id
      FROM bfs_result_1 r
      CROSS JOIN bfs_frontier_1_init f0;
    
      SELECT 
       _gsql2rsql_c_id AS id
      ,_gsql2rsql_c_name AS name
      ,defaulted_peers AS defaulted_peers
      ,defaulted_loans AS defaulted_loans
      ,(defaulted_peers) * (1.0) AS network_risk_score
    FROM (
      SELECT 
         _gsql2rsql_c_id AS _gsql2rsql_c_id
        ,COUNT(DISTINCT _gsql2rsql_peer_id) AS defaulted_peers
        ,COUNT(DISTINCT _gsql2rsql_l_id) AS defaulted_loans
        ,_gsql2rsql_c_name AS _gsql2rsql_c_name
        ,_gsql2rsql_c_status AS _gsql2rsql_c_status
      FROM (
        SELECT
           _left_0._gsql2rsql_c_id AS _gsql2rsql_c_id
          ,_left_0._gsql2rsql_c_name AS _gsql2rsql_c_name
          ,_left_0._gsql2rsql_c_status AS _gsql2rsql_c_status
          ,_left_0._gsql2rsql_peer_id AS _gsql2rsql_peer_id
          ,_left_0._gsql2rsql__anon2_customer_id AS _gsql2rsql__anon2_customer_id
          ,_left_0._gsql2rsql__anon2_loan_id AS _gsql2rsql__anon2_loan_id
          ,_right_0._gsql2rsql_l_id AS _gsql2rsql_l_id
          ,_right_0._gsql2rsql_l_status AS _gsql2rsql_l_status
        FROM (
          SELECT
             _left_1._gsql2rsql_c_id AS _gsql2rsql_c_id
            ,_left_1._gsql2rsql_c_name AS _gsql2rsql_c_name
            ,_left_1._gsql2rsql_c_status AS _gsql2rsql_c_status
            ,_left_1._gsql2rsql_peer_id AS _gsql2rsql_peer_id
            ,_right_1._gsql2rsql__anon2_customer_id AS _gsql2rsql__anon2_customer_id
            ,_right_1._gsql2rsql__anon2_loan_id AS _gsql2rsql__anon2_loan_id
          FROM (
            SELECT
               sink.id AS _gsql2rsql_peer_id
              ,sink.name AS _gsql2rsql_peer_name
              ,sink.status AS _gsql2rsql_peer_status
              ,source.id AS _gsql2rsql_c_id
              ,source.name AS _gsql2rsql_c_name
              ,source.status AS _gsql2rsql_c_status
              ,p.start_node
              ,p.end_node
              ,p.depth
            FROM paths_1 p
            JOIN catalog.credit.Customer sink
              ON sink.id = p.end_node
            JOIN catalog.credit.Customer source
              ON source.id = p.start_node
            WHERE p.depth >= 1 AND p.depth <= 2
          ) AS _left_1
          INNER JOIN (
            SELECT
               customer_id AS _gsql2rsql__anon2_customer_id
              ,loan_id AS _gsql2rsql__anon2_loan_id
            FROM
              catalog.credit.CustomerLoan
          ) AS _right_1 ON
            _left_1._gsql2rsql_peer_id = _right_1._gsql2rsql__anon2_customer_id
        ) AS _left_0
        INNER JOIN (
          SELECT
             id AS _gsql2rsql_l_id
            ,status AS _gsql2rsql_l_status
          FROM
            catalog.credit.Loan
        ) AS _right_0 ON
          _right_0._gsql2rsql_l_id = _left_0._gsql2rsql__anon2_loan_id
      ) AS _proj
      WHERE (_gsql2rsql_l_status) = ('defaulted')
      GROUP BY _gsql2rsql_c_id, _gsql2rsql_c_name, _gsql2rsql_c_status
      HAVING (defaulted_peers) > (0)
    ) AS _proj
    ORDER BY network_risk_score DESC;
    END
    ```

??? example "Procedural BFS — PySpark 4.2 (numbered_views)"
    ```sql
    BEGIN
      DECLARE bfs_depth_1 INT DEFAULT 0;
      DECLARE bfs_frontier_count_1 BIGINT DEFAULT 1;
      DECLARE bfs_union_sql_1 STRING DEFAULT '';
    
      CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_1_0 AS
      SELECT n.id AS node
      FROM catalog.credit.Customer n;
      
      CREATE OR REPLACE TEMPORARY VIEW bfs_visited_1_0 AS
      SELECT node FROM bfs_frontier_1_0;
      
      WHILE bfs_frontier_count_1 > 0 AND bfs_depth_1 < 2 DO
        SET bfs_depth_1 = bfs_depth_1 + 1;
      
        EXECUTE IMMEDIATE
          'CREATE OR REPLACE TEMPORARY VIEW bfs_edges_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
           SELECT e.customer_id, e.knows_customer_id, CASE WHEN f.node = e.customer_id THEN e.knows_customer_id ELSE e.customer_id END AS _next_node, ' || CAST(bfs_depth_1 AS STRING) || ' AS _bfs_depth FROM catalog.credit.CustomerKnows e INNER JOIN bfs_frontier_1_' || CAST(bfs_depth_1 - 1 AS STRING) || ' f ON (e.customer_id = f.node OR e.knows_customer_id = f.node) WHERE CASE WHEN f.node = e.customer_id THEN e.knows_customer_id ELSE e.customer_id END NOT IN (SELECT node FROM bfs_visited_1_' || CAST(bfs_depth_1 - 1 AS STRING) || ')';
      
        EXECUTE IMMEDIATE
          'SET bfs_frontier_count_1 = (SELECT COUNT(1) FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING) || ')';
      
        IF bfs_frontier_count_1 > 0 THEN
      
          EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY VIEW bfs_visited_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
             SELECT node FROM bfs_visited_1_' || CAST(bfs_depth_1 - 1 AS STRING) || '
             UNION
             SELECT DISTINCT _next_node AS node
             FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
      
          EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
             SELECT DISTINCT _next_node AS node
             FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
      
          IF bfs_depth_1 >= 1 THEN
            IF bfs_union_sql_1 = '' THEN
              SET bfs_union_sql_1 =
                'SELECT customer_id, knows_customer_id, _next_node AS end_node, _bfs_depth AS depth FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
            ELSE
              SET bfs_union_sql_1 = bfs_union_sql_1
                || ' UNION ALL SELECT customer_id, knows_customer_id, _next_node AS end_node, _bfs_depth AS depth FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
            END IF;
          END IF;
      
        END IF;
      END WHILE;
      
      IF bfs_union_sql_1 != '' THEN
        EXECUTE IMMEDIATE
          'CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
           SELECT f0.node AS start_node, r.end_node, r.depth,
                  r.customer_id, r.knows_customer_id
           FROM (' || bfs_union_sql_1 || ') r
           CROSS JOIN bfs_frontier_1_0 f0';
      ELSE
        CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
        SELECT
          CAST(NULL AS STRING) AS start_node,
          CAST(NULL AS STRING) AS end_node,
          CAST(NULL AS INT) AS depth,
          CAST(NULL AS STRING) AS customer_id,
          CAST(NULL AS STRING) AS knows_customer_id
        WHERE 1 = 0;
      END IF;
    
      SELECT 
       _gsql2rsql_c_id AS id
      ,_gsql2rsql_c_name AS name
      ,defaulted_peers AS defaulted_peers
      ,defaulted_loans AS defaulted_loans
      ,(defaulted_peers) * (1.0) AS network_risk_score
    FROM (
      SELECT 
         _gsql2rsql_c_id AS _gsql2rsql_c_id
        ,COUNT(DISTINCT _gsql2rsql_peer_id) AS defaulted_peers
        ,COUNT(DISTINCT _gsql2rsql_l_id) AS defaulted_loans
        ,_gsql2rsql_c_name AS _gsql2rsql_c_name
        ,_gsql2rsql_c_status AS _gsql2rsql_c_status
      FROM (
        SELECT
           _left_0._gsql2rsql_c_id AS _gsql2rsql_c_id
          ,_left_0._gsql2rsql_c_name AS _gsql2rsql_c_name
          ,_left_0._gsql2rsql_c_status AS _gsql2rsql_c_status
          ,_left_0._gsql2rsql_peer_id AS _gsql2rsql_peer_id
          ,_left_0._gsql2rsql__anon2_customer_id AS _gsql2rsql__anon2_customer_id
          ,_left_0._gsql2rsql__anon2_loan_id AS _gsql2rsql__anon2_loan_id
          ,_right_0._gsql2rsql_l_id AS _gsql2rsql_l_id
          ,_right_0._gsql2rsql_l_status AS _gsql2rsql_l_status
        FROM (
          SELECT
             _left_1._gsql2rsql_c_id AS _gsql2rsql_c_id
            ,_left_1._gsql2rsql_c_name AS _gsql2rsql_c_name
            ,_left_1._gsql2rsql_c_status AS _gsql2rsql_c_status
            ,_left_1._gsql2rsql_peer_id AS _gsql2rsql_peer_id
            ,_right_1._gsql2rsql__anon2_customer_id AS _gsql2rsql__anon2_customer_id
            ,_right_1._gsql2rsql__anon2_loan_id AS _gsql2rsql__anon2_loan_id
          FROM (
            SELECT
               sink.id AS _gsql2rsql_peer_id
              ,sink.name AS _gsql2rsql_peer_name
              ,sink.status AS _gsql2rsql_peer_status
              ,source.id AS _gsql2rsql_c_id
              ,source.name AS _gsql2rsql_c_name
              ,source.status AS _gsql2rsql_c_status
              ,p.start_node
              ,p.end_node
              ,p.depth
            FROM paths_1 p
            JOIN catalog.credit.Customer sink
              ON sink.id = p.end_node
            JOIN catalog.credit.Customer source
              ON source.id = p.start_node
            WHERE p.depth >= 1 AND p.depth <= 2
          ) AS _left_1
          INNER JOIN (
            SELECT
               customer_id AS _gsql2rsql__anon2_customer_id
              ,loan_id AS _gsql2rsql__anon2_loan_id
            FROM
              catalog.credit.CustomerLoan
          ) AS _right_1 ON
            _left_1._gsql2rsql_peer_id = _right_1._gsql2rsql__anon2_customer_id
        ) AS _left_0
        INNER JOIN (
          SELECT
             id AS _gsql2rsql_l_id
            ,status AS _gsql2rsql_l_status
          FROM
            catalog.credit.Loan
        ) AS _right_0 ON
          _right_0._gsql2rsql_l_id = _left_0._gsql2rsql__anon2_loan_id
      ) AS _proj
      WHERE (_gsql2rsql_l_status) = ('defaulted')
      GROUP BY _gsql2rsql_c_id, _gsql2rsql_c_name, _gsql2rsql_c_status
      HAVING (defaulted_peers) > (0)
    ) AS _proj
    ORDER BY network_risk_score DESC;
    END
    ```

---

## 3. Variable-length path traversal (1 to 3 hops)

**Source**: Features

???+ note "OpenCypher Query"
    ```cypher
    MATCH (p:Person)-[:KNOWS*1..3]->(f:Person)
    WHERE p.name = 'Alice'
    RETURN DISTINCT f.name AS reachable
    ```

??? example "Procedural BFS — Databricks (temp_tables)"
    ```sql
    BEGIN
      DECLARE current_depth_1 INT DEFAULT 0;
      DECLARE rows_in_frontier_1 BIGINT DEFAULT 1;
    
      DROP TEMPORARY TABLE IF EXISTS bfs_visited_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_frontier_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_result_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_frontier_1_init;
      CREATE TEMPORARY TABLE bfs_visited_1 (node STRING);
      CREATE TEMPORARY TABLE bfs_frontier_1 AS
      SELECT n.id AS node
      FROM catalog.demo.Person n
      WHERE (n.name) = ('Alice');
      INSERT INTO bfs_visited_1
      SELECT node FROM bfs_frontier_1;
      CREATE TEMPORARY TABLE bfs_result_1 (person_id STRING, friend_id STRING, since STRING, strength STRING, _next_node STRING, _bfs_depth INT);
      CREATE TEMPORARY TABLE bfs_frontier_1_init AS
      SELECT node FROM bfs_frontier_1;
      
      WHILE rows_in_frontier_1 > 0 AND current_depth_1 < 3 DO
        SET current_depth_1 = current_depth_1 + 1;
      
        DROP TEMPORARY TABLE IF EXISTS bfs_edges_1;
        CREATE TEMPORARY TABLE bfs_edges_1 AS
        SELECT e.person_id, e.friend_id, e.since, e.strength, e.friend_id AS _next_node
      FROM catalog.demo.Knows e
      INNER JOIN bfs_frontier_1 f ON e.person_id = f.node
      WHERE e.friend_id NOT IN (SELECT node FROM bfs_visited_1);
      
        SET rows_in_frontier_1 = (SELECT COUNT(1) FROM bfs_edges_1);
      
        IF rows_in_frontier_1 > 0 THEN
          INSERT INTO bfs_visited_1
          SELECT DISTINCT _next_node FROM bfs_edges_1;
          DROP TEMPORARY TABLE bfs_frontier_1;
          CREATE TEMPORARY TABLE bfs_frontier_1 AS
          SELECT DISTINCT _next_node AS node FROM bfs_edges_1;
          INSERT INTO bfs_result_1
          SELECT *, current_depth_1 AS _bfs_depth FROM bfs_edges_1;
        END IF;
      END WHILE;
      
      CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
      SELECT f0.node AS start_node, r._next_node AS end_node, r._bfs_depth AS depth,
             r.person_id, r.friend_id, r.since, r.strength
      FROM bfs_result_1 r
      CROSS JOIN bfs_frontier_1_init f0;
    
      SELECT 
       FIRST(_gsql2rsql_f_name) AS reachable
    FROM (
      SELECT
         sink.id AS _gsql2rsql_f_id
        ,sink.name AS _gsql2rsql_f_name
        ,sink.age AS _gsql2rsql_f_age
        ,sink.nickname AS _gsql2rsql_f_nickname
        ,sink.salary AS _gsql2rsql_f_salary
        ,sink.active AS _gsql2rsql_f_active
        ,p.start_node
        ,p.end_node
        ,p.depth
      FROM paths_1 p
      JOIN catalog.demo.Person sink
        ON sink.id = p.end_node
      WHERE p.depth >= 1 AND p.depth <= 3
    ) AS _proj
    GROUP BY TO_JSON(NAMED_STRUCT('_', _gsql2rsql_f_name));
    END
    ```

??? example "Procedural BFS — PySpark 4.2 (numbered_views)"
    ```sql
    BEGIN
      DECLARE bfs_depth_1 INT DEFAULT 0;
      DECLARE bfs_frontier_count_1 BIGINT DEFAULT 1;
      DECLARE bfs_union_sql_1 STRING DEFAULT '';
    
      CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_1_0 AS
      SELECT n.id AS node
      FROM catalog.demo.Person n
      WHERE (n.name) = ('Alice');
      
      CREATE OR REPLACE TEMPORARY VIEW bfs_visited_1_0 AS
      SELECT node FROM bfs_frontier_1_0;
      
      WHILE bfs_frontier_count_1 > 0 AND bfs_depth_1 < 3 DO
        SET bfs_depth_1 = bfs_depth_1 + 1;
      
        EXECUTE IMMEDIATE
          'CREATE OR REPLACE TEMPORARY VIEW bfs_edges_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
           SELECT e.person_id, e.friend_id, e.since, e.strength, e.friend_id AS _next_node, ' || CAST(bfs_depth_1 AS STRING) || ' AS _bfs_depth FROM catalog.demo.Knows e INNER JOIN bfs_frontier_1_' || CAST(bfs_depth_1 - 1 AS STRING) || ' f ON e.person_id = f.node WHERE e.friend_id NOT IN (SELECT node FROM bfs_visited_1_' || CAST(bfs_depth_1 - 1 AS STRING) || ')';
      
        EXECUTE IMMEDIATE
          'SET bfs_frontier_count_1 = (SELECT COUNT(1) FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING) || ')';
      
        IF bfs_frontier_count_1 > 0 THEN
      
          EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY VIEW bfs_visited_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
             SELECT node FROM bfs_visited_1_' || CAST(bfs_depth_1 - 1 AS STRING) || '
             UNION
             SELECT DISTINCT _next_node AS node
             FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
      
          EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
             SELECT DISTINCT _next_node AS node
             FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
      
          IF bfs_depth_1 >= 1 THEN
            IF bfs_union_sql_1 = '' THEN
              SET bfs_union_sql_1 =
                'SELECT person_id, friend_id, since, strength, _next_node AS end_node, _bfs_depth AS depth FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
            ELSE
              SET bfs_union_sql_1 = bfs_union_sql_1
                || ' UNION ALL SELECT person_id, friend_id, since, strength, _next_node AS end_node, _bfs_depth AS depth FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
            END IF;
          END IF;
      
        END IF;
      END WHILE;
      
      IF bfs_union_sql_1 != '' THEN
        EXECUTE IMMEDIATE
          'CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
           SELECT f0.node AS start_node, r.end_node, r.depth,
                  r.person_id, r.friend_id, r.since, r.strength
           FROM (' || bfs_union_sql_1 || ') r
           CROSS JOIN bfs_frontier_1_0 f0';
      ELSE
        CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
        SELECT
          CAST(NULL AS STRING) AS start_node,
          CAST(NULL AS STRING) AS end_node,
          CAST(NULL AS INT) AS depth,
          CAST(NULL AS STRING) AS person_id,
          CAST(NULL AS STRING) AS friend_id,
          CAST(NULL AS STRING) AS since,
          CAST(NULL AS STRING) AS strength
        WHERE 1 = 0;
      END IF;
    
      SELECT 
       FIRST(_gsql2rsql_f_name) AS reachable
    FROM (
      SELECT
         sink.id AS _gsql2rsql_f_id
        ,sink.name AS _gsql2rsql_f_name
        ,sink.age AS _gsql2rsql_f_age
        ,sink.nickname AS _gsql2rsql_f_nickname
        ,sink.salary AS _gsql2rsql_f_salary
        ,sink.active AS _gsql2rsql_f_active
        ,p.start_node
        ,p.end_node
        ,p.depth
      FROM paths_1 p
      JOIN catalog.demo.Person sink
        ON sink.id = p.end_node
      WHERE p.depth >= 1 AND p.depth <= 3
    ) AS _proj
    GROUP BY TO_JSON(NAMED_STRUCT('_', _gsql2rsql_f_name));
    END
    ```

---

## 4. Variable-length path with zero-length (includes self)

**Source**: Features

???+ note "OpenCypher Query"
    ```cypher
    MATCH (p:Person)-[:KNOWS*0..2]->(f:Person)
    WHERE p.name = 'Alice'
    RETURN DISTINCT f.name AS reachable
    ```

??? example "Procedural BFS — Databricks (temp_tables)"
    ```sql
    BEGIN
      DECLARE current_depth_1 INT DEFAULT 0;
      DECLARE rows_in_frontier_1 BIGINT DEFAULT 1;
    
      DROP TEMPORARY TABLE IF EXISTS bfs_visited_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_frontier_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_result_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_frontier_1_init;
      CREATE TEMPORARY TABLE bfs_visited_1 (node STRING);
      CREATE TEMPORARY TABLE bfs_frontier_1 AS
      SELECT n.id AS node
      FROM catalog.demo.Person n
      WHERE (n.name) = ('Alice');
      INSERT INTO bfs_visited_1
      SELECT node FROM bfs_frontier_1;
      CREATE TEMPORARY TABLE bfs_result_1 (person_id STRING, friend_id STRING, since STRING, strength STRING, _next_node STRING, _bfs_depth INT);
      CREATE TEMPORARY TABLE bfs_frontier_1_init AS
      SELECT node FROM bfs_frontier_1;
      
      WHILE rows_in_frontier_1 > 0 AND current_depth_1 < 2 DO
        SET current_depth_1 = current_depth_1 + 1;
      
        DROP TEMPORARY TABLE IF EXISTS bfs_edges_1;
        CREATE TEMPORARY TABLE bfs_edges_1 AS
        SELECT e.person_id, e.friend_id, e.since, e.strength, e.friend_id AS _next_node
      FROM catalog.demo.Knows e
      INNER JOIN bfs_frontier_1 f ON e.person_id = f.node
      WHERE e.friend_id NOT IN (SELECT node FROM bfs_visited_1);
      
        SET rows_in_frontier_1 = (SELECT COUNT(1) FROM bfs_edges_1);
      
        IF rows_in_frontier_1 > 0 THEN
          INSERT INTO bfs_visited_1
          SELECT DISTINCT _next_node FROM bfs_edges_1;
          DROP TEMPORARY TABLE bfs_frontier_1;
          CREATE TEMPORARY TABLE bfs_frontier_1 AS
          SELECT DISTINCT _next_node AS node FROM bfs_edges_1;
          INSERT INTO bfs_result_1
          SELECT *, current_depth_1 AS _bfs_depth FROM bfs_edges_1;
        END IF;
      END WHILE;
      
      CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
      SELECT f0.node AS start_node, r._next_node AS end_node, r._bfs_depth AS depth,
             r.person_id, r.friend_id, r.since, r.strength
      FROM bfs_result_1 r
      CROSS JOIN bfs_frontier_1_init f0;
    
      SELECT 
       FIRST(_gsql2rsql_f_name) AS reachable
    FROM (
      SELECT
         sink.id AS _gsql2rsql_f_id
        ,sink.name AS _gsql2rsql_f_name
        ,sink.age AS _gsql2rsql_f_age
        ,sink.nickname AS _gsql2rsql_f_nickname
        ,sink.salary AS _gsql2rsql_f_salary
        ,sink.active AS _gsql2rsql_f_active
        ,p.start_node
        ,p.end_node
        ,p.depth
      FROM paths_1 p
      JOIN catalog.demo.Person sink
        ON sink.id = p.end_node
      WHERE p.depth >= 0 AND p.depth <= 2
    ) AS _proj
    GROUP BY TO_JSON(NAMED_STRUCT('_', _gsql2rsql_f_name));
    END
    ```

??? example "Procedural BFS — PySpark 4.2 (numbered_views)"
    ```sql
    BEGIN
      DECLARE bfs_depth_1 INT DEFAULT 0;
      DECLARE bfs_frontier_count_1 BIGINT DEFAULT 1;
      DECLARE bfs_union_sql_1 STRING DEFAULT '';
    
      CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_1_0 AS
      SELECT n.id AS node
      FROM catalog.demo.Person n
      WHERE (n.name) = ('Alice');
      
      CREATE OR REPLACE TEMPORARY VIEW bfs_visited_1_0 AS
      SELECT node FROM bfs_frontier_1_0;
      
      WHILE bfs_frontier_count_1 > 0 AND bfs_depth_1 < 2 DO
        SET bfs_depth_1 = bfs_depth_1 + 1;
      
        EXECUTE IMMEDIATE
          'CREATE OR REPLACE TEMPORARY VIEW bfs_edges_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
           SELECT e.person_id, e.friend_id, e.since, e.strength, e.friend_id AS _next_node, ' || CAST(bfs_depth_1 AS STRING) || ' AS _bfs_depth FROM catalog.demo.Knows e INNER JOIN bfs_frontier_1_' || CAST(bfs_depth_1 - 1 AS STRING) || ' f ON e.person_id = f.node WHERE e.friend_id NOT IN (SELECT node FROM bfs_visited_1_' || CAST(bfs_depth_1 - 1 AS STRING) || ')';
      
        EXECUTE IMMEDIATE
          'SET bfs_frontier_count_1 = (SELECT COUNT(1) FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING) || ')';
      
        IF bfs_frontier_count_1 > 0 THEN
      
          EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY VIEW bfs_visited_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
             SELECT node FROM bfs_visited_1_' || CAST(bfs_depth_1 - 1 AS STRING) || '
             UNION
             SELECT DISTINCT _next_node AS node
             FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
      
          EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
             SELECT DISTINCT _next_node AS node
             FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
      
          IF bfs_depth_1 >= 0 THEN
            IF bfs_union_sql_1 = '' THEN
              SET bfs_union_sql_1 =
                'SELECT person_id, friend_id, since, strength, _next_node AS end_node, _bfs_depth AS depth FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
            ELSE
              SET bfs_union_sql_1 = bfs_union_sql_1
                || ' UNION ALL SELECT person_id, friend_id, since, strength, _next_node AS end_node, _bfs_depth AS depth FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
            END IF;
          END IF;
      
        END IF;
      END WHILE;
      
      IF bfs_union_sql_1 != '' THEN
        EXECUTE IMMEDIATE
          'CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
           SELECT f0.node AS start_node, r.end_node, r.depth,
                  r.person_id, r.friend_id, r.since, r.strength
           FROM (' || bfs_union_sql_1 || ') r
           CROSS JOIN bfs_frontier_1_0 f0';
      ELSE
        CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
        SELECT
          CAST(NULL AS STRING) AS start_node,
          CAST(NULL AS STRING) AS end_node,
          CAST(NULL AS INT) AS depth,
          CAST(NULL AS STRING) AS person_id,
          CAST(NULL AS STRING) AS friend_id,
          CAST(NULL AS STRING) AS since,
          CAST(NULL AS STRING) AS strength
        WHERE 1 = 0;
      END IF;
    
      SELECT 
       FIRST(_gsql2rsql_f_name) AS reachable
    FROM (
      SELECT
         sink.id AS _gsql2rsql_f_id
        ,sink.name AS _gsql2rsql_f_name
        ,sink.age AS _gsql2rsql_f_age
        ,sink.nickname AS _gsql2rsql_f_nickname
        ,sink.salary AS _gsql2rsql_f_salary
        ,sink.active AS _gsql2rsql_f_active
        ,p.start_node
        ,p.end_node
        ,p.depth
      FROM paths_1 p
      JOIN catalog.demo.Person sink
        ON sink.id = p.end_node
      WHERE p.depth >= 0 AND p.depth <= 2
    ) AS _proj
    GROUP BY TO_JSON(NAMED_STRUCT('_', _gsql2rsql_f_name));
    END
    ```

---

## 5. Simplest sink filter pushdown

**Source**: Features

???+ note "OpenCypher Query"
    ```cypher
    MATCH (a:Person)-[:KNOWS*1..2]->(b:Person)
    WHERE b.age > 30
    RETURN a.name, b.name
    ```

??? example "Procedural BFS — Databricks (temp_tables)"
    ```sql
    BEGIN
      DECLARE current_depth_1 INT DEFAULT 0;
      DECLARE rows_in_frontier_1 BIGINT DEFAULT 1;
    
      DROP TEMPORARY TABLE IF EXISTS bfs_visited_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_frontier_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_result_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_frontier_1_init;
      CREATE TEMPORARY TABLE bfs_visited_1 (node STRING);
      CREATE TEMPORARY TABLE bfs_frontier_1 AS
      SELECT n.id AS node
      FROM catalog.demo.Person n;
      INSERT INTO bfs_visited_1
      SELECT node FROM bfs_frontier_1;
      CREATE TEMPORARY TABLE bfs_result_1 (person_id STRING, friend_id STRING, since STRING, strength STRING, _next_node STRING, _bfs_depth INT);
      CREATE TEMPORARY TABLE bfs_frontier_1_init AS
      SELECT node FROM bfs_frontier_1;
      
      WHILE rows_in_frontier_1 > 0 AND current_depth_1 < 2 DO
        SET current_depth_1 = current_depth_1 + 1;
      
        DROP TEMPORARY TABLE IF EXISTS bfs_edges_1;
        CREATE TEMPORARY TABLE bfs_edges_1 AS
        SELECT e.person_id, e.friend_id, e.since, e.strength, e.friend_id AS _next_node
      FROM catalog.demo.Knows e
      INNER JOIN bfs_frontier_1 f ON e.person_id = f.node
      WHERE e.friend_id NOT IN (SELECT node FROM bfs_visited_1);
      
        SET rows_in_frontier_1 = (SELECT COUNT(1) FROM bfs_edges_1);
      
        IF rows_in_frontier_1 > 0 THEN
          INSERT INTO bfs_visited_1
          SELECT DISTINCT _next_node FROM bfs_edges_1;
          DROP TEMPORARY TABLE bfs_frontier_1;
          CREATE TEMPORARY TABLE bfs_frontier_1 AS
          SELECT DISTINCT _next_node AS node FROM bfs_edges_1;
          INSERT INTO bfs_result_1
          SELECT *, current_depth_1 AS _bfs_depth FROM bfs_edges_1;
        END IF;
      END WHILE;
      
      CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
      SELECT f0.node AS start_node, r._next_node AS end_node, r._bfs_depth AS depth,
             r.person_id, r.friend_id, r.since, r.strength
      FROM bfs_result_1 r
      CROSS JOIN bfs_frontier_1_init f0;
    
      SELECT 
       _gsql2rsql_a_name AS name
      ,_gsql2rsql_b_name AS name
    FROM (
      SELECT
         sink.id AS _gsql2rsql_b_id
        ,sink.name AS _gsql2rsql_b_name
        ,sink.age AS _gsql2rsql_b_age
        ,sink.nickname AS _gsql2rsql_b_nickname
        ,sink.salary AS _gsql2rsql_b_salary
        ,sink.active AS _gsql2rsql_b_active
        ,source.id AS _gsql2rsql_a_id
        ,source.name AS _gsql2rsql_a_name
        ,source.age AS _gsql2rsql_a_age
        ,source.nickname AS _gsql2rsql_a_nickname
        ,source.salary AS _gsql2rsql_a_salary
        ,source.active AS _gsql2rsql_a_active
        ,p.start_node
        ,p.end_node
        ,p.depth
      FROM paths_1 p
      JOIN catalog.demo.Person sink
        ON sink.id = p.end_node
      JOIN catalog.demo.Person source
        ON source.id = p.start_node
      WHERE p.depth >= 1 AND p.depth <= 2 AND (sink.age) > (30)
    ) AS _proj;
    END
    ```

??? example "Procedural BFS — PySpark 4.2 (numbered_views)"
    ```sql
    BEGIN
      DECLARE bfs_depth_1 INT DEFAULT 0;
      DECLARE bfs_frontier_count_1 BIGINT DEFAULT 1;
      DECLARE bfs_union_sql_1 STRING DEFAULT '';
    
      CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_1_0 AS
      SELECT n.id AS node
      FROM catalog.demo.Person n;
      
      CREATE OR REPLACE TEMPORARY VIEW bfs_visited_1_0 AS
      SELECT node FROM bfs_frontier_1_0;
      
      WHILE bfs_frontier_count_1 > 0 AND bfs_depth_1 < 2 DO
        SET bfs_depth_1 = bfs_depth_1 + 1;
      
        EXECUTE IMMEDIATE
          'CREATE OR REPLACE TEMPORARY VIEW bfs_edges_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
           SELECT e.person_id, e.friend_id, e.since, e.strength, e.friend_id AS _next_node, ' || CAST(bfs_depth_1 AS STRING) || ' AS _bfs_depth FROM catalog.demo.Knows e INNER JOIN bfs_frontier_1_' || CAST(bfs_depth_1 - 1 AS STRING) || ' f ON e.person_id = f.node WHERE e.friend_id NOT IN (SELECT node FROM bfs_visited_1_' || CAST(bfs_depth_1 - 1 AS STRING) || ')';
      
        EXECUTE IMMEDIATE
          'SET bfs_frontier_count_1 = (SELECT COUNT(1) FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING) || ')';
      
        IF bfs_frontier_count_1 > 0 THEN
      
          EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY VIEW bfs_visited_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
             SELECT node FROM bfs_visited_1_' || CAST(bfs_depth_1 - 1 AS STRING) || '
             UNION
             SELECT DISTINCT _next_node AS node
             FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
      
          EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
             SELECT DISTINCT _next_node AS node
             FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
      
          IF bfs_depth_1 >= 1 THEN
            IF bfs_union_sql_1 = '' THEN
              SET bfs_union_sql_1 =
                'SELECT person_id, friend_id, since, strength, _next_node AS end_node, _bfs_depth AS depth FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
            ELSE
              SET bfs_union_sql_1 = bfs_union_sql_1
                || ' UNION ALL SELECT person_id, friend_id, since, strength, _next_node AS end_node, _bfs_depth AS depth FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
            END IF;
          END IF;
      
        END IF;
      END WHILE;
      
      IF bfs_union_sql_1 != '' THEN
        EXECUTE IMMEDIATE
          'CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
           SELECT f0.node AS start_node, r.end_node, r.depth,
                  r.person_id, r.friend_id, r.since, r.strength
           FROM (' || bfs_union_sql_1 || ') r
           CROSS JOIN bfs_frontier_1_0 f0';
      ELSE
        CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
        SELECT
          CAST(NULL AS STRING) AS start_node,
          CAST(NULL AS STRING) AS end_node,
          CAST(NULL AS INT) AS depth,
          CAST(NULL AS STRING) AS person_id,
          CAST(NULL AS STRING) AS friend_id,
          CAST(NULL AS STRING) AS since,
          CAST(NULL AS STRING) AS strength
        WHERE 1 = 0;
      END IF;
    
      SELECT 
       _gsql2rsql_a_name AS name
      ,_gsql2rsql_b_name AS name
    FROM (
      SELECT
         sink.id AS _gsql2rsql_b_id
        ,sink.name AS _gsql2rsql_b_name
        ,sink.age AS _gsql2rsql_b_age
        ,sink.nickname AS _gsql2rsql_b_nickname
        ,sink.salary AS _gsql2rsql_b_salary
        ,sink.active AS _gsql2rsql_b_active
        ,source.id AS _gsql2rsql_a_id
        ,source.name AS _gsql2rsql_a_name
        ,source.age AS _gsql2rsql_a_age
        ,source.nickname AS _gsql2rsql_a_nickname
        ,source.salary AS _gsql2rsql_a_salary
        ,source.active AS _gsql2rsql_a_active
        ,p.start_node
        ,p.end_node
        ,p.depth
      FROM paths_1 p
      JOIN catalog.demo.Person sink
        ON sink.id = p.end_node
      JOIN catalog.demo.Person source
        ON source.id = p.start_node
      WHERE p.depth >= 1 AND p.depth <= 2 AND (sink.age) > (30)
    ) AS _proj;
    END
    ```

---

## 6. Variable-length path with sink filter pushdown

**Source**: Features

???+ note "OpenCypher Query"
    ```cypher
    MATCH path = (a:Person)-[:KNOWS*2..4]->(b:Person)
    WHERE b.age > 50
    RETURN a.id, b.id, LENGTH(path) AS chain_length
    ```

??? example "Procedural BFS — Databricks (temp_tables)"
    ```sql
    BEGIN
      DECLARE current_depth_1 INT DEFAULT 0;
      DECLARE rows_in_frontier_1 BIGINT DEFAULT 1;
    
      DROP TEMPORARY TABLE IF EXISTS bfs_visited_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_frontier_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_result_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_frontier_1_init;
      CREATE TEMPORARY TABLE bfs_visited_1 (node STRING);
      CREATE TEMPORARY TABLE bfs_frontier_1 AS
      SELECT n.id AS node
      FROM catalog.demo.Person n;
      INSERT INTO bfs_visited_1
      SELECT node FROM bfs_frontier_1;
      CREATE TEMPORARY TABLE bfs_result_1 (person_id STRING, friend_id STRING, since STRING, strength STRING, _next_node STRING, _bfs_depth INT);
      CREATE TEMPORARY TABLE bfs_frontier_1_init AS
      SELECT node FROM bfs_frontier_1;
      
      WHILE rows_in_frontier_1 > 0 AND current_depth_1 < 4 DO
        SET current_depth_1 = current_depth_1 + 1;
      
        DROP TEMPORARY TABLE IF EXISTS bfs_edges_1;
        CREATE TEMPORARY TABLE bfs_edges_1 AS
        SELECT e.person_id, e.friend_id, e.since, e.strength, e.friend_id AS _next_node
      FROM catalog.demo.Knows e
      INNER JOIN bfs_frontier_1 f ON e.person_id = f.node
      WHERE e.friend_id NOT IN (SELECT node FROM bfs_visited_1);
      
        SET rows_in_frontier_1 = (SELECT COUNT(1) FROM bfs_edges_1);
      
        IF rows_in_frontier_1 > 0 THEN
          INSERT INTO bfs_visited_1
          SELECT DISTINCT _next_node FROM bfs_edges_1;
          DROP TEMPORARY TABLE bfs_frontier_1;
          CREATE TEMPORARY TABLE bfs_frontier_1 AS
          SELECT DISTINCT _next_node AS node FROM bfs_edges_1;
          IF current_depth_1 >= 2 THEN
            INSERT INTO bfs_result_1
            SELECT *, current_depth_1 AS _bfs_depth FROM bfs_edges_1;
          END IF;
        END IF;
      END WHILE;
      
      CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
      SELECT f0.node AS start_node, r._next_node AS end_node, r._bfs_depth AS depth,
             r.person_id, r.friend_id, r.since, r.strength,
             ARRAY(NAMED_STRUCT('person_id', r.person_id, 'friend_id', r.friend_id, 'since', r.since, 'strength', r.strength)) AS path_edges,
             CAST(NULL AS ARRAY<STRING>) AS path
      FROM bfs_result_1 r
      CROSS JOIN bfs_frontier_1_init f0;
    
      SELECT 
       _gsql2rsql_a_id AS id
      ,_gsql2rsql_b_id AS id
      ,(SIZE(_gsql2rsql_path_id) - 1) AS chain_length
    FROM (
      SELECT
         sink.id AS _gsql2rsql_b_id
        ,sink.name AS _gsql2rsql_b_name
        ,sink.age AS _gsql2rsql_b_age
        ,sink.nickname AS _gsql2rsql_b_nickname
        ,sink.salary AS _gsql2rsql_b_salary
        ,sink.active AS _gsql2rsql_b_active
        ,source.id AS _gsql2rsql_a_id
        ,source.name AS _gsql2rsql_a_name
        ,source.age AS _gsql2rsql_a_age
        ,source.nickname AS _gsql2rsql_a_nickname
        ,source.salary AS _gsql2rsql_a_salary
        ,source.active AS _gsql2rsql_a_active
        ,p.start_node
        ,p.end_node
        ,p.depth
        ,p.path AS _gsql2rsql_path_id
        ,p.path_edges AS _gsql2rsql_path_edges
      FROM paths_1 p
      JOIN catalog.demo.Person sink
        ON sink.id = p.end_node
      JOIN catalog.demo.Person source
        ON source.id = p.start_node
      WHERE p.depth >= 2 AND p.depth <= 4 AND (sink.age) > (50)
    ) AS _proj;
    END
    ```

??? example "Procedural BFS — PySpark 4.2 (numbered_views)"
    ```sql
    BEGIN
      DECLARE bfs_depth_1 INT DEFAULT 0;
      DECLARE bfs_frontier_count_1 BIGINT DEFAULT 1;
      DECLARE bfs_union_sql_1 STRING DEFAULT '';
    
      CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_1_0 AS
      SELECT n.id AS node
      FROM catalog.demo.Person n;
      
      CREATE OR REPLACE TEMPORARY VIEW bfs_visited_1_0 AS
      SELECT node FROM bfs_frontier_1_0;
      
      WHILE bfs_frontier_count_1 > 0 AND bfs_depth_1 < 4 DO
        SET bfs_depth_1 = bfs_depth_1 + 1;
      
        EXECUTE IMMEDIATE
          'CREATE OR REPLACE TEMPORARY VIEW bfs_edges_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
           SELECT e.person_id, e.friend_id, e.since, e.strength, e.friend_id AS _next_node, ' || CAST(bfs_depth_1 AS STRING) || ' AS _bfs_depth FROM catalog.demo.Knows e INNER JOIN bfs_frontier_1_' || CAST(bfs_depth_1 - 1 AS STRING) || ' f ON e.person_id = f.node WHERE e.friend_id NOT IN (SELECT node FROM bfs_visited_1_' || CAST(bfs_depth_1 - 1 AS STRING) || ')';
      
        EXECUTE IMMEDIATE
          'SET bfs_frontier_count_1 = (SELECT COUNT(1) FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING) || ')';
      
        IF bfs_frontier_count_1 > 0 THEN
      
          EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY VIEW bfs_visited_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
             SELECT node FROM bfs_visited_1_' || CAST(bfs_depth_1 - 1 AS STRING) || '
             UNION
             SELECT DISTINCT _next_node AS node
             FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
      
          EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
             SELECT DISTINCT _next_node AS node
             FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
      
          IF bfs_depth_1 >= 2 THEN
            IF bfs_union_sql_1 = '' THEN
              SET bfs_union_sql_1 =
                'SELECT person_id, friend_id, since, strength, _next_node AS end_node, _bfs_depth AS depth FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
            ELSE
              SET bfs_union_sql_1 = bfs_union_sql_1
                || ' UNION ALL SELECT person_id, friend_id, since, strength, _next_node AS end_node, _bfs_depth AS depth FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
            END IF;
          END IF;
      
        END IF;
      END WHILE;
      
      IF bfs_union_sql_1 != '' THEN
        EXECUTE IMMEDIATE
          'CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
           SELECT f0.node AS start_node, r.end_node, r.depth,
                  r.person_id, r.friend_id, r.since, r.strength,
                  ARRAY(NAMED_STRUCT(''person_id'', r.person_id, ''friend_id'', r.friend_id, ''since'', r.since, ''strength'', r.strength)) AS path_edges,
                  CAST(NULL AS ARRAY<STRING>) AS path
           FROM (' || bfs_union_sql_1 || ') r
           CROSS JOIN bfs_frontier_1_0 f0';
      ELSE
        CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
        SELECT
          CAST(NULL AS STRING) AS start_node,
          CAST(NULL AS STRING) AS end_node,
          CAST(NULL AS INT) AS depth,
          CAST(NULL AS STRING) AS person_id,
          CAST(NULL AS STRING) AS friend_id,
          CAST(NULL AS STRING) AS since,
          CAST(NULL AS STRING) AS strength,
          CAST(NULL AS ARRAY<STRUCT<person_id: STRING, friend_id: STRING, since: STRING, strength: STRING>>) AS path_edges,
          CAST(NULL AS ARRAY<STRING>) AS path
        WHERE 1 = 0;
      END IF;
    
      SELECT 
       _gsql2rsql_a_id AS id
      ,_gsql2rsql_b_id AS id
      ,(SIZE(_gsql2rsql_path_id) - 1) AS chain_length
    FROM (
      SELECT
         sink.id AS _gsql2rsql_b_id
        ,sink.name AS _gsql2rsql_b_name
        ,sink.age AS _gsql2rsql_b_age
        ,sink.nickname AS _gsql2rsql_b_nickname
        ,sink.salary AS _gsql2rsql_b_salary
        ,sink.active AS _gsql2rsql_b_active
        ,source.id AS _gsql2rsql_a_id
        ,source.name AS _gsql2rsql_a_name
        ,source.age AS _gsql2rsql_a_age
        ,source.nickname AS _gsql2rsql_a_nickname
        ,source.salary AS _gsql2rsql_a_salary
        ,source.active AS _gsql2rsql_a_active
        ,p.start_node
        ,p.end_node
        ,p.depth
        ,p.path AS _gsql2rsql_path_id
        ,p.path_edges AS _gsql2rsql_path_edges
      FROM paths_1 p
      JOIN catalog.demo.Person sink
        ON sink.id = p.end_node
      JOIN catalog.demo.Person source
        ON source.id = p.start_node
      WHERE p.depth >= 2 AND p.depth <= 4 AND (sink.age) > (50)
    ) AS _proj;
    END
    ```

---

## 7. Variable-length with source AND sink filter pushdown

**Source**: Features

???+ note "OpenCypher Query"
    ```cypher
    MATCH path = (a:Person)-[:KNOWS*2..4]->(b:Person)
    WHERE a.age > 30 AND b.age > 50
    RETURN a.id, b.id
    ```

??? example "Procedural BFS — Databricks (temp_tables)"
    ```sql
    BEGIN
      DECLARE current_depth_1 INT DEFAULT 0;
      DECLARE rows_in_frontier_1 BIGINT DEFAULT 1;
    
      DROP TEMPORARY TABLE IF EXISTS bfs_visited_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_frontier_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_result_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_frontier_1_init;
      CREATE TEMPORARY TABLE bfs_visited_1 (node STRING);
      CREATE TEMPORARY TABLE bfs_frontier_1 AS
      SELECT n.id AS node
      FROM catalog.demo.Person n
      WHERE (n.age) > (30);
      INSERT INTO bfs_visited_1
      SELECT node FROM bfs_frontier_1;
      CREATE TEMPORARY TABLE bfs_result_1 (person_id STRING, friend_id STRING, since STRING, strength STRING, _next_node STRING, _bfs_depth INT);
      CREATE TEMPORARY TABLE bfs_frontier_1_init AS
      SELECT node FROM bfs_frontier_1;
      
      WHILE rows_in_frontier_1 > 0 AND current_depth_1 < 4 DO
        SET current_depth_1 = current_depth_1 + 1;
      
        DROP TEMPORARY TABLE IF EXISTS bfs_edges_1;
        CREATE TEMPORARY TABLE bfs_edges_1 AS
        SELECT e.person_id, e.friend_id, e.since, e.strength, e.friend_id AS _next_node
      FROM catalog.demo.Knows e
      INNER JOIN bfs_frontier_1 f ON e.person_id = f.node
      WHERE e.friend_id NOT IN (SELECT node FROM bfs_visited_1);
      
        SET rows_in_frontier_1 = (SELECT COUNT(1) FROM bfs_edges_1);
      
        IF rows_in_frontier_1 > 0 THEN
          INSERT INTO bfs_visited_1
          SELECT DISTINCT _next_node FROM bfs_edges_1;
          DROP TEMPORARY TABLE bfs_frontier_1;
          CREATE TEMPORARY TABLE bfs_frontier_1 AS
          SELECT DISTINCT _next_node AS node FROM bfs_edges_1;
          IF current_depth_1 >= 2 THEN
            INSERT INTO bfs_result_1
            SELECT *, current_depth_1 AS _bfs_depth FROM bfs_edges_1;
          END IF;
        END IF;
      END WHILE;
      
      CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
      SELECT f0.node AS start_node, r._next_node AS end_node, r._bfs_depth AS depth,
             r.person_id, r.friend_id, r.since, r.strength,
             ARRAY(NAMED_STRUCT('person_id', r.person_id, 'friend_id', r.friend_id, 'since', r.since, 'strength', r.strength)) AS path_edges
      FROM bfs_result_1 r
      CROSS JOIN bfs_frontier_1_init f0;
    
      SELECT 
       _gsql2rsql_a_id AS id
      ,_gsql2rsql_b_id AS id
    FROM (
      SELECT
         sink.id AS _gsql2rsql_b_id
        ,sink.name AS _gsql2rsql_b_name
        ,sink.age AS _gsql2rsql_b_age
        ,sink.nickname AS _gsql2rsql_b_nickname
        ,sink.salary AS _gsql2rsql_b_salary
        ,sink.active AS _gsql2rsql_b_active
        ,source.id AS _gsql2rsql_a_id
        ,source.name AS _gsql2rsql_a_name
        ,source.age AS _gsql2rsql_a_age
        ,source.nickname AS _gsql2rsql_a_nickname
        ,source.salary AS _gsql2rsql_a_salary
        ,source.active AS _gsql2rsql_a_active
        ,p.start_node
        ,p.end_node
        ,p.depth
        ,p.path_edges AS _gsql2rsql_path_edges
      FROM paths_1 p
      JOIN catalog.demo.Person sink
        ON sink.id = p.end_node
      JOIN catalog.demo.Person source
        ON source.id = p.start_node
      WHERE p.depth >= 2 AND p.depth <= 4 AND (sink.age) > (50)
    ) AS _proj;
    END
    ```

??? example "Procedural BFS — PySpark 4.2 (numbered_views)"
    ```sql
    BEGIN
      DECLARE bfs_depth_1 INT DEFAULT 0;
      DECLARE bfs_frontier_count_1 BIGINT DEFAULT 1;
      DECLARE bfs_union_sql_1 STRING DEFAULT '';
    
      CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_1_0 AS
      SELECT n.id AS node
      FROM catalog.demo.Person n
      WHERE (n.age) > (30);
      
      CREATE OR REPLACE TEMPORARY VIEW bfs_visited_1_0 AS
      SELECT node FROM bfs_frontier_1_0;
      
      WHILE bfs_frontier_count_1 > 0 AND bfs_depth_1 < 4 DO
        SET bfs_depth_1 = bfs_depth_1 + 1;
      
        EXECUTE IMMEDIATE
          'CREATE OR REPLACE TEMPORARY VIEW bfs_edges_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
           SELECT e.person_id, e.friend_id, e.since, e.strength, e.friend_id AS _next_node, ' || CAST(bfs_depth_1 AS STRING) || ' AS _bfs_depth FROM catalog.demo.Knows e INNER JOIN bfs_frontier_1_' || CAST(bfs_depth_1 - 1 AS STRING) || ' f ON e.person_id = f.node WHERE e.friend_id NOT IN (SELECT node FROM bfs_visited_1_' || CAST(bfs_depth_1 - 1 AS STRING) || ')';
      
        EXECUTE IMMEDIATE
          'SET bfs_frontier_count_1 = (SELECT COUNT(1) FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING) || ')';
      
        IF bfs_frontier_count_1 > 0 THEN
      
          EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY VIEW bfs_visited_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
             SELECT node FROM bfs_visited_1_' || CAST(bfs_depth_1 - 1 AS STRING) || '
             UNION
             SELECT DISTINCT _next_node AS node
             FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
      
          EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
             SELECT DISTINCT _next_node AS node
             FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
      
          IF bfs_depth_1 >= 2 THEN
            IF bfs_union_sql_1 = '' THEN
              SET bfs_union_sql_1 =
                'SELECT person_id, friend_id, since, strength, _next_node AS end_node, _bfs_depth AS depth FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
            ELSE
              SET bfs_union_sql_1 = bfs_union_sql_1
                || ' UNION ALL SELECT person_id, friend_id, since, strength, _next_node AS end_node, _bfs_depth AS depth FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
            END IF;
          END IF;
      
        END IF;
      END WHILE;
      
      IF bfs_union_sql_1 != '' THEN
        EXECUTE IMMEDIATE
          'CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
           SELECT f0.node AS start_node, r.end_node, r.depth,
                  r.person_id, r.friend_id, r.since, r.strength,
                  ARRAY(NAMED_STRUCT(''person_id'', r.person_id, ''friend_id'', r.friend_id, ''since'', r.since, ''strength'', r.strength)) AS path_edges
           FROM (' || bfs_union_sql_1 || ') r
           CROSS JOIN bfs_frontier_1_0 f0';
      ELSE
        CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
        SELECT
          CAST(NULL AS STRING) AS start_node,
          CAST(NULL AS STRING) AS end_node,
          CAST(NULL AS INT) AS depth,
          CAST(NULL AS STRING) AS person_id,
          CAST(NULL AS STRING) AS friend_id,
          CAST(NULL AS STRING) AS since,
          CAST(NULL AS STRING) AS strength,
          CAST(NULL AS ARRAY<STRUCT<person_id: STRING, friend_id: STRING, since: STRING, strength: STRING>>) AS path_edges
        WHERE 1 = 0;
      END IF;
    
      SELECT 
       _gsql2rsql_a_id AS id
      ,_gsql2rsql_b_id AS id
    FROM (
      SELECT
         sink.id AS _gsql2rsql_b_id
        ,sink.name AS _gsql2rsql_b_name
        ,sink.age AS _gsql2rsql_b_age
        ,sink.nickname AS _gsql2rsql_b_nickname
        ,sink.salary AS _gsql2rsql_b_salary
        ,sink.active AS _gsql2rsql_b_active
        ,source.id AS _gsql2rsql_a_id
        ,source.name AS _gsql2rsql_a_name
        ,source.age AS _gsql2rsql_a_age
        ,source.nickname AS _gsql2rsql_a_nickname
        ,source.salary AS _gsql2rsql_a_salary
        ,source.active AS _gsql2rsql_a_active
        ,p.start_node
        ,p.end_node
        ,p.depth
        ,p.path_edges AS _gsql2rsql_path_edges
      FROM paths_1 p
      JOIN catalog.demo.Person sink
        ON sink.id = p.end_node
      JOIN catalog.demo.Person source
        ON source.id = p.start_node
      WHERE p.depth >= 2 AND p.depth <= 4 AND (sink.age) > (50)
    ) AS _proj;
    END
    ```

---

## 8. Variable-length with compound sink filter

**Source**: Features

???+ note "OpenCypher Query"
    ```cypher
    MATCH path = (a:Person)-[:KNOWS*1..3]->(b:Person)
    WHERE b.age > 40 AND b.active = true
    RETURN a.id, b.id, [n IN nodes(path) | n.id] AS path_nodes
    ```

??? example "Procedural BFS — Databricks (temp_tables)"
    ```sql
    BEGIN
      DECLARE current_depth_1 INT DEFAULT 0;
      DECLARE rows_in_frontier_1 BIGINT DEFAULT 1;
    
      DROP TEMPORARY TABLE IF EXISTS bfs_visited_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_frontier_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_result_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_frontier_1_init;
      CREATE TEMPORARY TABLE bfs_visited_1 (node STRING);
      CREATE TEMPORARY TABLE bfs_frontier_1 AS
      SELECT n.id AS node
      FROM catalog.demo.Person n;
      INSERT INTO bfs_visited_1
      SELECT node FROM bfs_frontier_1;
      CREATE TEMPORARY TABLE bfs_result_1 (person_id STRING, friend_id STRING, since STRING, strength STRING, _next_node STRING, _bfs_depth INT);
      CREATE TEMPORARY TABLE bfs_frontier_1_init AS
      SELECT node FROM bfs_frontier_1;
      
      WHILE rows_in_frontier_1 > 0 AND current_depth_1 < 3 DO
        SET current_depth_1 = current_depth_1 + 1;
      
        DROP TEMPORARY TABLE IF EXISTS bfs_edges_1;
        CREATE TEMPORARY TABLE bfs_edges_1 AS
        SELECT e.person_id, e.friend_id, e.since, e.strength, e.friend_id AS _next_node
      FROM catalog.demo.Knows e
      INNER JOIN bfs_frontier_1 f ON e.person_id = f.node
      WHERE e.friend_id NOT IN (SELECT node FROM bfs_visited_1);
      
        SET rows_in_frontier_1 = (SELECT COUNT(1) FROM bfs_edges_1);
      
        IF rows_in_frontier_1 > 0 THEN
          INSERT INTO bfs_visited_1
          SELECT DISTINCT _next_node FROM bfs_edges_1;
          DROP TEMPORARY TABLE bfs_frontier_1;
          CREATE TEMPORARY TABLE bfs_frontier_1 AS
          SELECT DISTINCT _next_node AS node FROM bfs_edges_1;
          INSERT INTO bfs_result_1
          SELECT *, current_depth_1 AS _bfs_depth FROM bfs_edges_1;
        END IF;
      END WHILE;
      
      CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
      SELECT f0.node AS start_node, r._next_node AS end_node, r._bfs_depth AS depth,
             r.person_id, r.friend_id, r.since, r.strength,
             ARRAY(NAMED_STRUCT('person_id', r.person_id, 'friend_id', r.friend_id, 'since', r.since, 'strength', r.strength)) AS path_edges,
             CAST(NULL AS ARRAY<STRING>) AS path
      FROM bfs_result_1 r
      CROSS JOIN bfs_frontier_1_init f0;
    
      SELECT 
       _gsql2rsql_a_id AS id
      ,_gsql2rsql_b_id AS id
      ,_gsql2rsql_path_id AS path_nodes
    FROM (
      SELECT
         sink.id AS _gsql2rsql_b_id
        ,sink.name AS _gsql2rsql_b_name
        ,sink.age AS _gsql2rsql_b_age
        ,sink.nickname AS _gsql2rsql_b_nickname
        ,sink.salary AS _gsql2rsql_b_salary
        ,sink.active AS _gsql2rsql_b_active
        ,source.id AS _gsql2rsql_a_id
        ,source.name AS _gsql2rsql_a_name
        ,source.age AS _gsql2rsql_a_age
        ,source.nickname AS _gsql2rsql_a_nickname
        ,source.salary AS _gsql2rsql_a_salary
        ,source.active AS _gsql2rsql_a_active
        ,p.start_node
        ,p.end_node
        ,p.depth
        ,p.path AS _gsql2rsql_path_id
        ,p.path_edges AS _gsql2rsql_path_edges
      FROM paths_1 p
      JOIN catalog.demo.Person sink
        ON sink.id = p.end_node
      JOIN catalog.demo.Person source
        ON source.id = p.start_node
      WHERE p.depth >= 1 AND p.depth <= 3 AND ((sink.age) > (40)) AND ((sink.active) = (TRUE))
    ) AS _proj;
    END
    ```

??? example "Procedural BFS — PySpark 4.2 (numbered_views)"
    ```sql
    BEGIN
      DECLARE bfs_depth_1 INT DEFAULT 0;
      DECLARE bfs_frontier_count_1 BIGINT DEFAULT 1;
      DECLARE bfs_union_sql_1 STRING DEFAULT '';
    
      CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_1_0 AS
      SELECT n.id AS node
      FROM catalog.demo.Person n;
      
      CREATE OR REPLACE TEMPORARY VIEW bfs_visited_1_0 AS
      SELECT node FROM bfs_frontier_1_0;
      
      WHILE bfs_frontier_count_1 > 0 AND bfs_depth_1 < 3 DO
        SET bfs_depth_1 = bfs_depth_1 + 1;
      
        EXECUTE IMMEDIATE
          'CREATE OR REPLACE TEMPORARY VIEW bfs_edges_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
           SELECT e.person_id, e.friend_id, e.since, e.strength, e.friend_id AS _next_node, ' || CAST(bfs_depth_1 AS STRING) || ' AS _bfs_depth FROM catalog.demo.Knows e INNER JOIN bfs_frontier_1_' || CAST(bfs_depth_1 - 1 AS STRING) || ' f ON e.person_id = f.node WHERE e.friend_id NOT IN (SELECT node FROM bfs_visited_1_' || CAST(bfs_depth_1 - 1 AS STRING) || ')';
      
        EXECUTE IMMEDIATE
          'SET bfs_frontier_count_1 = (SELECT COUNT(1) FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING) || ')';
      
        IF bfs_frontier_count_1 > 0 THEN
      
          EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY VIEW bfs_visited_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
             SELECT node FROM bfs_visited_1_' || CAST(bfs_depth_1 - 1 AS STRING) || '
             UNION
             SELECT DISTINCT _next_node AS node
             FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
      
          EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
             SELECT DISTINCT _next_node AS node
             FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
      
          IF bfs_depth_1 >= 1 THEN
            IF bfs_union_sql_1 = '' THEN
              SET bfs_union_sql_1 =
                'SELECT person_id, friend_id, since, strength, _next_node AS end_node, _bfs_depth AS depth FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
            ELSE
              SET bfs_union_sql_1 = bfs_union_sql_1
                || ' UNION ALL SELECT person_id, friend_id, since, strength, _next_node AS end_node, _bfs_depth AS depth FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
            END IF;
          END IF;
      
        END IF;
      END WHILE;
      
      IF bfs_union_sql_1 != '' THEN
        EXECUTE IMMEDIATE
          'CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
           SELECT f0.node AS start_node, r.end_node, r.depth,
                  r.person_id, r.friend_id, r.since, r.strength,
                  ARRAY(NAMED_STRUCT(''person_id'', r.person_id, ''friend_id'', r.friend_id, ''since'', r.since, ''strength'', r.strength)) AS path_edges,
                  CAST(NULL AS ARRAY<STRING>) AS path
           FROM (' || bfs_union_sql_1 || ') r
           CROSS JOIN bfs_frontier_1_0 f0';
      ELSE
        CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
        SELECT
          CAST(NULL AS STRING) AS start_node,
          CAST(NULL AS STRING) AS end_node,
          CAST(NULL AS INT) AS depth,
          CAST(NULL AS STRING) AS person_id,
          CAST(NULL AS STRING) AS friend_id,
          CAST(NULL AS STRING) AS since,
          CAST(NULL AS STRING) AS strength,
          CAST(NULL AS ARRAY<STRUCT<person_id: STRING, friend_id: STRING, since: STRING, strength: STRING>>) AS path_edges,
          CAST(NULL AS ARRAY<STRING>) AS path
        WHERE 1 = 0;
      END IF;
    
      SELECT 
       _gsql2rsql_a_id AS id
      ,_gsql2rsql_b_id AS id
      ,_gsql2rsql_path_id AS path_nodes
    FROM (
      SELECT
         sink.id AS _gsql2rsql_b_id
        ,sink.name AS _gsql2rsql_b_name
        ,sink.age AS _gsql2rsql_b_age
        ,sink.nickname AS _gsql2rsql_b_nickname
        ,sink.salary AS _gsql2rsql_b_salary
        ,sink.active AS _gsql2rsql_b_active
        ,source.id AS _gsql2rsql_a_id
        ,source.name AS _gsql2rsql_a_name
        ,source.age AS _gsql2rsql_a_age
        ,source.nickname AS _gsql2rsql_a_nickname
        ,source.salary AS _gsql2rsql_a_salary
        ,source.active AS _gsql2rsql_a_active
        ,p.start_node
        ,p.end_node
        ,p.depth
        ,p.path AS _gsql2rsql_path_id
        ,p.path_edges AS _gsql2rsql_path_edges
      FROM paths_1 p
      JOIN catalog.demo.Person sink
        ON sink.id = p.end_node
      JOIN catalog.demo.Person source
        ON source.id = p.start_node
      WHERE p.depth >= 1 AND p.depth <= 3 AND ((sink.age) > (40)) AND ((sink.active) = (TRUE))
    ) AS _proj;
    END
    ```

---

## 9. Variable-length with sink filter and edge predicate

**Source**: Features

???+ note "OpenCypher Query"
    ```cypher
    MATCH path = (a:Person)-[:KNOWS*2..5]->(b:Person)
    WHERE b.age > 60
      AND ALL(k IN relationships(path) WHERE k.since > 2010)
    RETURN a.id, b.id, LENGTH(path) AS hops
    ```

??? example "Procedural BFS — Databricks (temp_tables)"
    ```sql
    BEGIN
      DECLARE current_depth_1 INT DEFAULT 0;
      DECLARE rows_in_frontier_1 BIGINT DEFAULT 1;
    
      DROP TEMPORARY TABLE IF EXISTS bfs_visited_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_frontier_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_result_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_frontier_1_init;
      CREATE TEMPORARY TABLE bfs_visited_1 (node STRING);
      CREATE TEMPORARY TABLE bfs_frontier_1 AS
      SELECT n.id AS node
      FROM catalog.demo.Person n;
      INSERT INTO bfs_visited_1
      SELECT node FROM bfs_frontier_1;
      CREATE TEMPORARY TABLE bfs_result_1 (person_id STRING, friend_id STRING, since STRING, strength STRING, _next_node STRING, _bfs_depth INT);
      CREATE TEMPORARY TABLE bfs_frontier_1_init AS
      SELECT node FROM bfs_frontier_1;
      
      WHILE rows_in_frontier_1 > 0 AND current_depth_1 < 5 DO
        SET current_depth_1 = current_depth_1 + 1;
      
        DROP TEMPORARY TABLE IF EXISTS bfs_edges_1;
        CREATE TEMPORARY TABLE bfs_edges_1 AS
        SELECT e.person_id, e.friend_id, e.since, e.strength, e.friend_id AS _next_node
      FROM catalog.demo.Knows e
      INNER JOIN bfs_frontier_1 f ON e.person_id = f.node
      WHERE e.friend_id NOT IN (SELECT node FROM bfs_visited_1) AND (e.since) > (2010);
      
        SET rows_in_frontier_1 = (SELECT COUNT(1) FROM bfs_edges_1);
      
        IF rows_in_frontier_1 > 0 THEN
          INSERT INTO bfs_visited_1
          SELECT DISTINCT _next_node FROM bfs_edges_1;
          DROP TEMPORARY TABLE bfs_frontier_1;
          CREATE TEMPORARY TABLE bfs_frontier_1 AS
          SELECT DISTINCT _next_node AS node FROM bfs_edges_1;
          IF current_depth_1 >= 2 THEN
            INSERT INTO bfs_result_1
            SELECT *, current_depth_1 AS _bfs_depth FROM bfs_edges_1;
          END IF;
        END IF;
      END WHILE;
      
      CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
      SELECT f0.node AS start_node, r._next_node AS end_node, r._bfs_depth AS depth,
             r.person_id, r.friend_id, r.since, r.strength,
             ARRAY(NAMED_STRUCT('person_id', r.person_id, 'friend_id', r.friend_id, 'since', r.since, 'strength', r.strength)) AS path_edges,
             CAST(NULL AS ARRAY<STRING>) AS path
      FROM bfs_result_1 r
      CROSS JOIN bfs_frontier_1_init f0;
    
      SELECT 
       _gsql2rsql_a_id AS id
      ,_gsql2rsql_b_id AS id
      ,(SIZE(_gsql2rsql_path_id) - 1) AS hops
    FROM (
      SELECT
         sink.id AS _gsql2rsql_b_id
        ,sink.name AS _gsql2rsql_b_name
        ,sink.age AS _gsql2rsql_b_age
        ,sink.nickname AS _gsql2rsql_b_nickname
        ,sink.salary AS _gsql2rsql_b_salary
        ,sink.active AS _gsql2rsql_b_active
        ,source.id AS _gsql2rsql_a_id
        ,source.name AS _gsql2rsql_a_name
        ,source.age AS _gsql2rsql_a_age
        ,source.nickname AS _gsql2rsql_a_nickname
        ,source.salary AS _gsql2rsql_a_salary
        ,source.active AS _gsql2rsql_a_active
        ,p.start_node
        ,p.end_node
        ,p.depth
        ,p.path AS _gsql2rsql_path_id
        ,p.path_edges AS _gsql2rsql_path_edges
      FROM paths_1 p
      JOIN catalog.demo.Person sink
        ON sink.id = p.end_node
      JOIN catalog.demo.Person source
        ON source.id = p.start_node
      WHERE p.depth >= 2 AND p.depth <= 5 AND (sink.age) > (60)
    ) AS _proj;
    END
    ```

??? example "Procedural BFS — PySpark 4.2 (numbered_views)"
    ```sql
    BEGIN
      DECLARE bfs_depth_1 INT DEFAULT 0;
      DECLARE bfs_frontier_count_1 BIGINT DEFAULT 1;
      DECLARE bfs_union_sql_1 STRING DEFAULT '';
    
      CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_1_0 AS
      SELECT n.id AS node
      FROM catalog.demo.Person n;
      
      CREATE OR REPLACE TEMPORARY VIEW bfs_visited_1_0 AS
      SELECT node FROM bfs_frontier_1_0;
      
      WHILE bfs_frontier_count_1 > 0 AND bfs_depth_1 < 5 DO
        SET bfs_depth_1 = bfs_depth_1 + 1;
      
        EXECUTE IMMEDIATE
          'CREATE OR REPLACE TEMPORARY VIEW bfs_edges_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
           SELECT e.person_id, e.friend_id, e.since, e.strength, e.friend_id AS _next_node, ' || CAST(bfs_depth_1 AS STRING) || ' AS _bfs_depth FROM catalog.demo.Knows e INNER JOIN bfs_frontier_1_' || CAST(bfs_depth_1 - 1 AS STRING) || ' f ON e.person_id = f.node WHERE e.friend_id NOT IN (SELECT node FROM bfs_visited_1_' || CAST(bfs_depth_1 - 1 AS STRING) || ') AND (e.since) > (2010)';
      
        EXECUTE IMMEDIATE
          'SET bfs_frontier_count_1 = (SELECT COUNT(1) FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING) || ')';
      
        IF bfs_frontier_count_1 > 0 THEN
      
          EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY VIEW bfs_visited_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
             SELECT node FROM bfs_visited_1_' || CAST(bfs_depth_1 - 1 AS STRING) || '
             UNION
             SELECT DISTINCT _next_node AS node
             FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
      
          EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
             SELECT DISTINCT _next_node AS node
             FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
      
          IF bfs_depth_1 >= 2 THEN
            IF bfs_union_sql_1 = '' THEN
              SET bfs_union_sql_1 =
                'SELECT person_id, friend_id, since, strength, _next_node AS end_node, _bfs_depth AS depth FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
            ELSE
              SET bfs_union_sql_1 = bfs_union_sql_1
                || ' UNION ALL SELECT person_id, friend_id, since, strength, _next_node AS end_node, _bfs_depth AS depth FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
            END IF;
          END IF;
      
        END IF;
      END WHILE;
      
      IF bfs_union_sql_1 != '' THEN
        EXECUTE IMMEDIATE
          'CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
           SELECT f0.node AS start_node, r.end_node, r.depth,
                  r.person_id, r.friend_id, r.since, r.strength,
                  ARRAY(NAMED_STRUCT(''person_id'', r.person_id, ''friend_id'', r.friend_id, ''since'', r.since, ''strength'', r.strength)) AS path_edges,
                  CAST(NULL AS ARRAY<STRING>) AS path
           FROM (' || bfs_union_sql_1 || ') r
           CROSS JOIN bfs_frontier_1_0 f0';
      ELSE
        CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
        SELECT
          CAST(NULL AS STRING) AS start_node,
          CAST(NULL AS STRING) AS end_node,
          CAST(NULL AS INT) AS depth,
          CAST(NULL AS STRING) AS person_id,
          CAST(NULL AS STRING) AS friend_id,
          CAST(NULL AS STRING) AS since,
          CAST(NULL AS STRING) AS strength,
          CAST(NULL AS ARRAY<STRUCT<person_id: STRING, friend_id: STRING, since: STRING, strength: STRING>>) AS path_edges,
          CAST(NULL AS ARRAY<STRING>) AS path
        WHERE 1 = 0;
      END IF;
    
      SELECT 
       _gsql2rsql_a_id AS id
      ,_gsql2rsql_b_id AS id
      ,(SIZE(_gsql2rsql_path_id) - 1) AS hops
    FROM (
      SELECT
         sink.id AS _gsql2rsql_b_id
        ,sink.name AS _gsql2rsql_b_name
        ,sink.age AS _gsql2rsql_b_age
        ,sink.nickname AS _gsql2rsql_b_nickname
        ,sink.salary AS _gsql2rsql_b_salary
        ,sink.active AS _gsql2rsql_b_active
        ,source.id AS _gsql2rsql_a_id
        ,source.name AS _gsql2rsql_a_name
        ,source.age AS _gsql2rsql_a_age
        ,source.nickname AS _gsql2rsql_a_nickname
        ,source.salary AS _gsql2rsql_a_salary
        ,source.active AS _gsql2rsql_a_active
        ,p.start_node
        ,p.end_node
        ,p.depth
        ,p.path AS _gsql2rsql_path_id
        ,p.path_edges AS _gsql2rsql_path_edges
      FROM paths_1 p
      JOIN catalog.demo.Person sink
        ON sink.id = p.end_node
      JOIN catalog.demo.Person source
        ON source.id = p.start_node
      WHERE p.depth >= 2 AND p.depth <= 5 AND (sink.age) > (60)
    ) AS _proj;
    END
    ```

---

## 10. Variable-length paths with multi-hop traversal and aggregation

**Source**: Features

???+ note "OpenCypher Query"
    ```cypher
    MATCH (p:Person)-[:KNOWS*1..3]-(friend:Person)-[:WORKS_AT]->(c:Company)
    WHERE c.industry = 'Technology'
    RETURN p.name, COUNT(DISTINCT friend) AS tech_connections
    ORDER BY tech_connections DESC
    LIMIT 10
    ```

??? example "Procedural BFS — Databricks (temp_tables)"
    ```sql
    BEGIN
      DECLARE current_depth_1 INT DEFAULT 0;
      DECLARE rows_in_frontier_1 BIGINT DEFAULT 1;
    
      DROP TEMPORARY TABLE IF EXISTS bfs_visited_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_frontier_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_result_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_frontier_1_init;
      CREATE TEMPORARY TABLE bfs_visited_1 (node STRING);
      CREATE TEMPORARY TABLE bfs_frontier_1 AS
      SELECT n.id AS node
      FROM catalog.demo.Person n;
      INSERT INTO bfs_visited_1
      SELECT node FROM bfs_frontier_1;
      CREATE TEMPORARY TABLE bfs_result_1 (person_id STRING, friend_id STRING, since STRING, strength STRING, _next_node STRING, _bfs_depth INT);
      CREATE TEMPORARY TABLE bfs_frontier_1_init AS
      SELECT node FROM bfs_frontier_1;
      
      WHILE rows_in_frontier_1 > 0 AND current_depth_1 < 3 DO
        SET current_depth_1 = current_depth_1 + 1;
      
        DROP TEMPORARY TABLE IF EXISTS bfs_edges_1;
        CREATE TEMPORARY TABLE bfs_edges_1 AS
        SELECT e.person_id, e.friend_id, e.since, e.strength, CASE WHEN f.node = e.person_id THEN e.friend_id ELSE e.person_id END AS _next_node
      FROM catalog.demo.Knows e
      INNER JOIN bfs_frontier_1 f ON (e.person_id = f.node OR e.friend_id = f.node)
      WHERE CASE WHEN f.node = e.person_id THEN e.friend_id ELSE e.person_id END NOT IN (SELECT node FROM bfs_visited_1);
      
        SET rows_in_frontier_1 = (SELECT COUNT(1) FROM bfs_edges_1);
      
        IF rows_in_frontier_1 > 0 THEN
          INSERT INTO bfs_visited_1
          SELECT DISTINCT _next_node FROM bfs_edges_1;
          DROP TEMPORARY TABLE bfs_frontier_1;
          CREATE TEMPORARY TABLE bfs_frontier_1 AS
          SELECT DISTINCT _next_node AS node FROM bfs_edges_1;
          INSERT INTO bfs_result_1
          SELECT *, current_depth_1 AS _bfs_depth FROM bfs_edges_1;
        END IF;
      END WHILE;
      
      CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
      SELECT f0.node AS start_node, r._next_node AS end_node, r._bfs_depth AS depth,
             r.person_id, r.friend_id, r.since, r.strength
      FROM bfs_result_1 r
      CROSS JOIN bfs_frontier_1_init f0;
    
      SELECT 
       _gsql2rsql_p_name AS name
      ,COUNT(DISTINCT _gsql2rsql_friend_id) AS tech_connections
    FROM (
      SELECT
         _left_0._gsql2rsql_p_name AS _gsql2rsql_p_name
        ,_left_0._gsql2rsql_friend_id AS _gsql2rsql_friend_id
        ,_left_0._gsql2rsql__anon2_person_id AS _gsql2rsql__anon2_person_id
        ,_left_0._gsql2rsql__anon2_company_id AS _gsql2rsql__anon2_company_id
        ,_right_0._gsql2rsql_c_industry AS _gsql2rsql_c_industry
      FROM (
        SELECT
           _left_1._gsql2rsql_p_name AS _gsql2rsql_p_name
          ,_left_1._gsql2rsql_friend_id AS _gsql2rsql_friend_id
          ,_right_1._gsql2rsql__anon2_person_id AS _gsql2rsql__anon2_person_id
          ,_right_1._gsql2rsql__anon2_company_id AS _gsql2rsql__anon2_company_id
        FROM (
          SELECT
             sink.id AS _gsql2rsql_friend_id
            ,sink.name AS _gsql2rsql_friend_name
            ,sink.age AS _gsql2rsql_friend_age
            ,sink.nickname AS _gsql2rsql_friend_nickname
            ,sink.salary AS _gsql2rsql_friend_salary
            ,sink.active AS _gsql2rsql_friend_active
            ,source.id AS _gsql2rsql_p_id
            ,source.name AS _gsql2rsql_p_name
            ,source.age AS _gsql2rsql_p_age
            ,source.nickname AS _gsql2rsql_p_nickname
            ,source.salary AS _gsql2rsql_p_salary
            ,source.active AS _gsql2rsql_p_active
            ,p.start_node
            ,p.end_node
            ,p.depth
          FROM paths_1 p
          JOIN catalog.demo.Person sink
            ON sink.id = p.end_node
          JOIN catalog.demo.Person source
            ON source.id = p.start_node
          WHERE p.depth >= 1 AND p.depth <= 3
        ) AS _left_1
        INNER JOIN (
          SELECT
             person_id AS _gsql2rsql__anon2_person_id
            ,company_id AS _gsql2rsql__anon2_company_id
          FROM
            catalog.demo.WorksAt
        ) AS _right_1 ON
          _left_1._gsql2rsql_friend_id = _right_1._gsql2rsql__anon2_person_id
      ) AS _left_0
      INNER JOIN (
        SELECT
           id AS _gsql2rsql_c_id
          ,industry AS _gsql2rsql_c_industry
        FROM
          catalog.demo.Company
      ) AS _right_0 ON
        _right_0._gsql2rsql_c_id = _left_0._gsql2rsql__anon2_company_id
    ) AS _proj
    WHERE (_gsql2rsql_c_industry) = ('Technology')
    GROUP BY _gsql2rsql_p_name
    ORDER BY tech_connections DESC
    LIMIT 10;
    END
    ```

??? example "Procedural BFS — PySpark 4.2 (numbered_views)"
    ```sql
    BEGIN
      DECLARE bfs_depth_1 INT DEFAULT 0;
      DECLARE bfs_frontier_count_1 BIGINT DEFAULT 1;
      DECLARE bfs_union_sql_1 STRING DEFAULT '';
    
      CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_1_0 AS
      SELECT n.id AS node
      FROM catalog.demo.Person n;
      
      CREATE OR REPLACE TEMPORARY VIEW bfs_visited_1_0 AS
      SELECT node FROM bfs_frontier_1_0;
      
      WHILE bfs_frontier_count_1 > 0 AND bfs_depth_1 < 3 DO
        SET bfs_depth_1 = bfs_depth_1 + 1;
      
        EXECUTE IMMEDIATE
          'CREATE OR REPLACE TEMPORARY VIEW bfs_edges_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
           SELECT e.person_id, e.friend_id, e.since, e.strength, CASE WHEN f.node = e.person_id THEN e.friend_id ELSE e.person_id END AS _next_node, ' || CAST(bfs_depth_1 AS STRING) || ' AS _bfs_depth FROM catalog.demo.Knows e INNER JOIN bfs_frontier_1_' || CAST(bfs_depth_1 - 1 AS STRING) || ' f ON (e.person_id = f.node OR e.friend_id = f.node) WHERE CASE WHEN f.node = e.person_id THEN e.friend_id ELSE e.person_id END NOT IN (SELECT node FROM bfs_visited_1_' || CAST(bfs_depth_1 - 1 AS STRING) || ')';
      
        EXECUTE IMMEDIATE
          'SET bfs_frontier_count_1 = (SELECT COUNT(1) FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING) || ')';
      
        IF bfs_frontier_count_1 > 0 THEN
      
          EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY VIEW bfs_visited_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
             SELECT node FROM bfs_visited_1_' || CAST(bfs_depth_1 - 1 AS STRING) || '
             UNION
             SELECT DISTINCT _next_node AS node
             FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
      
          EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
             SELECT DISTINCT _next_node AS node
             FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
      
          IF bfs_depth_1 >= 1 THEN
            IF bfs_union_sql_1 = '' THEN
              SET bfs_union_sql_1 =
                'SELECT person_id, friend_id, since, strength, _next_node AS end_node, _bfs_depth AS depth FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
            ELSE
              SET bfs_union_sql_1 = bfs_union_sql_1
                || ' UNION ALL SELECT person_id, friend_id, since, strength, _next_node AS end_node, _bfs_depth AS depth FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
            END IF;
          END IF;
      
        END IF;
      END WHILE;
      
      IF bfs_union_sql_1 != '' THEN
        EXECUTE IMMEDIATE
          'CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
           SELECT f0.node AS start_node, r.end_node, r.depth,
                  r.person_id, r.friend_id, r.since, r.strength
           FROM (' || bfs_union_sql_1 || ') r
           CROSS JOIN bfs_frontier_1_0 f0';
      ELSE
        CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
        SELECT
          CAST(NULL AS STRING) AS start_node,
          CAST(NULL AS STRING) AS end_node,
          CAST(NULL AS INT) AS depth,
          CAST(NULL AS STRING) AS person_id,
          CAST(NULL AS STRING) AS friend_id,
          CAST(NULL AS STRING) AS since,
          CAST(NULL AS STRING) AS strength
        WHERE 1 = 0;
      END IF;
    
      SELECT 
       _gsql2rsql_p_name AS name
      ,COUNT(DISTINCT _gsql2rsql_friend_id) AS tech_connections
    FROM (
      SELECT
         _left_0._gsql2rsql_p_name AS _gsql2rsql_p_name
        ,_left_0._gsql2rsql_friend_id AS _gsql2rsql_friend_id
        ,_left_0._gsql2rsql__anon2_person_id AS _gsql2rsql__anon2_person_id
        ,_left_0._gsql2rsql__anon2_company_id AS _gsql2rsql__anon2_company_id
        ,_right_0._gsql2rsql_c_industry AS _gsql2rsql_c_industry
      FROM (
        SELECT
           _left_1._gsql2rsql_p_name AS _gsql2rsql_p_name
          ,_left_1._gsql2rsql_friend_id AS _gsql2rsql_friend_id
          ,_right_1._gsql2rsql__anon2_person_id AS _gsql2rsql__anon2_person_id
          ,_right_1._gsql2rsql__anon2_company_id AS _gsql2rsql__anon2_company_id
        FROM (
          SELECT
             sink.id AS _gsql2rsql_friend_id
            ,sink.name AS _gsql2rsql_friend_name
            ,sink.age AS _gsql2rsql_friend_age
            ,sink.nickname AS _gsql2rsql_friend_nickname
            ,sink.salary AS _gsql2rsql_friend_salary
            ,sink.active AS _gsql2rsql_friend_active
            ,source.id AS _gsql2rsql_p_id
            ,source.name AS _gsql2rsql_p_name
            ,source.age AS _gsql2rsql_p_age
            ,source.nickname AS _gsql2rsql_p_nickname
            ,source.salary AS _gsql2rsql_p_salary
            ,source.active AS _gsql2rsql_p_active
            ,p.start_node
            ,p.end_node
            ,p.depth
          FROM paths_1 p
          JOIN catalog.demo.Person sink
            ON sink.id = p.end_node
          JOIN catalog.demo.Person source
            ON source.id = p.start_node
          WHERE p.depth >= 1 AND p.depth <= 3
        ) AS _left_1
        INNER JOIN (
          SELECT
             person_id AS _gsql2rsql__anon2_person_id
            ,company_id AS _gsql2rsql__anon2_company_id
          FROM
            catalog.demo.WorksAt
        ) AS _right_1 ON
          _left_1._gsql2rsql_friend_id = _right_1._gsql2rsql__anon2_person_id
      ) AS _left_0
      INNER JOIN (
        SELECT
           id AS _gsql2rsql_c_id
          ,industry AS _gsql2rsql_c_industry
        FROM
          catalog.demo.Company
      ) AS _right_0 ON
        _right_0._gsql2rsql_c_id = _left_0._gsql2rsql__anon2_company_id
    ) AS _proj
    WHERE (_gsql2rsql_c_industry) = ('Technology')
    GROUP BY _gsql2rsql_p_name
    ORDER BY tech_connections DESC
    LIMIT 10;
    END
    ```

---

## 11. BFS with inline source filter (CRITICAL OPTIMIZATION)

**Source**: Features

???+ note "OpenCypher Query"
    ```cypher
    MATCH path = (a:Person {name: 'Alice'})-[:KNOWS*1..3]->(b:Person)
    RETURN b.name, length(path) AS hops
    ORDER BY hops
    LIMIT 50
    ```

??? example "Procedural BFS — Databricks (temp_tables)"
    ```sql
    BEGIN
      DECLARE current_depth_1 INT DEFAULT 0;
      DECLARE rows_in_frontier_1 BIGINT DEFAULT 1;
    
      DROP TEMPORARY TABLE IF EXISTS bfs_visited_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_frontier_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_result_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_frontier_1_init;
      CREATE TEMPORARY TABLE bfs_visited_1 (node STRING);
      CREATE TEMPORARY TABLE bfs_frontier_1 AS
      SELECT n.id AS node
      FROM catalog.demo.Person n
      WHERE (n.name) = ('Alice');
      INSERT INTO bfs_visited_1
      SELECT node FROM bfs_frontier_1;
      CREATE TEMPORARY TABLE bfs_result_1 (person_id STRING, friend_id STRING, since STRING, strength STRING, _next_node STRING, _bfs_depth INT);
      CREATE TEMPORARY TABLE bfs_frontier_1_init AS
      SELECT node FROM bfs_frontier_1;
      
      WHILE rows_in_frontier_1 > 0 AND current_depth_1 < 3 DO
        SET current_depth_1 = current_depth_1 + 1;
      
        DROP TEMPORARY TABLE IF EXISTS bfs_edges_1;
        CREATE TEMPORARY TABLE bfs_edges_1 AS
        SELECT e.person_id, e.friend_id, e.since, e.strength, e.friend_id AS _next_node
      FROM catalog.demo.Knows e
      INNER JOIN bfs_frontier_1 f ON e.person_id = f.node
      WHERE e.friend_id NOT IN (SELECT node FROM bfs_visited_1);
      
        SET rows_in_frontier_1 = (SELECT COUNT(1) FROM bfs_edges_1);
      
        IF rows_in_frontier_1 > 0 THEN
          INSERT INTO bfs_visited_1
          SELECT DISTINCT _next_node FROM bfs_edges_1;
          DROP TEMPORARY TABLE bfs_frontier_1;
          CREATE TEMPORARY TABLE bfs_frontier_1 AS
          SELECT DISTINCT _next_node AS node FROM bfs_edges_1;
          INSERT INTO bfs_result_1
          SELECT *, current_depth_1 AS _bfs_depth FROM bfs_edges_1;
        END IF;
      END WHILE;
      
      CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
      SELECT f0.node AS start_node, r._next_node AS end_node, r._bfs_depth AS depth,
             r.person_id, r.friend_id, r.since, r.strength,
             ARRAY(NAMED_STRUCT('person_id', r.person_id, 'friend_id', r.friend_id, 'since', r.since, 'strength', r.strength)) AS path_edges,
             CAST(NULL AS ARRAY<STRING>) AS path
      FROM bfs_result_1 r
      CROSS JOIN bfs_frontier_1_init f0;
    
      SELECT 
       _gsql2rsql_b_name AS name
      ,(SIZE(_gsql2rsql_path_id) - 1) AS hops
    FROM (
      SELECT
         sink.id AS _gsql2rsql_b_id
        ,sink.name AS _gsql2rsql_b_name
        ,sink.age AS _gsql2rsql_b_age
        ,sink.nickname AS _gsql2rsql_b_nickname
        ,sink.salary AS _gsql2rsql_b_salary
        ,sink.active AS _gsql2rsql_b_active
        ,p.start_node
        ,p.end_node
        ,p.depth
        ,p.path AS _gsql2rsql_path_id
        ,p.path_edges AS _gsql2rsql_path_edges
      FROM paths_1 p
      JOIN catalog.demo.Person sink
        ON sink.id = p.end_node
      WHERE p.depth >= 1 AND p.depth <= 3
    ) AS _proj
    ORDER BY hops ASC
    LIMIT 50;
    END
    ```

??? example "Procedural BFS — PySpark 4.2 (numbered_views)"
    ```sql
    BEGIN
      DECLARE bfs_depth_1 INT DEFAULT 0;
      DECLARE bfs_frontier_count_1 BIGINT DEFAULT 1;
      DECLARE bfs_union_sql_1 STRING DEFAULT '';
    
      CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_1_0 AS
      SELECT n.id AS node
      FROM catalog.demo.Person n
      WHERE (n.name) = ('Alice');
      
      CREATE OR REPLACE TEMPORARY VIEW bfs_visited_1_0 AS
      SELECT node FROM bfs_frontier_1_0;
      
      WHILE bfs_frontier_count_1 > 0 AND bfs_depth_1 < 3 DO
        SET bfs_depth_1 = bfs_depth_1 + 1;
      
        EXECUTE IMMEDIATE
          'CREATE OR REPLACE TEMPORARY VIEW bfs_edges_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
           SELECT e.person_id, e.friend_id, e.since, e.strength, e.friend_id AS _next_node, ' || CAST(bfs_depth_1 AS STRING) || ' AS _bfs_depth FROM catalog.demo.Knows e INNER JOIN bfs_frontier_1_' || CAST(bfs_depth_1 - 1 AS STRING) || ' f ON e.person_id = f.node WHERE e.friend_id NOT IN (SELECT node FROM bfs_visited_1_' || CAST(bfs_depth_1 - 1 AS STRING) || ')';
      
        EXECUTE IMMEDIATE
          'SET bfs_frontier_count_1 = (SELECT COUNT(1) FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING) || ')';
      
        IF bfs_frontier_count_1 > 0 THEN
      
          EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY VIEW bfs_visited_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
             SELECT node FROM bfs_visited_1_' || CAST(bfs_depth_1 - 1 AS STRING) || '
             UNION
             SELECT DISTINCT _next_node AS node
             FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
      
          EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
             SELECT DISTINCT _next_node AS node
             FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
      
          IF bfs_depth_1 >= 1 THEN
            IF bfs_union_sql_1 = '' THEN
              SET bfs_union_sql_1 =
                'SELECT person_id, friend_id, since, strength, _next_node AS end_node, _bfs_depth AS depth FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
            ELSE
              SET bfs_union_sql_1 = bfs_union_sql_1
                || ' UNION ALL SELECT person_id, friend_id, since, strength, _next_node AS end_node, _bfs_depth AS depth FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
            END IF;
          END IF;
      
        END IF;
      END WHILE;
      
      IF bfs_union_sql_1 != '' THEN
        EXECUTE IMMEDIATE
          'CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
           SELECT f0.node AS start_node, r.end_node, r.depth,
                  r.person_id, r.friend_id, r.since, r.strength,
                  ARRAY(NAMED_STRUCT(''person_id'', r.person_id, ''friend_id'', r.friend_id, ''since'', r.since, ''strength'', r.strength)) AS path_edges,
                  CAST(NULL AS ARRAY<STRING>) AS path
           FROM (' || bfs_union_sql_1 || ') r
           CROSS JOIN bfs_frontier_1_0 f0';
      ELSE
        CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
        SELECT
          CAST(NULL AS STRING) AS start_node,
          CAST(NULL AS STRING) AS end_node,
          CAST(NULL AS INT) AS depth,
          CAST(NULL AS STRING) AS person_id,
          CAST(NULL AS STRING) AS friend_id,
          CAST(NULL AS STRING) AS since,
          CAST(NULL AS STRING) AS strength,
          CAST(NULL AS ARRAY<STRUCT<person_id: STRING, friend_id: STRING, since: STRING, strength: STRING>>) AS path_edges,
          CAST(NULL AS ARRAY<STRING>) AS path
        WHERE 1 = 0;
      END IF;
    
      SELECT 
       _gsql2rsql_b_name AS name
      ,(SIZE(_gsql2rsql_path_id) - 1) AS hops
    FROM (
      SELECT
         sink.id AS _gsql2rsql_b_id
        ,sink.name AS _gsql2rsql_b_name
        ,sink.age AS _gsql2rsql_b_age
        ,sink.nickname AS _gsql2rsql_b_nickname
        ,sink.salary AS _gsql2rsql_b_salary
        ,sink.active AS _gsql2rsql_b_active
        ,p.start_node
        ,p.end_node
        ,p.depth
        ,p.path AS _gsql2rsql_path_id
        ,p.path_edges AS _gsql2rsql_path_edges
      FROM paths_1 p
      JOIN catalog.demo.Person sink
        ON sink.id = p.end_node
      WHERE p.depth >= 1 AND p.depth <= 3
    ) AS _proj
    ORDER BY hops ASC
    LIMIT 50;
    END
    ```

---

## 12. BFS with inline filter on target node

**Source**: Features

???+ note "OpenCypher Query"
    ```cypher
    MATCH path = (a:Person)-[:KNOWS*1..3]->(b:Person {active: true})
    RETURN a.name, b.name, length(path) AS hops
    ORDER BY hops
    LIMIT 50
    ```

??? example "Procedural BFS — Databricks (temp_tables)"
    ```sql
    BEGIN
      DECLARE current_depth_1 INT DEFAULT 0;
      DECLARE rows_in_frontier_1 BIGINT DEFAULT 1;
    
      DROP TEMPORARY TABLE IF EXISTS bfs_visited_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_frontier_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_result_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_frontier_1_init;
      CREATE TEMPORARY TABLE bfs_visited_1 (node STRING);
      CREATE TEMPORARY TABLE bfs_frontier_1 AS
      SELECT n.id AS node
      FROM catalog.demo.Person n;
      INSERT INTO bfs_visited_1
      SELECT node FROM bfs_frontier_1;
      CREATE TEMPORARY TABLE bfs_result_1 (person_id STRING, friend_id STRING, since STRING, strength STRING, _next_node STRING, _bfs_depth INT);
      CREATE TEMPORARY TABLE bfs_frontier_1_init AS
      SELECT node FROM bfs_frontier_1;
      
      WHILE rows_in_frontier_1 > 0 AND current_depth_1 < 3 DO
        SET current_depth_1 = current_depth_1 + 1;
      
        DROP TEMPORARY TABLE IF EXISTS bfs_edges_1;
        CREATE TEMPORARY TABLE bfs_edges_1 AS
        SELECT e.person_id, e.friend_id, e.since, e.strength, e.friend_id AS _next_node
      FROM catalog.demo.Knows e
      INNER JOIN bfs_frontier_1 f ON e.person_id = f.node
      WHERE e.friend_id NOT IN (SELECT node FROM bfs_visited_1);
      
        SET rows_in_frontier_1 = (SELECT COUNT(1) FROM bfs_edges_1);
      
        IF rows_in_frontier_1 > 0 THEN
          INSERT INTO bfs_visited_1
          SELECT DISTINCT _next_node FROM bfs_edges_1;
          DROP TEMPORARY TABLE bfs_frontier_1;
          CREATE TEMPORARY TABLE bfs_frontier_1 AS
          SELECT DISTINCT _next_node AS node FROM bfs_edges_1;
          INSERT INTO bfs_result_1
          SELECT *, current_depth_1 AS _bfs_depth FROM bfs_edges_1;
        END IF;
      END WHILE;
      
      CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
      SELECT f0.node AS start_node, r._next_node AS end_node, r._bfs_depth AS depth,
             r.person_id, r.friend_id, r.since, r.strength,
             ARRAY(NAMED_STRUCT('person_id', r.person_id, 'friend_id', r.friend_id, 'since', r.since, 'strength', r.strength)) AS path_edges,
             CAST(NULL AS ARRAY<STRING>) AS path
      FROM bfs_result_1 r
      CROSS JOIN bfs_frontier_1_init f0;
    
      SELECT 
       _gsql2rsql_a_name AS name
      ,_gsql2rsql_b_name AS name
      ,(SIZE(_gsql2rsql_path_id) - 1) AS hops
    FROM (
      SELECT
         sink.id AS _gsql2rsql_b_id
        ,sink.name AS _gsql2rsql_b_name
        ,sink.age AS _gsql2rsql_b_age
        ,sink.nickname AS _gsql2rsql_b_nickname
        ,sink.salary AS _gsql2rsql_b_salary
        ,sink.active AS _gsql2rsql_b_active
        ,source.id AS _gsql2rsql_a_id
        ,source.name AS _gsql2rsql_a_name
        ,source.age AS _gsql2rsql_a_age
        ,source.nickname AS _gsql2rsql_a_nickname
        ,source.salary AS _gsql2rsql_a_salary
        ,source.active AS _gsql2rsql_a_active
        ,p.start_node
        ,p.end_node
        ,p.depth
        ,p.path AS _gsql2rsql_path_id
        ,p.path_edges AS _gsql2rsql_path_edges
      FROM paths_1 p
      JOIN catalog.demo.Person sink
        ON sink.id = p.end_node
      JOIN catalog.demo.Person source
        ON source.id = p.start_node
      WHERE p.depth >= 1 AND p.depth <= 3 AND (sink.active) = (TRUE)
    ) AS _proj
    ORDER BY hops ASC
    LIMIT 50;
    END
    ```

??? example "Procedural BFS — PySpark 4.2 (numbered_views)"
    ```sql
    BEGIN
      DECLARE bfs_depth_1 INT DEFAULT 0;
      DECLARE bfs_frontier_count_1 BIGINT DEFAULT 1;
      DECLARE bfs_union_sql_1 STRING DEFAULT '';
    
      CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_1_0 AS
      SELECT n.id AS node
      FROM catalog.demo.Person n;
      
      CREATE OR REPLACE TEMPORARY VIEW bfs_visited_1_0 AS
      SELECT node FROM bfs_frontier_1_0;
      
      WHILE bfs_frontier_count_1 > 0 AND bfs_depth_1 < 3 DO
        SET bfs_depth_1 = bfs_depth_1 + 1;
      
        EXECUTE IMMEDIATE
          'CREATE OR REPLACE TEMPORARY VIEW bfs_edges_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
           SELECT e.person_id, e.friend_id, e.since, e.strength, e.friend_id AS _next_node, ' || CAST(bfs_depth_1 AS STRING) || ' AS _bfs_depth FROM catalog.demo.Knows e INNER JOIN bfs_frontier_1_' || CAST(bfs_depth_1 - 1 AS STRING) || ' f ON e.person_id = f.node WHERE e.friend_id NOT IN (SELECT node FROM bfs_visited_1_' || CAST(bfs_depth_1 - 1 AS STRING) || ')';
      
        EXECUTE IMMEDIATE
          'SET bfs_frontier_count_1 = (SELECT COUNT(1) FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING) || ')';
      
        IF bfs_frontier_count_1 > 0 THEN
      
          EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY VIEW bfs_visited_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
             SELECT node FROM bfs_visited_1_' || CAST(bfs_depth_1 - 1 AS STRING) || '
             UNION
             SELECT DISTINCT _next_node AS node
             FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
      
          EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
             SELECT DISTINCT _next_node AS node
             FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
      
          IF bfs_depth_1 >= 1 THEN
            IF bfs_union_sql_1 = '' THEN
              SET bfs_union_sql_1 =
                'SELECT person_id, friend_id, since, strength, _next_node AS end_node, _bfs_depth AS depth FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
            ELSE
              SET bfs_union_sql_1 = bfs_union_sql_1
                || ' UNION ALL SELECT person_id, friend_id, since, strength, _next_node AS end_node, _bfs_depth AS depth FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
            END IF;
          END IF;
      
        END IF;
      END WHILE;
      
      IF bfs_union_sql_1 != '' THEN
        EXECUTE IMMEDIATE
          'CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
           SELECT f0.node AS start_node, r.end_node, r.depth,
                  r.person_id, r.friend_id, r.since, r.strength,
                  ARRAY(NAMED_STRUCT(''person_id'', r.person_id, ''friend_id'', r.friend_id, ''since'', r.since, ''strength'', r.strength)) AS path_edges,
                  CAST(NULL AS ARRAY<STRING>) AS path
           FROM (' || bfs_union_sql_1 || ') r
           CROSS JOIN bfs_frontier_1_0 f0';
      ELSE
        CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
        SELECT
          CAST(NULL AS STRING) AS start_node,
          CAST(NULL AS STRING) AS end_node,
          CAST(NULL AS INT) AS depth,
          CAST(NULL AS STRING) AS person_id,
          CAST(NULL AS STRING) AS friend_id,
          CAST(NULL AS STRING) AS since,
          CAST(NULL AS STRING) AS strength,
          CAST(NULL AS ARRAY<STRUCT<person_id: STRING, friend_id: STRING, since: STRING, strength: STRING>>) AS path_edges,
          CAST(NULL AS ARRAY<STRING>) AS path
        WHERE 1 = 0;
      END IF;
    
      SELECT 
       _gsql2rsql_a_name AS name
      ,_gsql2rsql_b_name AS name
      ,(SIZE(_gsql2rsql_path_id) - 1) AS hops
    FROM (
      SELECT
         sink.id AS _gsql2rsql_b_id
        ,sink.name AS _gsql2rsql_b_name
        ,sink.age AS _gsql2rsql_b_age
        ,sink.nickname AS _gsql2rsql_b_nickname
        ,sink.salary AS _gsql2rsql_b_salary
        ,sink.active AS _gsql2rsql_b_active
        ,source.id AS _gsql2rsql_a_id
        ,source.name AS _gsql2rsql_a_name
        ,source.age AS _gsql2rsql_a_age
        ,source.nickname AS _gsql2rsql_a_nickname
        ,source.salary AS _gsql2rsql_a_salary
        ,source.active AS _gsql2rsql_a_active
        ,p.start_node
        ,p.end_node
        ,p.depth
        ,p.path AS _gsql2rsql_path_id
        ,p.path_edges AS _gsql2rsql_path_edges
      FROM paths_1 p
      JOIN catalog.demo.Person sink
        ON sink.id = p.end_node
      JOIN catalog.demo.Person source
        ON source.id = p.start_node
      WHERE p.depth >= 1 AND p.depth <= 3 AND (sink.active) = (TRUE)
    ) AS _proj
    ORDER BY hops ASC
    LIMIT 50;
    END
    ```

---

## 13. NO LABEL: Variable-length path without labels

**Source**: Features

???+ note "OpenCypher Query"
    ```cypher
    MATCH path = (a)-[:KNOWS*1..2]->(b)
    RETURN a.id, b.id, length(path) AS hops
    LIMIT 50
    ```

??? example "Procedural BFS — Databricks (temp_tables)"
    ```sql
    BEGIN
      DECLARE current_depth_1 INT DEFAULT 0;
      DECLARE rows_in_frontier_1 BIGINT DEFAULT 1;
    
      DROP TEMPORARY TABLE IF EXISTS bfs_visited_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_frontier_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_result_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_frontier_1_init;
      CREATE TEMPORARY TABLE bfs_visited_1 (node STRING);
      CREATE TEMPORARY TABLE bfs_frontier_1 AS
      SELECT n.id AS node
      FROM catalog.demo.AllNodes n;
      INSERT INTO bfs_visited_1
      SELECT node FROM bfs_frontier_1;
      CREATE TEMPORARY TABLE bfs_result_1 (person_id STRING, friend_id STRING, since STRING, strength STRING, _next_node STRING, _bfs_depth INT);
      CREATE TEMPORARY TABLE bfs_frontier_1_init AS
      SELECT node FROM bfs_frontier_1;
      
      WHILE rows_in_frontier_1 > 0 AND current_depth_1 < 2 DO
        SET current_depth_1 = current_depth_1 + 1;
      
        DROP TEMPORARY TABLE IF EXISTS bfs_edges_1;
        CREATE TEMPORARY TABLE bfs_edges_1 AS
        SELECT e.person_id, e.friend_id, e.since, e.strength, e.friend_id AS _next_node
      FROM catalog.demo.Knows e
      INNER JOIN bfs_frontier_1 f ON e.person_id = f.node
      WHERE e.friend_id NOT IN (SELECT node FROM bfs_visited_1);
      
        SET rows_in_frontier_1 = (SELECT COUNT(1) FROM bfs_edges_1);
      
        IF rows_in_frontier_1 > 0 THEN
          INSERT INTO bfs_visited_1
          SELECT DISTINCT _next_node FROM bfs_edges_1;
          DROP TEMPORARY TABLE bfs_frontier_1;
          CREATE TEMPORARY TABLE bfs_frontier_1 AS
          SELECT DISTINCT _next_node AS node FROM bfs_edges_1;
          INSERT INTO bfs_result_1
          SELECT *, current_depth_1 AS _bfs_depth FROM bfs_edges_1;
        END IF;
      END WHILE;
      
      CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
      SELECT f0.node AS start_node, r._next_node AS end_node, r._bfs_depth AS depth,
             r.person_id, r.friend_id, r.since, r.strength,
             ARRAY(NAMED_STRUCT('person_id', r.person_id, 'friend_id', r.friend_id, 'since', r.since, 'strength', r.strength)) AS path_edges,
             CAST(NULL AS ARRAY<STRING>) AS path
      FROM bfs_result_1 r
      CROSS JOIN bfs_frontier_1_init f0;
    
      SELECT 
       _gsql2rsql_a_id AS id
      ,_gsql2rsql_b_id AS id
      ,(SIZE(_gsql2rsql_path_id) - 1) AS hops
    FROM (
      SELECT
         sink.id AS _gsql2rsql_b_id
        ,sink.name AS _gsql2rsql_b_name
        ,sink.age AS _gsql2rsql_b_age
        ,sink.nickname AS _gsql2rsql_b_nickname
        ,sink.salary AS _gsql2rsql_b_salary
        ,sink.active AS _gsql2rsql_b_active
        ,sink.population AS _gsql2rsql_b_population
        ,sink.country AS _gsql2rsql_b_country
        ,sink.title AS _gsql2rsql_b_title
        ,sink.year AS _gsql2rsql_b_year
        ,sink.genre AS _gsql2rsql_b_genre
        ,sink.rating AS _gsql2rsql_b_rating
        ,sink.industry AS _gsql2rsql_b_industry
        ,source.id AS _gsql2rsql_a_id
        ,source.name AS _gsql2rsql_a_name
        ,source.age AS _gsql2rsql_a_age
        ,source.nickname AS _gsql2rsql_a_nickname
        ,source.salary AS _gsql2rsql_a_salary
        ,source.active AS _gsql2rsql_a_active
        ,source.population AS _gsql2rsql_a_population
        ,source.country AS _gsql2rsql_a_country
        ,source.title AS _gsql2rsql_a_title
        ,source.year AS _gsql2rsql_a_year
        ,source.genre AS _gsql2rsql_a_genre
        ,source.rating AS _gsql2rsql_a_rating
        ,source.industry AS _gsql2rsql_a_industry
        ,p.start_node
        ,p.end_node
        ,p.depth
        ,p.path AS _gsql2rsql_path_id
        ,p.path_edges AS _gsql2rsql_path_edges
      FROM paths_1 p
      JOIN catalog.demo.AllNodes sink
        ON sink.id = p.end_node
      JOIN catalog.demo.AllNodes source
        ON source.id = p.start_node
      WHERE p.depth >= 1 AND p.depth <= 2
    ) AS _proj
    LIMIT 50;
    END
    ```

??? example "Procedural BFS — PySpark 4.2 (numbered_views)"
    ```sql
    BEGIN
      DECLARE bfs_depth_1 INT DEFAULT 0;
      DECLARE bfs_frontier_count_1 BIGINT DEFAULT 1;
      DECLARE bfs_union_sql_1 STRING DEFAULT '';
    
      CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_1_0 AS
      SELECT n.id AS node
      FROM catalog.demo.AllNodes n;
      
      CREATE OR REPLACE TEMPORARY VIEW bfs_visited_1_0 AS
      SELECT node FROM bfs_frontier_1_0;
      
      WHILE bfs_frontier_count_1 > 0 AND bfs_depth_1 < 2 DO
        SET bfs_depth_1 = bfs_depth_1 + 1;
      
        EXECUTE IMMEDIATE
          'CREATE OR REPLACE TEMPORARY VIEW bfs_edges_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
           SELECT e.person_id, e.friend_id, e.since, e.strength, e.friend_id AS _next_node, ' || CAST(bfs_depth_1 AS STRING) || ' AS _bfs_depth FROM catalog.demo.Knows e INNER JOIN bfs_frontier_1_' || CAST(bfs_depth_1 - 1 AS STRING) || ' f ON e.person_id = f.node WHERE e.friend_id NOT IN (SELECT node FROM bfs_visited_1_' || CAST(bfs_depth_1 - 1 AS STRING) || ')';
      
        EXECUTE IMMEDIATE
          'SET bfs_frontier_count_1 = (SELECT COUNT(1) FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING) || ')';
      
        IF bfs_frontier_count_1 > 0 THEN
      
          EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY VIEW bfs_visited_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
             SELECT node FROM bfs_visited_1_' || CAST(bfs_depth_1 - 1 AS STRING) || '
             UNION
             SELECT DISTINCT _next_node AS node
             FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
      
          EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
             SELECT DISTINCT _next_node AS node
             FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
      
          IF bfs_depth_1 >= 1 THEN
            IF bfs_union_sql_1 = '' THEN
              SET bfs_union_sql_1 =
                'SELECT person_id, friend_id, since, strength, _next_node AS end_node, _bfs_depth AS depth FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
            ELSE
              SET bfs_union_sql_1 = bfs_union_sql_1
                || ' UNION ALL SELECT person_id, friend_id, since, strength, _next_node AS end_node, _bfs_depth AS depth FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
            END IF;
          END IF;
      
        END IF;
      END WHILE;
      
      IF bfs_union_sql_1 != '' THEN
        EXECUTE IMMEDIATE
          'CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
           SELECT f0.node AS start_node, r.end_node, r.depth,
                  r.person_id, r.friend_id, r.since, r.strength,
                  ARRAY(NAMED_STRUCT(''person_id'', r.person_id, ''friend_id'', r.friend_id, ''since'', r.since, ''strength'', r.strength)) AS path_edges,
                  CAST(NULL AS ARRAY<STRING>) AS path
           FROM (' || bfs_union_sql_1 || ') r
           CROSS JOIN bfs_frontier_1_0 f0';
      ELSE
        CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
        SELECT
          CAST(NULL AS STRING) AS start_node,
          CAST(NULL AS STRING) AS end_node,
          CAST(NULL AS INT) AS depth,
          CAST(NULL AS STRING) AS person_id,
          CAST(NULL AS STRING) AS friend_id,
          CAST(NULL AS STRING) AS since,
          CAST(NULL AS STRING) AS strength,
          CAST(NULL AS ARRAY<STRUCT<person_id: STRING, friend_id: STRING, since: STRING, strength: STRING>>) AS path_edges,
          CAST(NULL AS ARRAY<STRING>) AS path
        WHERE 1 = 0;
      END IF;
    
      SELECT 
       _gsql2rsql_a_id AS id
      ,_gsql2rsql_b_id AS id
      ,(SIZE(_gsql2rsql_path_id) - 1) AS hops
    FROM (
      SELECT
         sink.id AS _gsql2rsql_b_id
        ,sink.name AS _gsql2rsql_b_name
        ,sink.age AS _gsql2rsql_b_age
        ,sink.nickname AS _gsql2rsql_b_nickname
        ,sink.salary AS _gsql2rsql_b_salary
        ,sink.active AS _gsql2rsql_b_active
        ,sink.population AS _gsql2rsql_b_population
        ,sink.country AS _gsql2rsql_b_country
        ,sink.title AS _gsql2rsql_b_title
        ,sink.year AS _gsql2rsql_b_year
        ,sink.genre AS _gsql2rsql_b_genre
        ,sink.rating AS _gsql2rsql_b_rating
        ,sink.industry AS _gsql2rsql_b_industry
        ,source.id AS _gsql2rsql_a_id
        ,source.name AS _gsql2rsql_a_name
        ,source.age AS _gsql2rsql_a_age
        ,source.nickname AS _gsql2rsql_a_nickname
        ,source.salary AS _gsql2rsql_a_salary
        ,source.active AS _gsql2rsql_a_active
        ,source.population AS _gsql2rsql_a_population
        ,source.country AS _gsql2rsql_a_country
        ,source.title AS _gsql2rsql_a_title
        ,source.year AS _gsql2rsql_a_year
        ,source.genre AS _gsql2rsql_a_genre
        ,source.rating AS _gsql2rsql_a_rating
        ,source.industry AS _gsql2rsql_a_industry
        ,p.start_node
        ,p.end_node
        ,p.depth
        ,p.path AS _gsql2rsql_path_id
        ,p.path_edges AS _gsql2rsql_path_edges
      FROM paths_1 p
      JOIN catalog.demo.AllNodes sink
        ON sink.id = p.end_node
      JOIN catalog.demo.AllNodes source
        ON source.id = p.start_node
      WHERE p.depth >= 1 AND p.depth <= 2
    ) AS _proj
    LIMIT 50;
    END
    ```

---

## 14. NO LABEL: VLP with labeled source, unlabeled target

**Source**: Features

???+ note "OpenCypher Query"
    ```cypher
    MATCH path = (a:Person)-[:KNOWS*1..2]->(b)
    RETURN a.name, b.id, length(path) AS hops
    LIMIT 50
    ```

??? example "Procedural BFS — Databricks (temp_tables)"
    ```sql
    BEGIN
      DECLARE current_depth_1 INT DEFAULT 0;
      DECLARE rows_in_frontier_1 BIGINT DEFAULT 1;
    
      DROP TEMPORARY TABLE IF EXISTS bfs_visited_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_frontier_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_result_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_frontier_1_init;
      CREATE TEMPORARY TABLE bfs_visited_1 (node STRING);
      CREATE TEMPORARY TABLE bfs_frontier_1 AS
      SELECT n.id AS node
      FROM catalog.demo.Person n;
      INSERT INTO bfs_visited_1
      SELECT node FROM bfs_frontier_1;
      CREATE TEMPORARY TABLE bfs_result_1 (person_id STRING, friend_id STRING, since STRING, strength STRING, _next_node STRING, _bfs_depth INT);
      CREATE TEMPORARY TABLE bfs_frontier_1_init AS
      SELECT node FROM bfs_frontier_1;
      
      WHILE rows_in_frontier_1 > 0 AND current_depth_1 < 2 DO
        SET current_depth_1 = current_depth_1 + 1;
      
        DROP TEMPORARY TABLE IF EXISTS bfs_edges_1;
        CREATE TEMPORARY TABLE bfs_edges_1 AS
        SELECT e.person_id, e.friend_id, e.since, e.strength, e.friend_id AS _next_node
      FROM catalog.demo.Knows e
      INNER JOIN bfs_frontier_1 f ON e.person_id = f.node
      WHERE e.friend_id NOT IN (SELECT node FROM bfs_visited_1);
      
        SET rows_in_frontier_1 = (SELECT COUNT(1) FROM bfs_edges_1);
      
        IF rows_in_frontier_1 > 0 THEN
          INSERT INTO bfs_visited_1
          SELECT DISTINCT _next_node FROM bfs_edges_1;
          DROP TEMPORARY TABLE bfs_frontier_1;
          CREATE TEMPORARY TABLE bfs_frontier_1 AS
          SELECT DISTINCT _next_node AS node FROM bfs_edges_1;
          INSERT INTO bfs_result_1
          SELECT *, current_depth_1 AS _bfs_depth FROM bfs_edges_1;
        END IF;
      END WHILE;
      
      CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
      SELECT f0.node AS start_node, r._next_node AS end_node, r._bfs_depth AS depth,
             r.person_id, r.friend_id, r.since, r.strength,
             ARRAY(NAMED_STRUCT('person_id', r.person_id, 'friend_id', r.friend_id, 'since', r.since, 'strength', r.strength)) AS path_edges,
             CAST(NULL AS ARRAY<STRING>) AS path
      FROM bfs_result_1 r
      CROSS JOIN bfs_frontier_1_init f0;
    
      SELECT 
       _gsql2rsql_a_name AS name
      ,_gsql2rsql_b_id AS id
      ,(SIZE(_gsql2rsql_path_id) - 1) AS hops
    FROM (
      SELECT
         sink.id AS _gsql2rsql_b_id
        ,sink.name AS _gsql2rsql_b_name
        ,sink.age AS _gsql2rsql_b_age
        ,sink.nickname AS _gsql2rsql_b_nickname
        ,sink.salary AS _gsql2rsql_b_salary
        ,sink.active AS _gsql2rsql_b_active
        ,sink.population AS _gsql2rsql_b_population
        ,sink.country AS _gsql2rsql_b_country
        ,sink.title AS _gsql2rsql_b_title
        ,sink.year AS _gsql2rsql_b_year
        ,sink.genre AS _gsql2rsql_b_genre
        ,sink.rating AS _gsql2rsql_b_rating
        ,sink.industry AS _gsql2rsql_b_industry
        ,source.id AS _gsql2rsql_a_id
        ,source.name AS _gsql2rsql_a_name
        ,source.age AS _gsql2rsql_a_age
        ,source.nickname AS _gsql2rsql_a_nickname
        ,source.salary AS _gsql2rsql_a_salary
        ,source.active AS _gsql2rsql_a_active
        ,p.start_node
        ,p.end_node
        ,p.depth
        ,p.path AS _gsql2rsql_path_id
        ,p.path_edges AS _gsql2rsql_path_edges
      FROM paths_1 p
      JOIN catalog.demo.AllNodes sink
        ON sink.id = p.end_node
      JOIN catalog.demo.Person source
        ON source.id = p.start_node
      WHERE p.depth >= 1 AND p.depth <= 2
    ) AS _proj
    LIMIT 50;
    END
    ```

??? example "Procedural BFS — PySpark 4.2 (numbered_views)"
    ```sql
    BEGIN
      DECLARE bfs_depth_1 INT DEFAULT 0;
      DECLARE bfs_frontier_count_1 BIGINT DEFAULT 1;
      DECLARE bfs_union_sql_1 STRING DEFAULT '';
    
      CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_1_0 AS
      SELECT n.id AS node
      FROM catalog.demo.Person n;
      
      CREATE OR REPLACE TEMPORARY VIEW bfs_visited_1_0 AS
      SELECT node FROM bfs_frontier_1_0;
      
      WHILE bfs_frontier_count_1 > 0 AND bfs_depth_1 < 2 DO
        SET bfs_depth_1 = bfs_depth_1 + 1;
      
        EXECUTE IMMEDIATE
          'CREATE OR REPLACE TEMPORARY VIEW bfs_edges_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
           SELECT e.person_id, e.friend_id, e.since, e.strength, e.friend_id AS _next_node, ' || CAST(bfs_depth_1 AS STRING) || ' AS _bfs_depth FROM catalog.demo.Knows e INNER JOIN bfs_frontier_1_' || CAST(bfs_depth_1 - 1 AS STRING) || ' f ON e.person_id = f.node WHERE e.friend_id NOT IN (SELECT node FROM bfs_visited_1_' || CAST(bfs_depth_1 - 1 AS STRING) || ')';
      
        EXECUTE IMMEDIATE
          'SET bfs_frontier_count_1 = (SELECT COUNT(1) FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING) || ')';
      
        IF bfs_frontier_count_1 > 0 THEN
      
          EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY VIEW bfs_visited_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
             SELECT node FROM bfs_visited_1_' || CAST(bfs_depth_1 - 1 AS STRING) || '
             UNION
             SELECT DISTINCT _next_node AS node
             FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
      
          EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
             SELECT DISTINCT _next_node AS node
             FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
      
          IF bfs_depth_1 >= 1 THEN
            IF bfs_union_sql_1 = '' THEN
              SET bfs_union_sql_1 =
                'SELECT person_id, friend_id, since, strength, _next_node AS end_node, _bfs_depth AS depth FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
            ELSE
              SET bfs_union_sql_1 = bfs_union_sql_1
                || ' UNION ALL SELECT person_id, friend_id, since, strength, _next_node AS end_node, _bfs_depth AS depth FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
            END IF;
          END IF;
      
        END IF;
      END WHILE;
      
      IF bfs_union_sql_1 != '' THEN
        EXECUTE IMMEDIATE
          'CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
           SELECT f0.node AS start_node, r.end_node, r.depth,
                  r.person_id, r.friend_id, r.since, r.strength,
                  ARRAY(NAMED_STRUCT(''person_id'', r.person_id, ''friend_id'', r.friend_id, ''since'', r.since, ''strength'', r.strength)) AS path_edges,
                  CAST(NULL AS ARRAY<STRING>) AS path
           FROM (' || bfs_union_sql_1 || ') r
           CROSS JOIN bfs_frontier_1_0 f0';
      ELSE
        CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
        SELECT
          CAST(NULL AS STRING) AS start_node,
          CAST(NULL AS STRING) AS end_node,
          CAST(NULL AS INT) AS depth,
          CAST(NULL AS STRING) AS person_id,
          CAST(NULL AS STRING) AS friend_id,
          CAST(NULL AS STRING) AS since,
          CAST(NULL AS STRING) AS strength,
          CAST(NULL AS ARRAY<STRUCT<person_id: STRING, friend_id: STRING, since: STRING, strength: STRING>>) AS path_edges,
          CAST(NULL AS ARRAY<STRING>) AS path
        WHERE 1 = 0;
      END IF;
    
      SELECT 
       _gsql2rsql_a_name AS name
      ,_gsql2rsql_b_id AS id
      ,(SIZE(_gsql2rsql_path_id) - 1) AS hops
    FROM (
      SELECT
         sink.id AS _gsql2rsql_b_id
        ,sink.name AS _gsql2rsql_b_name
        ,sink.age AS _gsql2rsql_b_age
        ,sink.nickname AS _gsql2rsql_b_nickname
        ,sink.salary AS _gsql2rsql_b_salary
        ,sink.active AS _gsql2rsql_b_active
        ,sink.population AS _gsql2rsql_b_population
        ,sink.country AS _gsql2rsql_b_country
        ,sink.title AS _gsql2rsql_b_title
        ,sink.year AS _gsql2rsql_b_year
        ,sink.genre AS _gsql2rsql_b_genre
        ,sink.rating AS _gsql2rsql_b_rating
        ,sink.industry AS _gsql2rsql_b_industry
        ,source.id AS _gsql2rsql_a_id
        ,source.name AS _gsql2rsql_a_name
        ,source.age AS _gsql2rsql_a_age
        ,source.nickname AS _gsql2rsql_a_nickname
        ,source.salary AS _gsql2rsql_a_salary
        ,source.active AS _gsql2rsql_a_active
        ,p.start_node
        ,p.end_node
        ,p.depth
        ,p.path AS _gsql2rsql_path_id
        ,p.path_edges AS _gsql2rsql_path_edges
      FROM paths_1 p
      JOIN catalog.demo.AllNodes sink
        ON sink.id = p.end_node
      JOIN catalog.demo.Person source
        ON source.id = p.start_node
      WHERE p.depth >= 1 AND p.depth <= 2
    ) AS _proj
    LIMIT 50;
    END
    ```

---

## 15. NO LABEL: VLP with unlabeled source, labeled target

**Source**: Features

???+ note "OpenCypher Query"
    ```cypher
    MATCH path = (a)-[:KNOWS*1..2]->(b:Person)
    RETURN a.id, b.name, length(path) AS hops
    LIMIT 50
    ```

??? example "Procedural BFS — Databricks (temp_tables)"
    ```sql
    BEGIN
      DECLARE current_depth_1 INT DEFAULT 0;
      DECLARE rows_in_frontier_1 BIGINT DEFAULT 1;
    
      DROP TEMPORARY TABLE IF EXISTS bfs_visited_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_frontier_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_result_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_frontier_1_init;
      CREATE TEMPORARY TABLE bfs_visited_1 (node STRING);
      CREATE TEMPORARY TABLE bfs_frontier_1 AS
      SELECT n.id AS node
      FROM catalog.demo.AllNodes n;
      INSERT INTO bfs_visited_1
      SELECT node FROM bfs_frontier_1;
      CREATE TEMPORARY TABLE bfs_result_1 (person_id STRING, friend_id STRING, since STRING, strength STRING, _next_node STRING, _bfs_depth INT);
      CREATE TEMPORARY TABLE bfs_frontier_1_init AS
      SELECT node FROM bfs_frontier_1;
      
      WHILE rows_in_frontier_1 > 0 AND current_depth_1 < 2 DO
        SET current_depth_1 = current_depth_1 + 1;
      
        DROP TEMPORARY TABLE IF EXISTS bfs_edges_1;
        CREATE TEMPORARY TABLE bfs_edges_1 AS
        SELECT e.person_id, e.friend_id, e.since, e.strength, e.friend_id AS _next_node
      FROM catalog.demo.Knows e
      INNER JOIN bfs_frontier_1 f ON e.person_id = f.node
      WHERE e.friend_id NOT IN (SELECT node FROM bfs_visited_1);
      
        SET rows_in_frontier_1 = (SELECT COUNT(1) FROM bfs_edges_1);
      
        IF rows_in_frontier_1 > 0 THEN
          INSERT INTO bfs_visited_1
          SELECT DISTINCT _next_node FROM bfs_edges_1;
          DROP TEMPORARY TABLE bfs_frontier_1;
          CREATE TEMPORARY TABLE bfs_frontier_1 AS
          SELECT DISTINCT _next_node AS node FROM bfs_edges_1;
          INSERT INTO bfs_result_1
          SELECT *, current_depth_1 AS _bfs_depth FROM bfs_edges_1;
        END IF;
      END WHILE;
      
      CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
      SELECT f0.node AS start_node, r._next_node AS end_node, r._bfs_depth AS depth,
             r.person_id, r.friend_id, r.since, r.strength,
             ARRAY(NAMED_STRUCT('person_id', r.person_id, 'friend_id', r.friend_id, 'since', r.since, 'strength', r.strength)) AS path_edges,
             CAST(NULL AS ARRAY<STRING>) AS path
      FROM bfs_result_1 r
      CROSS JOIN bfs_frontier_1_init f0;
    
      SELECT 
       _gsql2rsql_a_id AS id
      ,_gsql2rsql_b_name AS name
      ,(SIZE(_gsql2rsql_path_id) - 1) AS hops
    FROM (
      SELECT
         sink.id AS _gsql2rsql_b_id
        ,sink.name AS _gsql2rsql_b_name
        ,sink.age AS _gsql2rsql_b_age
        ,sink.nickname AS _gsql2rsql_b_nickname
        ,sink.salary AS _gsql2rsql_b_salary
        ,sink.active AS _gsql2rsql_b_active
        ,source.id AS _gsql2rsql_a_id
        ,source.name AS _gsql2rsql_a_name
        ,source.age AS _gsql2rsql_a_age
        ,source.nickname AS _gsql2rsql_a_nickname
        ,source.salary AS _gsql2rsql_a_salary
        ,source.active AS _gsql2rsql_a_active
        ,source.population AS _gsql2rsql_a_population
        ,source.country AS _gsql2rsql_a_country
        ,source.title AS _gsql2rsql_a_title
        ,source.year AS _gsql2rsql_a_year
        ,source.genre AS _gsql2rsql_a_genre
        ,source.rating AS _gsql2rsql_a_rating
        ,source.industry AS _gsql2rsql_a_industry
        ,p.start_node
        ,p.end_node
        ,p.depth
        ,p.path AS _gsql2rsql_path_id
        ,p.path_edges AS _gsql2rsql_path_edges
      FROM paths_1 p
      JOIN catalog.demo.Person sink
        ON sink.id = p.end_node
      JOIN catalog.demo.AllNodes source
        ON source.id = p.start_node
      WHERE p.depth >= 1 AND p.depth <= 2
    ) AS _proj
    LIMIT 50;
    END
    ```

??? example "Procedural BFS — PySpark 4.2 (numbered_views)"
    ```sql
    BEGIN
      DECLARE bfs_depth_1 INT DEFAULT 0;
      DECLARE bfs_frontier_count_1 BIGINT DEFAULT 1;
      DECLARE bfs_union_sql_1 STRING DEFAULT '';
    
      CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_1_0 AS
      SELECT n.id AS node
      FROM catalog.demo.AllNodes n;
      
      CREATE OR REPLACE TEMPORARY VIEW bfs_visited_1_0 AS
      SELECT node FROM bfs_frontier_1_0;
      
      WHILE bfs_frontier_count_1 > 0 AND bfs_depth_1 < 2 DO
        SET bfs_depth_1 = bfs_depth_1 + 1;
      
        EXECUTE IMMEDIATE
          'CREATE OR REPLACE TEMPORARY VIEW bfs_edges_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
           SELECT e.person_id, e.friend_id, e.since, e.strength, e.friend_id AS _next_node, ' || CAST(bfs_depth_1 AS STRING) || ' AS _bfs_depth FROM catalog.demo.Knows e INNER JOIN bfs_frontier_1_' || CAST(bfs_depth_1 - 1 AS STRING) || ' f ON e.person_id = f.node WHERE e.friend_id NOT IN (SELECT node FROM bfs_visited_1_' || CAST(bfs_depth_1 - 1 AS STRING) || ')';
      
        EXECUTE IMMEDIATE
          'SET bfs_frontier_count_1 = (SELECT COUNT(1) FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING) || ')';
      
        IF bfs_frontier_count_1 > 0 THEN
      
          EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY VIEW bfs_visited_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
             SELECT node FROM bfs_visited_1_' || CAST(bfs_depth_1 - 1 AS STRING) || '
             UNION
             SELECT DISTINCT _next_node AS node
             FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
      
          EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
             SELECT DISTINCT _next_node AS node
             FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
      
          IF bfs_depth_1 >= 1 THEN
            IF bfs_union_sql_1 = '' THEN
              SET bfs_union_sql_1 =
                'SELECT person_id, friend_id, since, strength, _next_node AS end_node, _bfs_depth AS depth FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
            ELSE
              SET bfs_union_sql_1 = bfs_union_sql_1
                || ' UNION ALL SELECT person_id, friend_id, since, strength, _next_node AS end_node, _bfs_depth AS depth FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
            END IF;
          END IF;
      
        END IF;
      END WHILE;
      
      IF bfs_union_sql_1 != '' THEN
        EXECUTE IMMEDIATE
          'CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
           SELECT f0.node AS start_node, r.end_node, r.depth,
                  r.person_id, r.friend_id, r.since, r.strength,
                  ARRAY(NAMED_STRUCT(''person_id'', r.person_id, ''friend_id'', r.friend_id, ''since'', r.since, ''strength'', r.strength)) AS path_edges,
                  CAST(NULL AS ARRAY<STRING>) AS path
           FROM (' || bfs_union_sql_1 || ') r
           CROSS JOIN bfs_frontier_1_0 f0';
      ELSE
        CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
        SELECT
          CAST(NULL AS STRING) AS start_node,
          CAST(NULL AS STRING) AS end_node,
          CAST(NULL AS INT) AS depth,
          CAST(NULL AS STRING) AS person_id,
          CAST(NULL AS STRING) AS friend_id,
          CAST(NULL AS STRING) AS since,
          CAST(NULL AS STRING) AS strength,
          CAST(NULL AS ARRAY<STRUCT<person_id: STRING, friend_id: STRING, since: STRING, strength: STRING>>) AS path_edges,
          CAST(NULL AS ARRAY<STRING>) AS path
        WHERE 1 = 0;
      END IF;
    
      SELECT 
       _gsql2rsql_a_id AS id
      ,_gsql2rsql_b_name AS name
      ,(SIZE(_gsql2rsql_path_id) - 1) AS hops
    FROM (
      SELECT
         sink.id AS _gsql2rsql_b_id
        ,sink.name AS _gsql2rsql_b_name
        ,sink.age AS _gsql2rsql_b_age
        ,sink.nickname AS _gsql2rsql_b_nickname
        ,sink.salary AS _gsql2rsql_b_salary
        ,sink.active AS _gsql2rsql_b_active
        ,source.id AS _gsql2rsql_a_id
        ,source.name AS _gsql2rsql_a_name
        ,source.age AS _gsql2rsql_a_age
        ,source.nickname AS _gsql2rsql_a_nickname
        ,source.salary AS _gsql2rsql_a_salary
        ,source.active AS _gsql2rsql_a_active
        ,source.population AS _gsql2rsql_a_population
        ,source.country AS _gsql2rsql_a_country
        ,source.title AS _gsql2rsql_a_title
        ,source.year AS _gsql2rsql_a_year
        ,source.genre AS _gsql2rsql_a_genre
        ,source.rating AS _gsql2rsql_a_rating
        ,source.industry AS _gsql2rsql_a_industry
        ,p.start_node
        ,p.end_node
        ,p.depth
        ,p.path AS _gsql2rsql_path_id
        ,p.path_edges AS _gsql2rsql_path_edges
      FROM paths_1 p
      JOIN catalog.demo.Person sink
        ON sink.id = p.end_node
      JOIN catalog.demo.AllNodes source
        ON source.id = p.start_node
      WHERE p.depth >= 1 AND p.depth <= 2
    ) AS _proj
    LIMIT 50;
    END
    ```

---

## 16. Identify camouflage patterns with hidden relationship chains

**Source**: Fraud

???+ note "OpenCypher Query"
    ```cypher
    MATCH path = (a:Account)-[:TRANSFER*2..4]->(b:Account)
    WHERE a.risk_score > 70 AND b.risk_score > 70
    RETURN a.id, b.id, LENGTH(path) AS chain_length,
           [node IN nodes(path) | node.id] AS path_nodes
    ORDER BY chain_length DESC
    ```

??? example "Procedural BFS — Databricks (temp_tables)"
    ```sql
    BEGIN
      DECLARE current_depth_1 INT DEFAULT 0;
      DECLARE rows_in_frontier_1 BIGINT DEFAULT 1;
    
      DROP TEMPORARY TABLE IF EXISTS bfs_visited_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_frontier_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_result_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_frontier_1_init;
      CREATE TEMPORARY TABLE bfs_visited_1 (node STRING);
      CREATE TEMPORARY TABLE bfs_frontier_1 AS
      SELECT n.id AS node
      FROM catalog.fraud.Account n
      WHERE (n.risk_score) > (70);
      INSERT INTO bfs_visited_1
      SELECT node FROM bfs_frontier_1;
      CREATE TEMPORARY TABLE bfs_result_1 (source_account_id STRING, target_account_id STRING, amount STRING, timestamp STRING, _next_node STRING, _bfs_depth INT);
      CREATE TEMPORARY TABLE bfs_frontier_1_init AS
      SELECT node FROM bfs_frontier_1;
      
      WHILE rows_in_frontier_1 > 0 AND current_depth_1 < 4 DO
        SET current_depth_1 = current_depth_1 + 1;
      
        DROP TEMPORARY TABLE IF EXISTS bfs_edges_1;
        CREATE TEMPORARY TABLE bfs_edges_1 AS
        SELECT e.source_account_id, e.target_account_id, e.amount, e.timestamp, e.target_account_id AS _next_node
      FROM catalog.fraud.Transfer e
      INNER JOIN bfs_frontier_1 f ON e.source_account_id = f.node
      WHERE e.target_account_id NOT IN (SELECT node FROM bfs_visited_1);
      
        SET rows_in_frontier_1 = (SELECT COUNT(1) FROM bfs_edges_1);
      
        IF rows_in_frontier_1 > 0 THEN
          INSERT INTO bfs_visited_1
          SELECT DISTINCT _next_node FROM bfs_edges_1;
          DROP TEMPORARY TABLE bfs_frontier_1;
          CREATE TEMPORARY TABLE bfs_frontier_1 AS
          SELECT DISTINCT _next_node AS node FROM bfs_edges_1;
          IF current_depth_1 >= 2 THEN
            INSERT INTO bfs_result_1
            SELECT *, current_depth_1 AS _bfs_depth FROM bfs_edges_1;
          END IF;
        END IF;
      END WHILE;
      
      CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
      SELECT f0.node AS start_node, r._next_node AS end_node, r._bfs_depth AS depth,
             r.source_account_id, r.target_account_id, r.amount, r.timestamp,
             ARRAY(NAMED_STRUCT('source_account_id', r.source_account_id, 'target_account_id', r.target_account_id, 'amount', r.amount, 'timestamp', r.timestamp)) AS path_edges,
             CAST(NULL AS ARRAY<STRING>) AS path
      FROM bfs_result_1 r
      CROSS JOIN bfs_frontier_1_init f0;
    
      SELECT 
       _gsql2rsql_a_id AS id
      ,_gsql2rsql_b_id AS id
      ,(SIZE(_gsql2rsql_path_id) - 1) AS chain_length
      ,_gsql2rsql_path_id AS path_nodes
    FROM (
      SELECT
         sink.id AS _gsql2rsql_b_id
        ,sink.holder_name AS _gsql2rsql_b_holder_name
        ,sink.risk_score AS _gsql2rsql_b_risk_score
        ,sink.status AS _gsql2rsql_b_status
        ,sink.default_date AS _gsql2rsql_b_default_date
        ,sink.home_country AS _gsql2rsql_b_home_country
        ,sink.kyc_status AS _gsql2rsql_b_kyc_status
        ,sink.days_since_creation AS _gsql2rsql_b_days_since_creation
        ,source.id AS _gsql2rsql_a_id
        ,source.holder_name AS _gsql2rsql_a_holder_name
        ,source.risk_score AS _gsql2rsql_a_risk_score
        ,source.status AS _gsql2rsql_a_status
        ,source.default_date AS _gsql2rsql_a_default_date
        ,source.home_country AS _gsql2rsql_a_home_country
        ,source.kyc_status AS _gsql2rsql_a_kyc_status
        ,source.days_since_creation AS _gsql2rsql_a_days_since_creation
        ,p.start_node
        ,p.end_node
        ,p.depth
        ,p.path AS _gsql2rsql_path_id
        ,p.path_edges AS _gsql2rsql_path_edges
      FROM paths_1 p
      JOIN catalog.fraud.Account sink
        ON sink.id = p.end_node
      JOIN catalog.fraud.Account source
        ON source.id = p.start_node
      WHERE p.depth >= 2 AND p.depth <= 4 AND (sink.risk_score) > (70)
    ) AS _proj
    ORDER BY chain_length DESC;
    END
    ```

??? example "Procedural BFS — PySpark 4.2 (numbered_views)"
    ```sql
    BEGIN
      DECLARE bfs_depth_1 INT DEFAULT 0;
      DECLARE bfs_frontier_count_1 BIGINT DEFAULT 1;
      DECLARE bfs_union_sql_1 STRING DEFAULT '';
    
      CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_1_0 AS
      SELECT n.id AS node
      FROM catalog.fraud.Account n
      WHERE (n.risk_score) > (70);
      
      CREATE OR REPLACE TEMPORARY VIEW bfs_visited_1_0 AS
      SELECT node FROM bfs_frontier_1_0;
      
      WHILE bfs_frontier_count_1 > 0 AND bfs_depth_1 < 4 DO
        SET bfs_depth_1 = bfs_depth_1 + 1;
      
        EXECUTE IMMEDIATE
          'CREATE OR REPLACE TEMPORARY VIEW bfs_edges_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
           SELECT e.source_account_id, e.target_account_id, e.amount, e.timestamp, e.target_account_id AS _next_node, ' || CAST(bfs_depth_1 AS STRING) || ' AS _bfs_depth FROM catalog.fraud.Transfer e INNER JOIN bfs_frontier_1_' || CAST(bfs_depth_1 - 1 AS STRING) || ' f ON e.source_account_id = f.node WHERE e.target_account_id NOT IN (SELECT node FROM bfs_visited_1_' || CAST(bfs_depth_1 - 1 AS STRING) || ')';
      
        EXECUTE IMMEDIATE
          'SET bfs_frontier_count_1 = (SELECT COUNT(1) FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING) || ')';
      
        IF bfs_frontier_count_1 > 0 THEN
      
          EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY VIEW bfs_visited_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
             SELECT node FROM bfs_visited_1_' || CAST(bfs_depth_1 - 1 AS STRING) || '
             UNION
             SELECT DISTINCT _next_node AS node
             FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
      
          EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
             SELECT DISTINCT _next_node AS node
             FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
      
          IF bfs_depth_1 >= 2 THEN
            IF bfs_union_sql_1 = '' THEN
              SET bfs_union_sql_1 =
                'SELECT source_account_id, target_account_id, amount, timestamp, _next_node AS end_node, _bfs_depth AS depth FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
            ELSE
              SET bfs_union_sql_1 = bfs_union_sql_1
                || ' UNION ALL SELECT source_account_id, target_account_id, amount, timestamp, _next_node AS end_node, _bfs_depth AS depth FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
            END IF;
          END IF;
      
        END IF;
      END WHILE;
      
      IF bfs_union_sql_1 != '' THEN
        EXECUTE IMMEDIATE
          'CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
           SELECT f0.node AS start_node, r.end_node, r.depth,
                  r.source_account_id, r.target_account_id, r.amount, r.timestamp,
                  ARRAY(NAMED_STRUCT(''source_account_id'', r.source_account_id, ''target_account_id'', r.target_account_id, ''amount'', r.amount, ''timestamp'', r.timestamp)) AS path_edges,
                  CAST(NULL AS ARRAY<STRING>) AS path
           FROM (' || bfs_union_sql_1 || ') r
           CROSS JOIN bfs_frontier_1_0 f0';
      ELSE
        CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
        SELECT
          CAST(NULL AS STRING) AS start_node,
          CAST(NULL AS STRING) AS end_node,
          CAST(NULL AS INT) AS depth,
          CAST(NULL AS STRING) AS source_account_id,
          CAST(NULL AS STRING) AS target_account_id,
          CAST(NULL AS STRING) AS amount,
          CAST(NULL AS STRING) AS timestamp,
          CAST(NULL AS ARRAY<STRUCT<source_account_id: STRING, target_account_id: STRING, amount: STRING, timestamp: STRING>>) AS path_edges,
          CAST(NULL AS ARRAY<STRING>) AS path
        WHERE 1 = 0;
      END IF;
    
      SELECT 
       _gsql2rsql_a_id AS id
      ,_gsql2rsql_b_id AS id
      ,(SIZE(_gsql2rsql_path_id) - 1) AS chain_length
      ,_gsql2rsql_path_id AS path_nodes
    FROM (
      SELECT
         sink.id AS _gsql2rsql_b_id
        ,sink.holder_name AS _gsql2rsql_b_holder_name
        ,sink.risk_score AS _gsql2rsql_b_risk_score
        ,sink.status AS _gsql2rsql_b_status
        ,sink.default_date AS _gsql2rsql_b_default_date
        ,sink.home_country AS _gsql2rsql_b_home_country
        ,sink.kyc_status AS _gsql2rsql_b_kyc_status
        ,sink.days_since_creation AS _gsql2rsql_b_days_since_creation
        ,source.id AS _gsql2rsql_a_id
        ,source.holder_name AS _gsql2rsql_a_holder_name
        ,source.risk_score AS _gsql2rsql_a_risk_score
        ,source.status AS _gsql2rsql_a_status
        ,source.default_date AS _gsql2rsql_a_default_date
        ,source.home_country AS _gsql2rsql_a_home_country
        ,source.kyc_status AS _gsql2rsql_a_kyc_status
        ,source.days_since_creation AS _gsql2rsql_a_days_since_creation
        ,p.start_node
        ,p.end_node
        ,p.depth
        ,p.path AS _gsql2rsql_path_id
        ,p.path_edges AS _gsql2rsql_path_edges
      FROM paths_1 p
      JOIN catalog.fraud.Account sink
        ON sink.id = p.end_node
      JOIN catalog.fraud.Account source
        ON source.id = p.start_node
      WHERE p.depth >= 2 AND p.depth <= 4 AND (sink.risk_score) > (70)
    ) AS _proj
    ORDER BY chain_length DESC;
    END
    ```

---

## 17. Trace money mule networks with rapid transfer chains

**Source**: Fraud

???+ note "OpenCypher Query"
    ```cypher
    MATCH path = (source:Account)-[:TRANSFER*3..6]->(sink:Account)
    WHERE ALL(rel IN relationships(path) WHERE rel.timestamp > TIMESTAMP() - DURATION('P7D'))
      AND ALL(rel IN relationships(path) WHERE rel.amount > 1000)
    WITH source, sink, path,
         REDUCE(total = 0, rel IN relationships(path) | total + rel.amount) AS total_amount
    RETURN source.id, sink.id, LENGTH(path) AS hops, total_amount
    ORDER BY total_amount DESC
    LIMIT 15
    ```

??? example "Procedural BFS — Databricks (temp_tables)"
    ```sql
    BEGIN
      DECLARE current_depth_1 INT DEFAULT 0;
      DECLARE rows_in_frontier_1 BIGINT DEFAULT 1;
    
      DROP TEMPORARY TABLE IF EXISTS bfs_visited_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_frontier_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_result_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_frontier_1_init;
      CREATE TEMPORARY TABLE bfs_visited_1 (node STRING);
      CREATE TEMPORARY TABLE bfs_frontier_1 AS
      SELECT n.id AS node
      FROM catalog.fraud.Account n;
      INSERT INTO bfs_visited_1
      SELECT node FROM bfs_frontier_1;
      CREATE TEMPORARY TABLE bfs_result_1 (source_account_id STRING, target_account_id STRING, amount STRING, timestamp STRING, _next_node STRING, _bfs_depth INT);
      CREATE TEMPORARY TABLE bfs_frontier_1_init AS
      SELECT node FROM bfs_frontier_1;
      
      WHILE rows_in_frontier_1 > 0 AND current_depth_1 < 6 DO
        SET current_depth_1 = current_depth_1 + 1;
      
        DROP TEMPORARY TABLE IF EXISTS bfs_edges_1;
        CREATE TEMPORARY TABLE bfs_edges_1 AS
        SELECT e.source_account_id, e.target_account_id, e.amount, e.timestamp, e.target_account_id AS _next_node
      FROM catalog.fraud.Transfer e
      INNER JOIN bfs_frontier_1 f ON e.source_account_id = f.node
      WHERE e.target_account_id NOT IN (SELECT node FROM bfs_visited_1) AND ((e.timestamp) > ((CURRENT_TIMESTAMP()) - (INTERVAL 7 DAY))) AND ((e.amount) > (1000));
      
        SET rows_in_frontier_1 = (SELECT COUNT(1) FROM bfs_edges_1);
      
        IF rows_in_frontier_1 > 0 THEN
          INSERT INTO bfs_visited_1
          SELECT DISTINCT _next_node FROM bfs_edges_1;
          DROP TEMPORARY TABLE bfs_frontier_1;
          CREATE TEMPORARY TABLE bfs_frontier_1 AS
          SELECT DISTINCT _next_node AS node FROM bfs_edges_1;
          IF current_depth_1 >= 3 THEN
            INSERT INTO bfs_result_1
            SELECT *, current_depth_1 AS _bfs_depth FROM bfs_edges_1;
          END IF;
        END IF;
      END WHILE;
      
      CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
      SELECT f0.node AS start_node, r._next_node AS end_node, r._bfs_depth AS depth,
             r.source_account_id, r.target_account_id, r.amount, r.timestamp,
             ARRAY(NAMED_STRUCT('source_account_id', r.source_account_id, 'target_account_id', r.target_account_id, 'amount', r.amount, 'timestamp', r.timestamp)) AS path_edges,
             CAST(NULL AS ARRAY<STRING>) AS path
      FROM bfs_result_1 r
      CROSS JOIN bfs_frontier_1_init f0;
    
      SELECT 
       _gsql2rsql_source_id AS id
      ,_gsql2rsql_sink_id AS id
      ,(SIZE(_gsql2rsql_path_id) - 1) AS hops
      ,total_amount AS total_amount
    FROM (
      SELECT 
         _gsql2rsql_source_id AS _gsql2rsql_source_id
        ,_gsql2rsql_sink_id AS _gsql2rsql_sink_id
        ,_gsql2rsql_path_id AS _gsql2rsql_path_id
        ,AGGREGATE(_gsql2rsql_path_edges, CAST(0 AS DOUBLE), (total, rel) -> (total) + (rel.amount)) AS total_amount
        ,_gsql2rsql_path_edges AS _gsql2rsql_path_edges
        ,_gsql2rsql_sink_days_since_creation AS _gsql2rsql_sink_days_since_creation
        ,_gsql2rsql_sink_default_date AS _gsql2rsql_sink_default_date
        ,_gsql2rsql_sink_holder_name AS _gsql2rsql_sink_holder_name
        ,_gsql2rsql_sink_home_country AS _gsql2rsql_sink_home_country
        ,_gsql2rsql_sink_kyc_status AS _gsql2rsql_sink_kyc_status
        ,_gsql2rsql_sink_risk_score AS _gsql2rsql_sink_risk_score
        ,_gsql2rsql_sink_status AS _gsql2rsql_sink_status
        ,_gsql2rsql_source_days_since_creation AS _gsql2rsql_source_days_since_creation
        ,_gsql2rsql_source_default_date AS _gsql2rsql_source_default_date
        ,_gsql2rsql_source_holder_name AS _gsql2rsql_source_holder_name
        ,_gsql2rsql_source_home_country AS _gsql2rsql_source_home_country
        ,_gsql2rsql_source_kyc_status AS _gsql2rsql_source_kyc_status
        ,_gsql2rsql_source_risk_score AS _gsql2rsql_source_risk_score
        ,_gsql2rsql_source_status AS _gsql2rsql_source_status
      FROM (
        SELECT
           sink.id AS _gsql2rsql_sink_id
          ,sink.holder_name AS _gsql2rsql_sink_holder_name
          ,sink.risk_score AS _gsql2rsql_sink_risk_score
          ,sink.status AS _gsql2rsql_sink_status
          ,sink.default_date AS _gsql2rsql_sink_default_date
          ,sink.home_country AS _gsql2rsql_sink_home_country
          ,sink.kyc_status AS _gsql2rsql_sink_kyc_status
          ,sink.days_since_creation AS _gsql2rsql_sink_days_since_creation
          ,source.id AS _gsql2rsql_source_id
          ,source.holder_name AS _gsql2rsql_source_holder_name
          ,source.risk_score AS _gsql2rsql_source_risk_score
          ,source.status AS _gsql2rsql_source_status
          ,source.default_date AS _gsql2rsql_source_default_date
          ,source.home_country AS _gsql2rsql_source_home_country
          ,source.kyc_status AS _gsql2rsql_source_kyc_status
          ,source.days_since_creation AS _gsql2rsql_source_days_since_creation
          ,p.start_node
          ,p.end_node
          ,p.depth
          ,p.path AS _gsql2rsql_path_id
          ,p.path_edges AS _gsql2rsql_path_edges
        FROM paths_1 p
        JOIN catalog.fraud.Account sink
          ON sink.id = p.end_node
        JOIN catalog.fraud.Account source
          ON source.id = p.start_node
        WHERE p.depth >= 3 AND p.depth <= 6
      ) AS _proj
    ) AS _proj
    ORDER BY total_amount DESC
    LIMIT 15;
    END
    ```

??? example "Procedural BFS — PySpark 4.2 (numbered_views)"
    ```sql
    BEGIN
      DECLARE bfs_depth_1 INT DEFAULT 0;
      DECLARE bfs_frontier_count_1 BIGINT DEFAULT 1;
      DECLARE bfs_union_sql_1 STRING DEFAULT '';
    
      CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_1_0 AS
      SELECT n.id AS node
      FROM catalog.fraud.Account n;
      
      CREATE OR REPLACE TEMPORARY VIEW bfs_visited_1_0 AS
      SELECT node FROM bfs_frontier_1_0;
      
      WHILE bfs_frontier_count_1 > 0 AND bfs_depth_1 < 6 DO
        SET bfs_depth_1 = bfs_depth_1 + 1;
      
        EXECUTE IMMEDIATE
          'CREATE OR REPLACE TEMPORARY VIEW bfs_edges_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
           SELECT e.source_account_id, e.target_account_id, e.amount, e.timestamp, e.target_account_id AS _next_node, ' || CAST(bfs_depth_1 AS STRING) || ' AS _bfs_depth FROM catalog.fraud.Transfer e INNER JOIN bfs_frontier_1_' || CAST(bfs_depth_1 - 1 AS STRING) || ' f ON e.source_account_id = f.node WHERE e.target_account_id NOT IN (SELECT node FROM bfs_visited_1_' || CAST(bfs_depth_1 - 1 AS STRING) || ') AND ((e.timestamp) > ((CURRENT_TIMESTAMP()) - (INTERVAL 7 DAY))) AND ((e.amount) > (1000))';
      
        EXECUTE IMMEDIATE
          'SET bfs_frontier_count_1 = (SELECT COUNT(1) FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING) || ')';
      
        IF bfs_frontier_count_1 > 0 THEN
      
          EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY VIEW bfs_visited_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
             SELECT node FROM bfs_visited_1_' || CAST(bfs_depth_1 - 1 AS STRING) || '
             UNION
             SELECT DISTINCT _next_node AS node
             FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
      
          EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
             SELECT DISTINCT _next_node AS node
             FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
      
          IF bfs_depth_1 >= 3 THEN
            IF bfs_union_sql_1 = '' THEN
              SET bfs_union_sql_1 =
                'SELECT source_account_id, target_account_id, amount, timestamp, _next_node AS end_node, _bfs_depth AS depth FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
            ELSE
              SET bfs_union_sql_1 = bfs_union_sql_1
                || ' UNION ALL SELECT source_account_id, target_account_id, amount, timestamp, _next_node AS end_node, _bfs_depth AS depth FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
            END IF;
          END IF;
      
        END IF;
      END WHILE;
      
      IF bfs_union_sql_1 != '' THEN
        EXECUTE IMMEDIATE
          'CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
           SELECT f0.node AS start_node, r.end_node, r.depth,
                  r.source_account_id, r.target_account_id, r.amount, r.timestamp,
                  ARRAY(NAMED_STRUCT(''source_account_id'', r.source_account_id, ''target_account_id'', r.target_account_id, ''amount'', r.amount, ''timestamp'', r.timestamp)) AS path_edges,
                  CAST(NULL AS ARRAY<STRING>) AS path
           FROM (' || bfs_union_sql_1 || ') r
           CROSS JOIN bfs_frontier_1_0 f0';
      ELSE
        CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
        SELECT
          CAST(NULL AS STRING) AS start_node,
          CAST(NULL AS STRING) AS end_node,
          CAST(NULL AS INT) AS depth,
          CAST(NULL AS STRING) AS source_account_id,
          CAST(NULL AS STRING) AS target_account_id,
          CAST(NULL AS STRING) AS amount,
          CAST(NULL AS STRING) AS timestamp,
          CAST(NULL AS ARRAY<STRUCT<source_account_id: STRING, target_account_id: STRING, amount: STRING, timestamp: STRING>>) AS path_edges,
          CAST(NULL AS ARRAY<STRING>) AS path
        WHERE 1 = 0;
      END IF;
    
      SELECT 
       _gsql2rsql_source_id AS id
      ,_gsql2rsql_sink_id AS id
      ,(SIZE(_gsql2rsql_path_id) - 1) AS hops
      ,total_amount AS total_amount
    FROM (
      SELECT 
         _gsql2rsql_source_id AS _gsql2rsql_source_id
        ,_gsql2rsql_sink_id AS _gsql2rsql_sink_id
        ,_gsql2rsql_path_id AS _gsql2rsql_path_id
        ,AGGREGATE(_gsql2rsql_path_edges, CAST(0 AS DOUBLE), (total, rel) -> (total) + (rel.amount)) AS total_amount
        ,_gsql2rsql_path_edges AS _gsql2rsql_path_edges
        ,_gsql2rsql_sink_days_since_creation AS _gsql2rsql_sink_days_since_creation
        ,_gsql2rsql_sink_default_date AS _gsql2rsql_sink_default_date
        ,_gsql2rsql_sink_holder_name AS _gsql2rsql_sink_holder_name
        ,_gsql2rsql_sink_home_country AS _gsql2rsql_sink_home_country
        ,_gsql2rsql_sink_kyc_status AS _gsql2rsql_sink_kyc_status
        ,_gsql2rsql_sink_risk_score AS _gsql2rsql_sink_risk_score
        ,_gsql2rsql_sink_status AS _gsql2rsql_sink_status
        ,_gsql2rsql_source_days_since_creation AS _gsql2rsql_source_days_since_creation
        ,_gsql2rsql_source_default_date AS _gsql2rsql_source_default_date
        ,_gsql2rsql_source_holder_name AS _gsql2rsql_source_holder_name
        ,_gsql2rsql_source_home_country AS _gsql2rsql_source_home_country
        ,_gsql2rsql_source_kyc_status AS _gsql2rsql_source_kyc_status
        ,_gsql2rsql_source_risk_score AS _gsql2rsql_source_risk_score
        ,_gsql2rsql_source_status AS _gsql2rsql_source_status
      FROM (
        SELECT
           sink.id AS _gsql2rsql_sink_id
          ,sink.holder_name AS _gsql2rsql_sink_holder_name
          ,sink.risk_score AS _gsql2rsql_sink_risk_score
          ,sink.status AS _gsql2rsql_sink_status
          ,sink.default_date AS _gsql2rsql_sink_default_date
          ,sink.home_country AS _gsql2rsql_sink_home_country
          ,sink.kyc_status AS _gsql2rsql_sink_kyc_status
          ,sink.days_since_creation AS _gsql2rsql_sink_days_since_creation
          ,source.id AS _gsql2rsql_source_id
          ,source.holder_name AS _gsql2rsql_source_holder_name
          ,source.risk_score AS _gsql2rsql_source_risk_score
          ,source.status AS _gsql2rsql_source_status
          ,source.default_date AS _gsql2rsql_source_default_date
          ,source.home_country AS _gsql2rsql_source_home_country
          ,source.kyc_status AS _gsql2rsql_source_kyc_status
          ,source.days_since_creation AS _gsql2rsql_source_days_since_creation
          ,p.start_node
          ,p.end_node
          ,p.depth
          ,p.path AS _gsql2rsql_path_id
          ,p.path_edges AS _gsql2rsql_path_edges
        FROM paths_1 p
        JOIN catalog.fraud.Account sink
          ON sink.id = p.end_node
        JOIN catalog.fraud.Account source
          ON source.id = p.start_node
        WHERE p.depth >= 3 AND p.depth <= 6
      ) AS _proj
    ) AS _proj
    ORDER BY total_amount DESC
    LIMIT 15;
    END
    ```

---

## 18. Find circular payment patterns indicating money laundering

**Source**: Fraud

???+ note "OpenCypher Query"
    ```cypher
    MATCH path = (a:Account)-[:TRANSFER*4..8]->(a)
    WHERE ALL(rel IN relationships(path) WHERE rel.amount > 500)
      AND LENGTH(path) >= 4
    WITH path, REDUCE(total = 0, rel IN relationships(path) | total + rel.amount) AS cycle_amount
    RETURN [node IN nodes(path) | node.id] AS cycle_accounts,
           LENGTH(path) AS cycle_length,
           cycle_amount
    ORDER BY cycle_amount DESC
    LIMIT 10
    ```

??? example "Procedural BFS — Databricks (temp_tables)"
    ```sql
    BEGIN
      DECLARE current_depth_1 INT DEFAULT 0;
      DECLARE rows_in_frontier_1 BIGINT DEFAULT 1;
    
      DROP TEMPORARY TABLE IF EXISTS bfs_visited_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_frontier_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_result_1;
      DROP TEMPORARY TABLE IF EXISTS bfs_frontier_1_init;
      CREATE TEMPORARY TABLE bfs_visited_1 (node STRING);
      CREATE TEMPORARY TABLE bfs_frontier_1 AS
      SELECT n.id AS node
      FROM catalog.fraud.Account n;
      INSERT INTO bfs_visited_1
      SELECT node FROM bfs_frontier_1;
      CREATE TEMPORARY TABLE bfs_result_1 (source_account_id STRING, target_account_id STRING, amount STRING, timestamp STRING, _next_node STRING, _bfs_depth INT);
      CREATE TEMPORARY TABLE bfs_frontier_1_init AS
      SELECT node FROM bfs_frontier_1;
      
      WHILE rows_in_frontier_1 > 0 AND current_depth_1 < 8 DO
        SET current_depth_1 = current_depth_1 + 1;
      
        DROP TEMPORARY TABLE IF EXISTS bfs_edges_1;
        CREATE TEMPORARY TABLE bfs_edges_1 AS
        SELECT e.source_account_id, e.target_account_id, e.amount, e.timestamp, e.target_account_id AS _next_node
      FROM catalog.fraud.Transfer e
      INNER JOIN bfs_frontier_1 f ON e.source_account_id = f.node
      WHERE e.target_account_id NOT IN (SELECT node FROM bfs_visited_1) AND (e.amount) > (500);
      
        SET rows_in_frontier_1 = (SELECT COUNT(1) FROM bfs_edges_1);
      
        IF rows_in_frontier_1 > 0 THEN
          INSERT INTO bfs_visited_1
          SELECT DISTINCT _next_node FROM bfs_edges_1;
          DROP TEMPORARY TABLE bfs_frontier_1;
          CREATE TEMPORARY TABLE bfs_frontier_1 AS
          SELECT DISTINCT _next_node AS node FROM bfs_edges_1;
          IF current_depth_1 >= 4 THEN
            INSERT INTO bfs_result_1
            SELECT *, current_depth_1 AS _bfs_depth FROM bfs_edges_1;
          END IF;
        END IF;
      END WHILE;
      
      CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
      SELECT f0.node AS start_node, r._next_node AS end_node, r._bfs_depth AS depth,
             r.source_account_id, r.target_account_id, r.amount, r.timestamp,
             ARRAY(NAMED_STRUCT('source_account_id', r.source_account_id, 'target_account_id', r.target_account_id, 'amount', r.amount, 'timestamp', r.timestamp)) AS path_edges,
             CAST(NULL AS ARRAY<STRING>) AS path
      FROM bfs_result_1 r
      CROSS JOIN bfs_frontier_1_init f0;
    
      SELECT 
       _gsql2rsql_path_id AS cycle_accounts
      ,(SIZE(_gsql2rsql_path_id) - 1) AS cycle_length
      ,cycle_amount AS cycle_amount
    FROM (
      SELECT 
         _gsql2rsql_path_id AS _gsql2rsql_path_id
        ,AGGREGATE(_gsql2rsql_path_edges, CAST(0 AS DOUBLE), (total, rel) -> (total) + (rel.amount)) AS cycle_amount
        ,_gsql2rsql_path_edges AS _gsql2rsql_path_edges
      FROM (
        SELECT
           p.start_node
          ,p.end_node
          ,p.depth
          ,p.path AS _gsql2rsql_path_id
          ,p.path_edges AS _gsql2rsql_path_edges
        FROM paths_1 p
        WHERE p.depth >= 4 AND p.depth <= 8 AND p.start_node = p.end_node
      ) AS _proj
      WHERE ((SIZE(_gsql2rsql_path_id) - 1)) >= (4)
    ) AS _proj
    ORDER BY cycle_amount DESC
    LIMIT 10;
    END
    ```

??? example "Procedural BFS — PySpark 4.2 (numbered_views)"
    ```sql
    BEGIN
      DECLARE bfs_depth_1 INT DEFAULT 0;
      DECLARE bfs_frontier_count_1 BIGINT DEFAULT 1;
      DECLARE bfs_union_sql_1 STRING DEFAULT '';
    
      CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_1_0 AS
      SELECT n.id AS node
      FROM catalog.fraud.Account n;
      
      CREATE OR REPLACE TEMPORARY VIEW bfs_visited_1_0 AS
      SELECT node FROM bfs_frontier_1_0;
      
      WHILE bfs_frontier_count_1 > 0 AND bfs_depth_1 < 8 DO
        SET bfs_depth_1 = bfs_depth_1 + 1;
      
        EXECUTE IMMEDIATE
          'CREATE OR REPLACE TEMPORARY VIEW bfs_edges_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
           SELECT e.source_account_id, e.target_account_id, e.amount, e.timestamp, e.target_account_id AS _next_node, ' || CAST(bfs_depth_1 AS STRING) || ' AS _bfs_depth FROM catalog.fraud.Transfer e INNER JOIN bfs_frontier_1_' || CAST(bfs_depth_1 - 1 AS STRING) || ' f ON e.source_account_id = f.node WHERE e.target_account_id NOT IN (SELECT node FROM bfs_visited_1_' || CAST(bfs_depth_1 - 1 AS STRING) || ') AND (e.amount) > (500)';
      
        EXECUTE IMMEDIATE
          'SET bfs_frontier_count_1 = (SELECT COUNT(1) FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING) || ')';
      
        IF bfs_frontier_count_1 > 0 THEN
      
          EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY VIEW bfs_visited_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
             SELECT node FROM bfs_visited_1_' || CAST(bfs_depth_1 - 1 AS STRING) || '
             UNION
             SELECT DISTINCT _next_node AS node
             FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
      
          EXECUTE IMMEDIATE
            'CREATE OR REPLACE TEMPORARY VIEW bfs_frontier_1_' || CAST(bfs_depth_1 AS STRING) || ' AS
             SELECT DISTINCT _next_node AS node
             FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
      
          IF bfs_depth_1 >= 4 THEN
            IF bfs_union_sql_1 = '' THEN
              SET bfs_union_sql_1 =
                'SELECT source_account_id, target_account_id, amount, timestamp, _next_node AS end_node, _bfs_depth AS depth FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
            ELSE
              SET bfs_union_sql_1 = bfs_union_sql_1
                || ' UNION ALL SELECT source_account_id, target_account_id, amount, timestamp, _next_node AS end_node, _bfs_depth AS depth FROM bfs_edges_1_' || CAST(bfs_depth_1 AS STRING);
            END IF;
          END IF;
      
        END IF;
      END WHILE;
      
      IF bfs_union_sql_1 != '' THEN
        EXECUTE IMMEDIATE
          'CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
           SELECT f0.node AS start_node, r.end_node, r.depth,
                  r.source_account_id, r.target_account_id, r.amount, r.timestamp,
                  ARRAY(NAMED_STRUCT(''source_account_id'', r.source_account_id, ''target_account_id'', r.target_account_id, ''amount'', r.amount, ''timestamp'', r.timestamp)) AS path_edges,
                  CAST(NULL AS ARRAY<STRING>) AS path
           FROM (' || bfs_union_sql_1 || ') r
           CROSS JOIN bfs_frontier_1_0 f0';
      ELSE
        CREATE OR REPLACE TEMPORARY VIEW paths_1 AS
        SELECT
          CAST(NULL AS STRING) AS start_node,
          CAST(NULL AS STRING) AS end_node,
          CAST(NULL AS INT) AS depth,
          CAST(NULL AS STRING) AS source_account_id,
          CAST(NULL AS STRING) AS target_account_id,
          CAST(NULL AS STRING) AS amount,
          CAST(NULL AS STRING) AS timestamp,
          CAST(NULL AS ARRAY<STRUCT<source_account_id: STRING, target_account_id: STRING, amount: STRING, timestamp: STRING>>) AS path_edges,
          CAST(NULL AS ARRAY<STRING>) AS path
        WHERE 1 = 0;
      END IF;
    
      SELECT 
       _gsql2rsql_path_id AS cycle_accounts
      ,(SIZE(_gsql2rsql_path_id) - 1) AS cycle_length
      ,cycle_amount AS cycle_amount
    FROM (
      SELECT 
         _gsql2rsql_path_id AS _gsql2rsql_path_id
        ,AGGREGATE(_gsql2rsql_path_edges, CAST(0 AS DOUBLE), (total, rel) -> (total) + (rel.amount)) AS cycle_amount
        ,_gsql2rsql_path_edges AS _gsql2rsql_path_edges
      FROM (
        SELECT
           p.start_node
          ,p.end_node
          ,p.depth
          ,p.path AS _gsql2rsql_path_id
          ,p.path_edges AS _gsql2rsql_path_edges
        FROM paths_1 p
        WHERE p.depth >= 4 AND p.depth <= 8 AND p.start_node = p.end_node
      ) AS _proj
      WHERE ((SIZE(_gsql2rsql_path_id) - 1)) >= (4)
    ) AS _proj
    ORDER BY cycle_amount DESC
    LIMIT 10;
    END
    ```

---
