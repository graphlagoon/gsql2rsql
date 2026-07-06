# gsql2rsql Architecture Audit Report

**Project:** gsql2rsql — OpenCypher → Databricks Spark SQL transpiler
**Version audited:** 0.9.3
**Date:** 2026-02-07
**Auditor:** Claude Opus 4.6 (automated code-pattern forensic analysis)

---

## Repository Artifacts Used

| Artifact | Path | Status |
|----------|------|--------|
| Parser/grammar | `src/gsql2rsql/parser/`, `CypherParser.g4` | Read in full |
| AST definitions | `src/gsql2rsql/parser/ast.py` (846 lines) | Read in full |
| Visitor | `src/gsql2rsql/parser/visitor.py` (1,597 lines) | Read in full |
| Planner | `src/gsql2rsql/planner/` (14 modules, ~9,811 lines) | Read in full |
| Renderer | `src/gsql2rsql/renderer/` (8 modules, ~6,760 lines) | Read in full |
| Common | `src/gsql2rsql/common/` (5 modules) | Read in full |
| Graph context | `src/gsql2rsql/graph_context.py` (478 lines) | Read in full |
| Tests | `tests/` (109 files, 1,407 tests) | Surveyed |
| CI/CD | `.github/workflows/ci.yml` | Read in full |
| Build | `pyproject.toml`, `Makefile` | Read in full |

**Confidence level for all claims:** **High** (full source access)

---

## 0 — Executive Summary

gsql2rsql is a well-engineered Python transpiler that converts OpenCypher graph queries into Databricks Spark SQL. The architecture follows a classic compiler pipeline — ANTLR-generated parser → custom AST → logical plan with relational operators → optimization passes → SQL string emission — and does so with reasonable separation of concerns. The codebase is at **v0.9.3 (Alpha)** with 1,407 tests (1,378 passing in the non-PySpark suite), mypy strict mode, pyright, ruff, and CI on GitHub Actions. The planner (9,811 lines) is the heaviest layer, handling graph-to-relational lowering, symbol table management, column resolution, predicate pushdown, dead table elimination, and bidirectional BFS optimization. The renderer (6,760 lines) was recently decomposed from a 5,964-line monolith into 6 focused modules.

**Top 5 recommendations:**

1. **Introduce a SQL AST / IR for code generation** — the renderer uses raw string concatenation, which is fragile and blocks dialect portability, SQL-level optimizations, and semantic validation of output.
2. **Extract optimization passes into a first-class pass manager** — optimizations are currently scattered across `subquery_optimizer.py` (1,737 lines), `bidirectional_optimizer.py`, `dead_table_eliminator.py`, and inline logic in `logical_plan.py`, with no uniform interface or ordering guarantee.
3. **Add property-based testing (Hypothesis)** — the test suite is strong on golden-file regression but has no generative testing to discover edge-case translation bugs.
4. **Formalize the Graph→Relational lowering as a distinct phase** — the boundary between "planning" and "lowering" is blurred; `match_tree.py`, `recursive_traversal.py`, and `aggregation_boundary.py` perform lowering but live alongside semantic analysis modules.
5. **Enable ruff lint in CI** — it's commented out in `ci.yml` (line 34-35), leaving the formatter/linter unenforced.

---

## 1 — Scope & Assumptions

- **Strict constraint confirmed:** Only OpenCypher → Spark SQL (Databricks dialect). No other targets.
- **Runtime:** Python 3.11+, PySpark 3.5+ (4.1 in dev deps). No Scala components.
- **Spark version:** Databricks SQL with `WITH RECURSIVE` support (recent Databricks Runtime feature). The transpiler uses `WITH RECURSIVE` CTEs for variable-length paths, which is a Databricks-specific extension not available in vanilla Spark SQL.
- **Expected query shapes:** Single-hop patterns, multi-hop VLP with cycle detection, undirected edges, aggregations, OPTIONAL MATCH, UNION, UNWIND, list comprehensions, EXISTS subqueries, path functions, inline property filters.
- **Schema model:** "Triple Store" — one nodes table and one edges table with type discriminator columns, not a property graph with per-label tables (though the schema provider can model both).

---

## 2 — High-Level Architecture Review

### Data Flow

```
┌────────────┐    ┌──────────┐    ┌──────────────┐    ┌────────────┐    ┌───────────┐
│  Cypher    │    │  ANTLR4  │    │   Custom     │    │  Logical   │    │  Optimized│
│  Query     │───>│  Lexer + │───>│   AST        │───>│  Plan      │───>│  Plan     │
│  (string)  │    │  Parser  │    │  (dataclass  │    │  (operator │    │           │
│            │    │          │    │   tree)      │    │   DAG)     │    │           │
└────────────┘    └──────────┘    └──────────────┘    └────────────┘    └───────────┘
                                                                              │
                                                                              v
                       ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
                       │  Databricks  │<───│  SQL String  │<───│  Column      │
                       │  SQL         │    │  Renderer    │    │  Resolution  │
                       │  (string)    │    │              │    │  (semantic)  │
                       └──────────────┘    └──────────────┘    └──────────────┘
```

### Component Responsibilities

| Component | Module(s) | Responsibility |
|-----------|-----------|----------------|
| **Lexer** | `parser/grammar/CypherLexer.py` (generated) | Tokenize Cypher input |
| **Parser** | `parser/grammar/CypherParser.py` (generated) | Build ANTLR parse tree |
| **Visitor** | `parser/visitor.py` | Convert parse tree → custom AST |
| **AST** | `parser/ast.py`, `parser/operators.py` | Immutable tree of Cypher constructs |
| **Planner** | `planner/logical_plan.py` | Orchestrate plan construction |
| **Lowering** | `planner/match_tree.py`, `planner/recursive_traversal.py`, `planner/aggregation_boundary.py` | Graph patterns → relational operators |
| **Operators** | `planner/operators.py` (1,857 lines) | Relational operator definitions (DataSource, Join, Selection, Projection, RecursiveTraversal, Unwind, SetOp, AggregationBoundary) |
| **Schema/Types** | `planner/schema.py`, `planner/data_types.py` | Authoritative type system (PrimitiveType, StructType, ArrayType) |
| **Symbol Table** | `planner/symbol_table.py` | Variable scoping and definitions |
| **Column Resolution** | `planner/column_resolver.py`, `planner/column_ref.py` | Semantic validation, reference resolution |
| **Optimizers** | `planner/subquery_optimizer.py`, `planner/dead_table_eliminator.py`, `planner/bidirectional_optimizer.py`, `planner/path_analyzer.py` | Plan transformations |
| **Renderer** | `renderer/sql_renderer.py` (orchestrator) | Operator → SQL string dispatch |
| **Expression Renderer** | `renderer/expression_renderer.py` | All expression → SQL translation |
| **Join Renderer** | `renderer/join_renderer.py` | JOIN clause generation |
| **CTE Renderer** | `renderer/recursive_cte_renderer.py` | WITH RECURSIVE CTE generation |
| **Render Context** | `renderer/render_context.py` | Shared state for sub-renderers |
| **Dialect** | `renderer/dialect.py` | Databricks SQL constants and patterns |
| **Schema Provider** | `renderer/schema_provider.py` | SQL table/column mapping |
| **Graph Context** | `graph_context.py` | High-level API combining all phases |

---

## 3 — Phase-by-Phase Technical Audit

### Phase 1: Lexing / Tokenization

**Exists?** Yes — `src/gsql2rsql/parser/grammar/CypherLexer.py` (auto-generated, ~52 KB)

**Current strengths:**
- ANTLR4-generated from `Cypher.g4` (812 lines, Apache 2.0 from Neo4j openCypher spec). Complete openCypher coverage.
- 128 named tokens including Unicode arrow variants (U+27E8, U+3008, etc.).
- Case-insensitive keywords via ANTLR grammar fragments.

**Current weaknesses:**
- No custom error listener attached — ANTLR's default `ConsoleErrorListener` writes to stderr. The parser (in `opencypher_parser.py:52-60`) does not suppress or redirect ANTLR lexer errors before the visitor runs.
- The generated `.py` file (52 KB) is checked into source control. Any grammar change requires re-running `java -jar antlr-4.13.1-complete.jar` manually.

**Code patterns currently used:**

| Pattern | Where | Pros | Cons |
|---------|-------|------|------|
| **Generated table-driven lexer** (ANTLR4 ATN) | `grammar/CypherLexer.py` | Correct by construction from grammar; handles Unicode, keywords, string escaping automatically | Cannot customize error recovery; 52 KB generated file in source control |
| **Lazy import** | `opencypher_parser.py:40-50` | Avoids loading ANTLR at import time | Late failure if ANTLR not installed |

**Important missing patterns:**
- **Custom error listener** — would allow structured error reporting with line/column on lex errors instead of raw stderr output. ANTLR provides `BaseErrorListener` for this.

**Micro-actions:**
1. Add a `SyntaxErrorCollector(ErrorListener)` class in `parser/error_listener.py` that collects errors into a list.
2. Attach it in `opencypher_parser.py`: `lexer.removeErrorListeners(); lexer.addErrorListener(collector)`.
3. After parsing, raise `TranspilerSyntaxErrorException` with collected errors including line/column.

**Priority:** Medium | **Effort:** Small | **Confidence:** High

---

### Phase 2: Parsing (Syntactic)

**Exists?** Yes — `src/gsql2rsql/parser/grammar/CypherParser.py` (auto-generated, ~375 KB)

**Current strengths:**
- Complete openCypher coverage — patterns, expressions (precedence-climbing), OPTIONAL MATCH, UNWIND, UNION, EXISTS subqueries, list comprehensions, REDUCE, CASE, named paths.
- Grammar is derived from the official Neo4j openCypher spec, ensuring compatibility.

**Current weaknesses:**
- Same lack of custom error listener as the lexer.
- Grammar includes updating clauses (`CREATE`, `MERGE`, `DELETE`, `SET`, `REMOVE`) that the planner/renderer do not support. The parser accepts them silently; only the planner or renderer will fail later. This violates fail-fast.
- `CypherParser.py` (375 KB) is checked in; should be `.gitignore`d with a regeneration step in CI.

**Code patterns currently used:**

| Pattern | Where | Pros | Cons |
|---------|-------|------|------|
| **Generated LL(*) parser** (ANTLR4 ALL(*)) | `grammar/CypherParser.py` | Handles full openCypher grammar including ambiguous patterns | 375 KB generated file; runtime overhead of Python ANTLR vs native |
| **Visitor dispatch** (custom, not ANTLR's) | `parser/visitor.py:1-1597` | Produces clean custom AST; can add validation logic per rule | 1,597 lines; mixes AST construction with validation; not extensible for new grammar rules without touching the monolith |

**Important missing patterns:**
- **Error recovery strategy** — ANTLR supports `DefaultErrorStrategy` and `BailErrorStrategy`. The current code uses the default (which tries to recover), potentially producing partial parse trees that the visitor silently consumes.
- **Grammar versioning / compatibility checks** — no assertion that the grammar version matches the runtime expectations.

**Micro-actions:**
1. Add `parser.setErrorHandler(BailErrorStrategy())` in `opencypher_parser.py` to fail fast on parse errors.
2. Add a grammar version constant in `grammar/__init__.py` and check it in the parser entry point.
3. Add `.gitignore` entries for generated files (`CypherLexer.py`, `CypherParser.py`, `CypherListener.py`, `CypherVisitor.py`, `*.interp`, `*.tokens`) and add a `make grammar` step in CI.

**Priority:** Medium | **Effort:** Small | **Confidence:** High

---

### Phase 3: AST Representation & Transformations

**Exists?** Yes — `src/gsql2rsql/parser/ast.py` (846 lines)

**Current strengths:**
- Clean `@dataclass` hierarchy with `TreeNode` base class providing `children`, `dump_tree()`, `get_children_of_type()`.
- 14 expression node types covering the full Cypher expression model.
- Graph pattern entities (`NodeEntity`, `RelationshipEntity`) are well-modeled with direction, hops, inline properties.
- `PartialQueryNode` accurately models the WITH-chaining pattern of Cypher.

**Current weaknesses:**
- **Not frozen:** Dataclasses are mutable despite being documented as "designed immutable." The planner mutates AST nodes (e.g., `match_clause.where_expression = remove_pushed_predicates(...)` in `recursive_traversal.py`). This is a correctness risk — shared AST nodes could be mutated through aliasing.
- **No `__eq__`/`__hash__` consistency:** Because dataclasses aren't frozen, hash is based on `id()` not structure, preventing structural comparison in tests.
- **No source location tracking:** AST nodes don't store line/column from the parse tree. Error messages from the planner/resolver can't point to the exact Cypher source location.

**Code patterns currently used:**

| Pattern | Where | Pros | Cons |
|---------|-------|------|------|
| **Composite AST nodes** (dataclass tree) | `parser/ast.py` | Type-safe, self-documenting, IDE-friendly | Mutable despite intent; no source locations |
| **Abstract base with `children` property** | `TreeNode` (line 24-31) | Enables generic tree traversal; `dump_tree()` for debugging | No visitor accept/dispatch protocol; traversal requires `isinstance` checks |
| **Enum-based operators** | `parser/operators.py` (BinaryOperator, AggregationFunction, Function) | Exhaustive matching; type-safe | Function enum has 50+ members — could be split by category |

**Important missing patterns:**
- **Source location span** (`SourceSpan(start_line, start_col, end_line, end_col)`) — critical for user-facing error messages. Would require the visitor to extract positions from ANTLR context objects (`ctx.start.line`, `ctx.start.column`).
- **Visitor/double-dispatch protocol** — the AST has no `accept(visitor)` method. All traversal is ad-hoc `isinstance` checks. A proper visitor protocol would enable clean AST transformations.
- **Frozen dataclasses** — would catch accidental mutation at the cost of requiring copy-on-write (`dataclasses.replace(node, field=new_value)`) in the planner.

**Concrete micro-actions:**

Before (current):
```python
@dataclass
class QueryExpressionBinary(QueryExpression):
    operator: BinaryOperatorInfo
    left_expression: QueryExpression
    right_expression: QueryExpression
```

After (with source locations and freeze):
```python
@dataclass(frozen=True)
class SourceSpan:
    start_line: int
    start_col: int
    end_line: int
    end_col: int

@dataclass(frozen=True)
class QueryExpressionBinary(QueryExpression):
    operator: BinaryOperatorInfo
    left_expression: QueryExpression
    right_expression: QueryExpression
    span: SourceSpan | None = None
```

**Priority:** High | **Effort:** Medium | **Confidence:** High

---

### Phase 4: Semantic Analysis / Name & Type Resolution

**Exists?** Yes — `planner/column_resolver.py` (1,383 lines), `planner/symbol_table.py` (431 lines), `planner/column_ref.py` (253 lines)

**Current strengths:**
- **Full symbol table with scoping:** `SymbolTable` class tracks variable definitions across scopes (global, WITH boundaries, aggregation boundaries). `enter_scope()`, `exit_scope()`, `clear_scope_for_aggregation()` correctly model Cypher's scope semantics.
- **Rich error diagnostics:** `ColumnResolutionError` provides boxed error output with query pointer (▲), available variables table, out-of-scope hints, Levenshtein-based "did you mean?" suggestions.
- **Two-phase resolution:** Phase 1 builds the symbol table; Phase 2 resolves expressions against it. This handles forward references within a single query part.
- **Authoritative type system:** `DataType` hierarchy (`PrimitiveType`, `StructType`, `ArrayType`) with frozen dataclasses. Operators declare output types; resolution trusts these declarations.

**Current weaknesses:**
- **No type-checking beyond schema properties:** The resolver validates that `a.name` exists on entity `a` but doesn't type-check expressions like `a.age + "hello"` (int + string). Type errors are deferred to Spark runtime.
- **Column resolver is 1,383 lines** — a single large file mixing symbol table building, expression resolution, entity projection resolution, and VLP type inference. Could benefit from decomposition.
- **`ResolutionResult` coupling:** The result object is consumed directly by the renderer, creating a tight coupling between planner and renderer concerns.

**Code patterns currently used:**

| Pattern | Where | Pros | Cons |
|---------|-------|------|------|
| **Symbol table with scope stack** | `symbol_table.py` | Correct Cypher scope modeling; tracks out-of-scope reasons for diagnostics | No scope visualization/debugging tool beyond `dump()` |
| **Two-phase resolution** | `column_resolver.py` | Handles forward references; separates building from validation | Two full traversals of the operator DAG |
| **Diagnostic collector** | `exceptions.py:69-281` | Rich error context with pointer arrows, tables, suggestions | Only for column resolution errors; parser errors don't get this treatment |
| **Levenshtein distance** | `exceptions.py:283-313` | "Did you mean X?" suggestions | O(n*m) per comparison; fine for variable counts |

**Important missing patterns:**
- **Expression type inference pass** — would catch type mismatches before SQL emission. Not critical for correctness (Spark will catch at runtime) but improves developer experience.
- **Separate resolution result for planner vs renderer** — currently the renderer depends on planner internals (`ResolvedColumnRef.sql_column_name`).

**Micro-actions:**
1. Split `column_resolver.py` into `symbol_builder.py` (Phase 1) and `expression_resolver.py` (Phase 2).
2. Add a lightweight `TypeChecker` pass after resolution that validates binary operator type compatibility.
3. Add `--explain-resolution` CLI flag to dump the full `ResolutionResult` for debugging.

**Priority:** Medium | **Effort:** Medium | **Confidence:** High

---

### Phase 5: Intermediate Representation (IR) / Lowering (Graph → Relational)

**Exists?** Yes — `planner/operators.py` (1,857 lines) defines the relational operator DAG. Lowering happens in `planner/match_tree.py` (586 lines), `planner/recursive_traversal.py` (513 lines), `planner/aggregation_boundary.py` (448 lines).

**Current strengths:**
- **Rich operator set:** DataSource, Join (INNER/LEFT/CROSS), Selection, Projection, RecursiveTraversal, Unwind, AggregationBoundary, SetOp. Covers all supported Cypher constructs.
- **Authoritative schema propagation:** Each operator declares input/output schema; `propagate_data_types_for_out_schema()` computes output types. ColumnResolver trusts these.
- **Join semantics well-modeled:** `JoinKeyPairType` enum (SOURCE, SINK, EITHER, BOTH, NODE_ID, EITHER_AS_SOURCE, EITHER_AS_SINK) captures the full range of graph-join semantics.
- **VLP operator is comprehensive:** `RecursiveTraversalOperator` (~480 lines) encodes path variable, edge collection, predicate pushdown, bidirectional BFS mode, direction handling, and visited-set cycle detection.

**Current weaknesses:**
- **Lowering is not a distinct phase:** `match_tree.py`, `recursive_traversal.py`, and `aggregation_boundary.py` are called from `logical_plan.py` but there's no explicit "lowering pass" concept. They run interleaved with plan construction.
- **`operators.py` at 1,857 lines** — all 8 operator types in one file. Each operator has complex `bind()`, `propagate_data_types_*()`, and `required_input_symbols()` logic.
- **Mutable operator DAG:** Operators have public `graph_in_operators`/`graph_out_operators` lists that optimizers mutate in-place. No immutability guarantee; hard to snapshot plan states for debugging.

**Code patterns currently used:**

| Pattern | Where | Pros | Cons |
|---------|-------|------|------|
| **Operator DAG** (mutable) | `operators.py` | Standard relational algebra representation; enables plan rewrites | Mutation makes debugging hard; no plan snapshots |
| **Schema propagation** (push-based) | `LogicalOperator.propagate_data_types_*()` | Each operator is self-contained for type computation | Tightly coupled to operator internals; no external type inference |
| **Join key pair classification** | `JoinKeyPairType` enum | Exhaustive graph-join modeling | 7 enum values with subtle semantics; documentation-heavy |
| **DataSource binding** | `DataSourceOperator.bind()` | Graph schema → SQL schema resolution in one step | Mixes two concerns: entity resolution and SQL table binding |

**Important missing patterns:**
- **Explicit lowering pass** — a `GraphToRelational` transformer that takes the AST (or a graph-level IR) and produces the operator DAG. Currently, lowering is ad-hoc.
- **Plan immutability / copy-on-write** — would enable plan comparison, debugging, and safe parallel optimization.
- **Operator decomposition** — `operators.py` should be split into `operators/base.py`, `operators/data_source.py`, `operators/join.py`, `operators/recursive.py`, etc.

**Micro-actions:**
1. Create `planner/operators/` package with one file per operator type.
2. Extract `match_tree.py` + `recursive_traversal.py` + `aggregation_boundary.py` into a `planner/lowering/` package with a unified `lower_query(ast, schema) -> LogicalPlan` entry point.
3. Add a `plan.snapshot() -> FrozenPlan` method for debugging (captures operator graph state).

**Priority:** High | **Effort:** Large | **Confidence:** High

---

### Phase 6: Optimization & Rewrite Passes

**Exists?** Yes — `planner/subquery_optimizer.py` (1,737 lines), `planner/dead_table_eliminator.py` (668 lines), `planner/bidirectional_optimizer.py` (306 lines), `planner/path_analyzer.py` (620 lines)

**Current strengths:**
- **Conservative approach:** SubqueryFlatteningOptimizer only flattens patterns proven 100% safe. This is correct — transpiler correctness > optimization aggressiveness.
- **Predicate pushdown:** Selection operators merged into DataSource operators when safe; VLP edge predicates pushed into CTE base/recursive cases.
- **Dead table elimination:** Removes unnecessary JOINs when entities aren't referenced downstream. Configurable via `optimize_dead_tables` flag.
- **Bidirectional BFS:** Detects VLP queries with equality filters on both endpoints and enables meeting-in-the-middle traversal. Two modes: recursive CTEs and unrolled CTEs.
- **Path usage analysis:** `PathExpressionAnalyzer` determines whether `nodes(path)` or `relationships(path)` is actually used, avoiding unnecessary path collection in CTEs.

**Current weaknesses:**
- **No pass manager:** Optimizations are called sequentially in `graph_context.py:408-422` and `subquery_optimizer.py:optimize_plan()`. No framework for pass ordering, pass dependencies, or fixed-point iteration.
- **`subquery_optimizer.py` at 1,737 lines** — contains 4 distinct optimizer classes (`SelectionPushdownOptimizer`, `SubqueryFlatteningOptimizer`, `DeadTableEliminationOptimizer`, and the `optimize_plan` orchestrator) in one file.
- **No cost model:** Optimizations are rule-based. For example, the undirected edge strategy (`union_edges` vs `or_join`) is a config flag, not a cost-based decision.

**Code patterns currently used:**

| Pattern | Where | Pros | Cons |
|---------|-------|------|------|
| **Rule-based rewriting** | All optimizer files | Predictable, easy to debug | Cannot adapt to data statistics |
| **Single-pass traversal** | Each optimizer walks the DAG once | Simple implementation | No fixed-point iteration; some optimizations might enable others |
| **Worklist pattern** (implicit) | `SubqueryFlatteningOptimizer` | Processes operators in topological order | Not a formal worklist; ordering is implicit |

**Important missing patterns:**
- **Pass manager** — a `PassManager` class that registers passes, manages ordering, and supports `run_until_fixpoint()`.
- **Pass interface** — `class OptimizationPass(ABC): def run(self, plan: LogicalPlan) -> bool` (returns True if plan was modified).
- **Cost estimation** — even a simple heuristic model (e.g., "UNION ALL of two scans < OR-join with nested loop") would improve strategy selection.

**Micro-actions:**

```python
# Before: ad-hoc orchestration in graph_context.py
optimize_plan(plan, enabled=True, pushdown_enabled=True, ...)
apply_bidirectional_optimization(plan, graph_schema=self._schema, mode=mode)

# After: pass manager
class OptimizationPass(ABC):
    @abstractmethod
    def run(self, plan: LogicalPlan) -> bool: ...

class PassManager:
    def __init__(self): self._passes: list[OptimizationPass] = []
    def add(self, p: OptimizationPass): self._passes.append(p)
    def run_all(self, plan: LogicalPlan):
        for p in self._passes: p.run(plan)

pm = PassManager()
pm.add(SelectionPushdownPass())
pm.add(SubqueryFlatteningPass())
pm.add(DeadTableEliminationPass())
pm.add(BidirectionalBFSPass(schema, mode))
pm.run_all(plan)
```

**Priority:** Medium | **Effort:** Medium | **Confidence:** High

---

### Phase 7: Recursion / Variable-Length Paths Handling

**Exists?** Yes — `planner/recursive_traversal.py` (513 lines) + `renderer/recursive_cte_renderer.py` (1,350 lines)

**Current strengths:**
- **WITH RECURSIVE CTE approach:** Full implementation of iterative path expansion via base case (depth=1 edges) + recursive case (extend by one hop) + cycle detection (`NOT ARRAY_CONTAINS(visited, target_id)`).
- **Path accumulation:** Collects both node IDs (`path` array) and edge structs (`path_edges` array) through the CTE.
- **Zero-length paths:** Supports `*0..N` by emitting a base case that includes the source node itself.
- **Predicate pushdown into CTE:** `ALL(r IN relationships(path) WHERE r.amount > 1000)` pushes the filter into both base and recursive cases of the CTE.
- **Bidirectional BFS:** Two strategies for meeting-in-the-middle:
  - `recursive`: Two parallel CTEs (forward and backward) joined at the meeting depth.
  - `unrolling`: Unrolled CTE levels (fwd0, fwd1, ..., bwd0, bwd1, ...) for shallow depths.
- **Undirected traversal:** Internal UNION ALL of forward and backward edges.

**Current weaknesses:**
- **Databricks-specific:** `WITH RECURSIVE` is a Databricks SQL extension. Standard Spark SQL does not support it. This limits portability to vanilla Spark.
- **No max-depth safety limit:** If `max_hops` is very large (or unbounded), the CTE could recurse deeply and consume excessive resources. No configurable safety cap.
- **CTE renderer at 1,350 lines:** Complex file with multiple code paths for forward/backward/undirected × standard/bidirectional × base/recursive cases.
- **`EdgeInfo` dataclass** (11 fields) is a parameter object that replaced closure captures, but its large surface area suggests the CTE renderer could be decomposed further.

**Code patterns currently used:**

| Pattern | Where | Pros | Cons |
|---------|-------|------|------|
| **WITH RECURSIVE CTE** | `recursive_cte_renderer.py` | Native SQL recursion; correct BFS semantics | Databricks-only; no vanilla Spark fallback |
| **Visited-set cycle detection** | CTE WHERE clause | Prevents infinite loops; correct for arbitrary graphs | O(path_length) per step via `ARRAY_CONTAINS` |
| **Template method** (skeleton + parts) | `_append_base_cases()` + `_append_recursive_cases()` | Clear separation of CTE structure | 6+ branch combinations make the code hard to follow |
| **Parameter object** (`EdgeInfo`) | `recursive_cte_renderer.py:36-58` | Replaces closure capture; explicit dependencies | 11 fields — suggests too many concerns in one renderer |

**Detailed VLP strategies discussion — see Section 6 below.**

**Priority:** High (correctness-critical) | **Effort:** Large | **Confidence:** High

---

### Phase 8: Code Generation (SQL String Emission)

**Exists?** Yes — `renderer/` package (6,760 lines across 8 files)

**Current strengths:**
- **Modular decomposition:** Post-refactoring architecture with `sql_renderer.py` (orchestrator), `expression_renderer.py`, `join_renderer.py`, `recursive_cte_renderer.py`, `render_context.py`, `dialect.py`.
- **Anti-circular-import pattern:** `RenderContext` shared state eliminates back-references from sub-renderers to `SQLRenderer`.
- **Callback injection:** `_render_operator` callback passed to sub-renderers for recursive rendering without circular imports.
- **Column pruning:** `_collect_required_columns()` pre-analyzes which columns are needed, enabling intermediate subqueries to select only required columns.
- **Comprehensive function mapping:** 100+ Cypher functions mapped to Databricks SQL equivalents in `expression_renderer.py`.
- **DISTINCT workaround:** `GROUP BY TO_JSON(NAMED_STRUCT('_', col))` for Spark's inability to `SELECT DISTINCT` on MAP types.

**Current weaknesses:**
- **Raw string concatenation:** All SQL is built via `lines.append(f"...")` and `"\n".join(lines)`. No intermediate SQL AST, no parameterized queries, no syntactic validation of output.
- **`expression_renderer.py` at 2,092 lines:** `_render_expression()` is a 176-line `isinstance` chain; `_render_function()` has 100+ `elif` branches. High cyclomatic complexity.
- **No SQL injection protection:** If user-controlled strings ever reach the renderer (e.g., through query parameters), raw string interpolation could produce invalid or dangerous SQL. Currently mitigated because the parser validates input, but parameter rendering (`QueryExpressionParameter`) directly interpolates `$param_name`.
- **No output validation:** Generated SQL is not parsed or validated before returning. A typo in a template string silently produces broken SQL.

**Code patterns currently used:**

| Pattern | Where | Pros | Cons |
|---------|-------|------|------|
| **String builder** (`lines.append()` + `join`) | All renderer files | Simple, fast, low abstraction overhead | Fragile; no syntactic guarantees; hard to compose |
| **Visitor pattern** (implicit) | `_render_expression()` isinstance chain | Handles all expression types | Not extensible; adding new expression types requires modifying the chain |
| **Strategy/dispatcher** | `_render_operator()` in sql_renderer.py | Clean routing to sub-renderers | O(n) isinstance checks; could use a dict dispatch |
| **Context object** | `RenderContext` | Shared state without circular imports | Mutable shared state — thread-unsafe |
| **Pattern dictionaries** | `dialect.py` (OPERATOR_PATTERNS, AGGREGATION_PATTERNS) | Clean mapping from operators to SQL templates | Limited to simple `{0} OP {1}` patterns |

**Important missing patterns:**
- **SQL AST / builder** — a lightweight `SQLNode` tree (Select, From, Join, Where, GroupBy, OrderBy, CTE, etc.) that is rendered to string at the final step. This would:
  - Enable SQL-level optimizations (e.g., merging nested SELECTs).
  - Allow dialect switching (Databricks vs standard Spark SQL vs Trino).
  - Provide syntactic validation before emission.
  - Enable SQL formatting/pretty-printing.
- **Parameterized query emission** — render parameters as `?` placeholders with a separate parameter list, preventing injection.

**Concrete micro-actions:**

Before (current):
```python
# expression_renderer.py — string concatenation
def _render_binary(self, expr, op):
    left = self._render_expression(expr.left_expression, op)
    right = self._render_expression(expr.right_expression, op)
    pattern = OPERATOR_PATTERNS[expr.operator.name]
    return pattern.format(left, right)
```

After (SQL AST):
```python
# sql_ast.py — lightweight SQL nodes
@dataclass(frozen=True)
class SQLBinaryOp:
    operator: str  # "AND", "+", "=", etc.
    left: SQLExpr
    right: SQLExpr
    def to_sql(self) -> str:
        return f"({self.left.to_sql()}) {self.operator} ({self.right.to_sql()})"

# expression_renderer.py — produce SQL AST
def _render_binary(self, expr, op) -> SQLBinaryOp:
    left = self._render_expression(expr.left_expression, op)
    right = self._render_expression(expr.right_expression, op)
    sql_op = OPERATOR_SQL_MAP[expr.operator.name]
    return SQLBinaryOp(sql_op, left, right)
```

**Priority:** High | **Effort:** Large | **Confidence:** High

---

### Phase 9: Testing & Validation

**Exists?** Yes — `tests/` (109 files, 1,407 tests)

**Current strengths:**
- **High test count:** 1,378 passing + 11 skipped + 3 xfailed (non-PySpark suite).
- **Golden-file regression tests:** 43 transpile test files in `tests/transpile_tests/` with expected SQL outputs in `tests/output/expected/`.
- **PySpark end-to-end tests:** 37+ test files that transpile Cypher → SQL and execute on real Spark to validate semantic correctness.
- **Structural SQL assertions:** `sql_assertions.py` provides `assert_has_join()`, `assert_has_recursive_cte()`, `assert_cycle_detection()`, etc.
- **Domain-specific test suites:** Credit scoring (15 queries), fraud detection (17 queries), feature showcase (54 queries) — all executed against Spark.
- **Test generation tooling:** `scripts/generate_all_golden_files.py`, `scripts/generate_test_template.py`.

**Current weaknesses:**
- **No property-based testing:** No Hypothesis or similar generative testing. All tests use hand-crafted queries.
- **PySpark tests disabled in CI:** `ci.yml` line 50: `# PySpark tests disabled - run locally`. This means semantic correctness is not continuously validated.
- **No mutation testing:** No tool like `mutmut` or `cosmic-ray` to validate test effectiveness.
- **Large test files:** `test_vlp_unwind_comprehensive.py` (70,210 lines), `test_list_predicate_rel_property.py` (111,010 lines) — generated tests that are hard to maintain manually.
- **Ruff linting disabled in CI:** `ci.yml` lines 34-35: `# - name: Lint with ruff` is commented out.

**Code patterns currently used:**

| Pattern | Where | Pros | Cons |
|---------|-------|------|------|
| **Golden-file testing** | `tests/transpile_tests/`, `tests/output/expected/` | Catches SQL regressions; easy to update baselines | Brittle — formatting changes require regenerating all goldens |
| **Structural assertions** | `tests/utils/sql_assertions.py` | Validates SQL structure without exact string matching | Limited to regex-based checks; no semantic equivalence |
| **PySpark integration tests** | `tests/test_pyspark_*.py` | Validates actual execution correctness | Slow; disabled in CI; Java/Spark compatibility issues |
| **Parameterized tests** | `@pytest.mark.parametrize` in 5 files | Tests multiple inputs with same logic | Limited usage — could be expanded |

**Important missing patterns:**
- **Property-based testing** — generate random Cypher ASTs and verify: (a) transpiler doesn't crash, (b) output SQL parses, (c) if executed, produces correct results.
- **Semantic equivalence testing** — given a Cypher query and expected result, verify that the transpiled SQL produces the same result on a small dataset.
- **CI PySpark execution** — use a lightweight Spark-in-Docker or Spark Connect for CI.

**Detailed testing strategy — see Section 5 below.**

**Priority:** High | **Effort:** Medium | **Confidence:** High

---

### Phase 10: Build, Packaging, CI/CD, Release Strategy

**Exists?** Yes — `pyproject.toml`, `Makefile`, `.github/workflows/ci.yml`, `release.yml`

**Current strengths:**
- **Modern Python packaging:** `pyproject.toml` with hatchling backend. uv for dependency management.
- **Semantic versioning:** `python-semantic-release` with conventional commits.
- **Comprehensive Makefile:** 40+ targets covering testing, linting, formatting, type-checking, grammar generation, documentation, SQL dumping.
- **CI matrix:** Tests on Python 3.12 and 3.13.
- **Type checking in CI:** mypy runs in CI pipeline.
- **Documentation pipeline:** mkdocs-material with auto-generated example pages.

**Current weaknesses:**
- **Ruff lint disabled in CI** (commented out).
- **PySpark tests disabled in CI** — no semantic correctness validation in CI.
- **No pyright in CI** — only mypy, despite pyright being configured in `pyproject.toml`.
- **Generated ANTLR files in source control** — should be `.gitignore`d and regenerated in CI.
- **No dependency vulnerability scanning** (e.g., `pip-audit` or Dependabot).
- **Python 3.11 claimed in classifiers but CI only tests 3.12 and 3.13.**

**Micro-actions:**
1. Uncomment ruff lint step in `ci.yml`.
2. Add pyright type-check step in CI.
3. Add a `grammar` step in CI that regenerates ANTLR files and verifies they match checked-in versions (or remove checked-in versions).
4. Add `pip-audit` or Dependabot for dependency security scanning.
5. Add Python 3.11 to CI matrix or remove it from classifiers.
6. Explore `pyspark-connect` or `delta-spark` for lightweight CI PySpark tests.

**Priority:** Medium | **Effort:** Small | **Confidence:** High

---

### Phase 11: Documentation, Examples & Developer Onboarding

**Exists?** Yes — `docs/`, `examples/`, `README.md`, `CONTRIBUTING.md`, `CHANGELOG.md`

**Current strengths:**
- **mkdocs-material site** with user guide, examples, contributing guide.
- **Rich example library:** 86 curated queries across credit scoring, fraud detection, and feature showcase, each with Cypher input + generated SQL output.
- **CLI with `--explain-scopes`** for debugging schema propagation.
- **`dump_tree()`, `dump_scope()`, `dump_graph()`** methods for AST/plan debugging.

**Current weaknesses:**
- **No architecture documentation** — no document explaining the compiler pipeline, module responsibilities, or data flow. New contributors must reverse-engineer from code.
- **No API documentation** — no Sphinx/pdoc generated API docs. The `graph_context.py` has good docstrings but they're not exposed as docs.
- **No developer onboarding guide** — `CONTRIBUTING.md` exists but doesn't explain the transpiler architecture or how to add support for new Cypher constructs.

**Micro-actions:**
1. Add `docs/architecture.md` with the pipeline diagram from this report's Section 2.
2. Add `docs/developer-guide.md` explaining how to add support for a new Cypher clause (parser → AST → planner → renderer).
3. Generate API docs with pdoc or mkdocstrings.

**Priority:** Medium | **Effort:** Small | **Confidence:** High

---

## 4 — Cross-Cutting Concerns

### Error Handling & Diagnostics

**Current state:** Five custom exception types in `common/exceptions.py` (`TranspilerSyntaxErrorException`, `TranspilerBindingException`, `TranspilerNotSupportedException`, `TranspilerInternalErrorException`, `UnsupportedQueryPatternError`). `ColumnResolutionError` has rich context (query pointer, available variables, suggestions). Parser errors lack line/column context from ANTLR.

**Recommendation:** Add custom ANTLR error listeners. Propagate source locations from parser through AST to planner errors.

### Immutability vs Mutability

**Current state:** AST nodes are mutable dataclasses (despite design intent). The operator DAG is mutable and mutated in-place by optimizers. Type objects (`DataType`) are correctly frozen.

**Recommendation:** Freeze AST dataclasses; use `dataclasses.replace()` for modifications. Consider plan snapshots before/after optimization.

### Concurrency & Thread-Safety

**Current state:** `RenderContext` holds mutable shared state (counters, column sets). Not thread-safe. However, `GraphContext.transpile()` is a single-threaded call chain, so this is not currently a problem.

**Recommendation:** No action needed unless batch transpilation is planned. If so, ensure `RenderContext` and renderer instances are per-call, not shared.

### Observability

**Current state:** Custom `ILoggable` interface with 4 levels. Optional logger parameter on all major classes. `--explain-scopes` CLI flag. `dump_*()` methods on AST, operators, plans.

**Recommendation:** Consider using Python's standard `logging` module instead of the custom `ILoggable` interface. Add structured logging for transpilation metrics (parse time, plan time, render time, operator count, CTE depth).

### Style & Consistency

**Current state:** ruff configured (line-length 100, comprehensive rule set). mypy strict. pyright standard. Ruff lint disabled in CI.

**Recommendation:** Enable ruff in CI. Consider adding ruff format check to pre-commit hooks.

### API Boundaries & Module Coupling

**Current state:** `GraphContext` is the public API. Internal modules have reasonable coupling via the `RenderContext` pattern. However, `ResolutionResult` (planner output) is consumed directly by the renderer, creating tight coupling.

**Recommendation:** Define a `RendererInput` protocol/interface that the renderer depends on, rather than the concrete `ResolutionResult` from the planner.

### Security Considerations for Codegen

**Current state:** Query parameters (`$param`) are rendered by name interpolation. The parser validates Cypher syntax, which prevents SQL injection through the main query path. However, `GraphContext._discover_types()` builds SQL with f-strings from column names, which is a potential injection vector if column names are user-controlled.

**Recommendation:**
1. Audit all f-string SQL construction in `graph_context.py` and `schema_provider.py`. Use backtick-quoting for identifiers.
2. For parameter rendering, emit SQL with `?` placeholders and a separate value list.

---

## 5 — Testing & Correctness Strategy

### Minimum Test Suite for Safe Refactoring

| Test Type | Current | Target | Purpose |
|-----------|---------|--------|---------|
| **Golden-file regression** | 43 files, ~406 tests | Maintain | Catches SQL output changes |
| **Structural assertion tests** | ~500 tests | Maintain | Validates SQL structure |
| **PySpark integration** | ~200 tests (local only) | Enable in CI | Validates semantic correctness |
| **Property-based (Hypothesis)** | 0 | 50+ | Discovers edge cases |
| **Mutation testing** | 0 | Run quarterly | Validates test effectiveness |

### Golden-File Test Example

```python
# tests/transpile_tests/test_01_simple_node_lookup.py
class TestSimpleNodeLookup(TranspilerTestCase):
    """MATCH (p:Person) RETURN p.name"""

    @pytest.fixture
    def schema(self):
        return movie_graph_schema()

    def test_golden(self, schema):
        sql = self.transpile("MATCH (p:Person) RETURN p.name", schema)
        self.assert_golden("01_simple_node_lookup", sql)

    def test_structure(self, schema):
        sql = self.transpile("MATCH (p:Person) RETURN p.name", schema)
        assert_has_select(sql)
        assert_has_from_table(sql, "nodes")
        assert_no_join(sql)
```

### Suggested Test Matrix

| Cypher Construct | Unit | Golden | PySpark | Property |
|-----------------|------|--------|---------|----------|
| Single-hop pattern | Yes | Yes | Yes | Yes |
| Multi-hop fixed-length | Yes | Yes | Yes | Yes |
| Variable-length path | Yes | Yes | Yes | - |
| VLP with predicates | Yes | Yes | Yes | - |
| Undirected edges | Yes | Yes | Yes | Yes |
| OPTIONAL MATCH | Yes | Yes | Yes | Yes |
| Aggregations (COUNT, SUM, COLLECT) | Yes | Yes | Yes | Yes |
| WITH chaining | Yes | Yes | Yes | - |
| UNION / UNION ALL | Yes | Yes | Yes | Yes |
| UNWIND | Yes | Yes | Yes | - |
| List comprehensions | Yes | Yes | Yes | - |
| EXISTS subqueries | Yes | Yes | Yes | - |
| CASE expressions | Yes | Yes | Yes | Yes |
| ORDER BY, LIMIT, SKIP | Yes | Yes | Yes | Yes |
| DISTINCT | Yes | Yes | Yes | Yes |
| No-label nodes | Yes | Yes | Yes | - |
| Named paths | Yes | Yes | Yes | - |
| Parameterized queries | - | - | - | Yes |

### Semantic Equivalence Testing Strategy

For cases where exact SQL may differ but semantics must be preserved:

```python
# tests/test_semantic_equivalence.py
def test_semantic_equivalence(spark, graph_context):
    """Verify transpiled SQL produces same results as reference implementation."""
    cypher = "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a.name, b.name ORDER BY a.name"

    # Transpile
    sql = graph_context.transpile(cypher)

    # Execute
    actual = spark.sql(sql).collect()

    # Reference: hand-written SQL known to be correct
    reference_sql = """
        SELECT src.name, dst.name
        FROM nodes src
        JOIN edges e ON src.node_id = e.src AND src.node_type = 'Person'
        JOIN nodes dst ON e.dst = dst.node_id AND dst.node_type = 'Person'
        WHERE e.relationship_type = 'KNOWS'
        ORDER BY src.name
    """
    expected = spark.sql(reference_sql).collect()

    assert actual == expected
```

### Property-Based Testing with Hypothesis

```python
from hypothesis import given, strategies as st

@given(st.sampled_from(["Person", "Company"]),
       st.sampled_from(["KNOWS", "WORKS_AT"]))
def test_single_hop_always_produces_valid_sql(source_label, edge_type):
    """Any single-hop pattern should produce parseable SQL."""
    cypher = f"MATCH (a:{source_label})-[:{edge_type}]->(b) RETURN a"
    sql = graph_context.transpile(cypher)
    # Should not raise
    assert "SELECT" in sql
    assert "FROM" in sql
```

### CI Integration Steps

1. Add `pyspark` to CI dependencies (use `pyspark[connect]` for lightweight testing).
2. Add a `test-pyspark` job gated behind a `pyspark-tests` label on PRs.
3. Run property-based tests nightly (they're slower).
4. Run mutation testing quarterly and report coverage.

---

## 6 — Recursion & Variable-Length Path Handling

### The Problem

Cypher VLP queries like `MATCH (a)-[:KNOWS*1..5]->(b)` require iterative graph traversal. Standard Spark SQL has **no `WITH RECURSIVE`** support. Databricks SQL added it as a proprietary extension.

### Strategy 1: WITH RECURSIVE CTE (Current Approach)

**Used by:** gsql2rsql (current implementation in `recursive_cte_renderer.py`)

```sql
WITH RECURSIVE paths AS (
    -- Base case: direct edges from source
    SELECT
        src.node_id AS _gsql2rsql_source_id,
        dst.node_id AS _gsql2rsql_target_id,
        ARRAY(src.node_id, dst.node_id) AS _gsql2rsql_path,
        1 AS _gsql2rsql_depth
    FROM edges e
    JOIN nodes src ON e.src = src.node_id
    JOIN nodes dst ON e.dst = dst.node_id
    WHERE e.relationship_type = 'KNOWS'

    UNION ALL

    -- Recursive case: extend by one hop
    SELECT
        p._gsql2rsql_source_id,
        dst.node_id AS _gsql2rsql_target_id,
        ARRAY_APPEND(p._gsql2rsql_path, dst.node_id) AS _gsql2rsql_path,
        p._gsql2rsql_depth + 1 AS _gsql2rsql_depth
    FROM paths p
    JOIN edges e ON p._gsql2rsql_target_id = e.src
    JOIN nodes dst ON e.dst = dst.node_id
    WHERE e.relationship_type = 'KNOWS'
      AND p._gsql2rsql_depth < 5
      AND NOT ARRAY_CONTAINS(p._gsql2rsql_path, dst.node_id)  -- cycle detection
)
SELECT * FROM paths
```

**Pros:**
- Correct BFS semantics with cycle detection.
- Predicate pushdown into CTE (edge filters in WHERE).
- Native SQL — no UDF or external library needed.
- Bidirectional optimization halves search space.

**Cons:**
- **Databricks-only.** Standard Spark SQL does not support `WITH RECURSIVE`.
- Performance depends on Spark's recursive CTE execution strategy (materialized per iteration).
- Memory pressure for deep paths with large `visited` arrays.

### Strategy 2: Iterative DataFrame Loop (Vanilla Spark Fallback)

For standard Spark SQL without `WITH RECURSIVE`, implement path expansion as a Python loop of DataFrame operations:

```python
# Python (PySpark) iterative approach
def expand_paths(spark, edges_df, source_df, min_hops, max_hops):
    """Iterative BFS using DataFrame operations."""
    # Initialize: depth-1 paths
    paths = (
        source_df.alias("src")
        .join(edges_df.alias("e"), col("src.node_id") == col("e.src"))
        .select(
            col("src.node_id").alias("source_id"),
            col("e.dst").alias("target_id"),
            array(col("src.node_id"), col("e.dst")).alias("path"),
            lit(1).alias("depth"),
        )
    )

    all_paths = paths if min_hops <= 1 else spark.createDataFrame([], paths.schema)

    for depth in range(2, max_hops + 1):
        # Extend paths by one hop
        new_paths = (
            paths.alias("p")
            .join(edges_df.alias("e"), col("p.target_id") == col("e.src"))
            .where(~array_contains(col("p.path"), col("e.dst")))  # cycle detection
            .select(
                col("p.source_id"),
                col("e.dst").alias("target_id"),
                array_append(col("p.path"), col("e.dst")).alias("path"),
                lit(depth).alias("depth"),
            )
        )

        if depth >= min_hops:
            all_paths = all_paths.union(new_paths)

        paths = new_paths
        # Early termination if no new paths
        if paths.isEmpty():
            break

    return all_paths
```

**Pros:**
- Works on vanilla Spark SQL (no Databricks dependency).
- Explicit loop gives control over termination and checkpointing.
- Can checkpoint intermediate results to avoid lineage explosion.

**Cons:**
- Requires a Python driver program — cannot be expressed as pure SQL.
- Each iteration triggers a Spark job. For `max_hops=10`, that's 10 Spark jobs.
- `isEmpty()` triggers an action (materialization) per iteration.
- Not composable with the rest of a SQL query — must be materialized as a temp view.

### Strategy 3: Unrolled Joins (Bounded Depth)

For small, bounded `max_hops` (e.g., ≤ 5), unroll the VLP into explicit self-joins:

```sql
-- MATCH (a)-[:KNOWS*1..3]->(b) unrolled
SELECT a.node_id AS source, e1.dst AS target, 1 AS depth
FROM nodes a JOIN edges e1 ON a.node_id = e1.src
WHERE e1.relationship_type = 'KNOWS'
UNION ALL
SELECT a.node_id, e2.dst, 2
FROM nodes a JOIN edges e1 ON a.node_id = e1.src
JOIN edges e2 ON e1.dst = e2.src
WHERE e1.relationship_type = 'KNOWS' AND e2.relationship_type = 'KNOWS'
  AND e2.dst <> a.node_id  -- simple cycle check
UNION ALL
SELECT a.node_id, e3.dst, 3
FROM nodes a JOIN edges e1 ON a.node_id = e1.src
JOIN edges e2 ON e1.dst = e2.src
JOIN edges e3 ON e2.dst = e3.src
WHERE e1.relationship_type = 'KNOWS' AND e2.relationship_type = 'KNOWS'
  AND e3.relationship_type = 'KNOWS'
  AND e2.dst <> a.node_id AND e3.dst <> e1.dst  -- partial cycle check
```

**Pros:**
- Pure SQL — works everywhere including vanilla Spark.
- Spark optimizer can apply join reordering, predicate pushdown.
- No iteration overhead.

**Cons:**
- SQL size grows exponentially with `max_hops`. At depth 5, you have 5 UNION ALL branches, the deepest with 5 self-joins.
- Cycle detection is incomplete without full path tracking (which requires arrays, defeating the purpose).
- Not feasible for `max_hops > 6` due to query plan explosion.

### Recommendation

**Default approach:** Keep `WITH RECURSIVE` CTE (Strategy 1) as the primary strategy, given the project's explicit Databricks SQL target. It is correct, composable, and optimizable.

**Fallback for vanilla Spark:** Emit an error with a clear message:

```
TranspilerNotSupportedException:
  Variable-length paths require WITH RECURSIVE, which is only available in Databricks SQL.
  For vanilla Spark, consider:
  1. Using GraphFrames: g.bfs(fromExpr="...", toExpr="...", maxPathLength=5)
  2. Using the gsql2rsql Python runtime: graph.execute_with_loop(query)
```

Optionally, implement Strategy 2 (iterative DataFrame loop) as a runtime helper in `pyspark_executor.py` that `GraphContext.execute()` can use when detecting a VLP query on non-Databricks Spark.

For bounded shallow VLPs (max_hops ≤ 4) where the user explicitly opts in, Strategy 3 (unrolled joins) could be offered via a `--unroll-vlp` flag.

---

## 7 — Migration / Refactor Plan (Roadmap)

### Phase v1: Foundation (Low Risk, High Value)

**Goals:** Improve CI hygiene, enable PySpark in CI, add source locations to AST.

**Tasks:**
1. Enable ruff lint in `ci.yml` and fix any violations.
2. Add pyright to CI pipeline.
3. Add custom ANTLR error listeners (`parser/error_listener.py`) — attach to lexer and parser.
4. Add `SourceSpan` to AST nodes; populate from ANTLR context in `visitor.py`.
5. Add Python 3.11 to CI matrix or remove from classifiers.
6. Add `pip-audit` step in CI.
7. Explore `pyspark[connect]` for CI PySpark tests; if feasible, add a CI job.
8. Gitignore ANTLR generated files; add grammar regeneration to CI.

**Tests to add:** 10 parser error tests with line/column assertions.

**Risk mitigation:** All changes are additive; no existing behavior changes.

**Acceptance criteria:** CI runs lint, type-check (mypy + pyright), grammar generation, non-PySpark tests, and optionally PySpark tests. Parser errors include line/column.

---

### Phase v2: Planner Decomposition (Medium Risk)

**Goals:** Improve maintainability by splitting large files and formalizing the lowering phase.

**Tasks:**
1. Split `operators.py` (1,857 lines) into `planner/operators/` package:
   - `base.py` (LogicalOperator, Schema propagation)
   - `data_source.py` (DataSourceOperator)
   - `join.py` (JoinOperator, JoinKeyPair, JoinType)
   - `selection.py` (SelectionOperator)
   - `projection.py` (ProjectionOperator)
   - `recursive.py` (RecursiveTraversalOperator)
   - `unwind.py` (UnwindOperator)
   - `set_op.py` (SetOperator)
   - `aggregation.py` (AggregationBoundaryOperator)
2. Create `planner/lowering/` package:
   - Move `match_tree.py`, `recursive_traversal.py`, `aggregation_boundary.py`.
   - Add `lowering/__init__.py` with `lower_query(ast, schema) -> LogicalPlan`.
3. Split `column_resolver.py` (1,383 lines) into `symbol_builder.py` and `expression_resolver.py`.
4. Split `subquery_optimizer.py` (1,737 lines) into separate files per optimizer.
5. Introduce `OptimizationPass` interface and `PassManager`.

**Tests to add:** No new tests needed — existing tests validate behavior. Run full suite after each file move. Clear `__pycache__` (known pitfall).

**Risk mitigation:** Purely structural refactoring. Move one file at a time; run tests after each move. Use `git mv` to preserve history.

**Acceptance criteria:** All 1,378 tests pass. No file exceeds 800 lines. Imports updated. No circular imports.

---

### Phase v3: SQL AST Layer (High Risk, High Value)

**Goals:** Replace string concatenation with a lightweight SQL AST in the renderer.

**Tasks:**
1. Design `renderer/sql_ast.py` with nodes:
   - `SQLSelect`, `SQLFrom`, `SQLJoin`, `SQLWhere`, `SQLGroupBy`, `SQLHaving`, `SQLOrderBy`, `SQLLimit`
   - `SQLCte`, `SQLRecursiveCte`, `SQLUnionAll`
   - `SQLColumn`, `SQLLiteral`, `SQLBinaryOp`, `SQLFunctionCall`, `SQLCase`, `SQLSubquery`
   - Each node has `to_sql(dialect: Dialect) -> str` method.
2. Create `renderer/sql_dialect.py` with `DatabricksSQLDialect` implementing type mappings and function names.
3. Migrate `expression_renderer.py` to produce `SQLExpr` nodes instead of strings. Start with leaf expressions (literals, columns), then binary operations, then functions.
4. Migrate `sql_renderer.py` to produce `SQLSelect` nodes.
5. Migrate `join_renderer.py` to produce `SQLJoin` nodes.
6. Migrate `recursive_cte_renderer.py` to produce `SQLCte` nodes.
7. Add `SQLValidator` that parses the final SQL string (using `sqlglot` or similar) to catch syntax errors.

**Tests to add:**
- 50+ unit tests for SQL AST `to_sql()` on each node type.
- Golden-file tests must still pass (exact SQL output may change — regenerate baselines).

**Risk mitigation:** Implement behind a feature flag (`use_sql_ast=True`). Keep the old string renderer until all tests pass with the new one. Migrate one renderer at a time.

**Acceptance criteria:** All tests pass with SQL AST renderer. Output SQL is validated by `sqlglot.parse()`. Dialect switching is possible (at least in theory).

---

### Phase v4: Testing & Correctness (Low Risk)

**Goals:** Add property-based testing and improve coverage.

**Tasks:**
1. Add `hypothesis` to dev dependencies.
2. Create `tests/property/` package with:
   - `test_prop_single_hop.py` — random node labels + edge types → valid SQL.
   - `test_prop_expressions.py` — random expression ASTs → valid SQL.
   - `test_prop_roundtrip.py` — Cypher → SQL → execute on Spark fixture → verify results.
3. Add `mutmut` configuration for mutation testing.
4. Run mutation testing on critical modules (renderer, column resolver).
5. Add semantic equivalence tests for 20 key query patterns.

**Tests to add:** 50+ property-based tests, 20 semantic equivalence tests.

**Risk mitigation:** Tests only — no production code changes.

**Acceptance criteria:** Property tests discover 0 new bugs (or discovered bugs are fixed). Mutation score > 80% on renderer.

---

## 8 — Example PRs / Code Snippets

### Example 1: Custom ANTLR Error Listener

Before (`opencypher_parser.py`):
```python
def parse(self, query_string: str) -> QueryNode:
    input_stream = InputStream(query_string)
    lexer = CypherLexer(input_stream)
    token_stream = CommonTokenStream(lexer)
    parser = CypherParser(token_stream)
    visitor = CypherVisitor(self._logger)
    result = visitor.visit(parser.oC_Cypher())
    if isinstance(result, QueryNode):
        return result
    return SingleQueryNode()
```

After:
```python
# parser/error_listener.py
from antlr4.error.ErrorListener import ErrorListener
from gsql2rsql.common.exceptions import TranspilerSyntaxErrorException

class SyntaxErrorCollector(ErrorListener):
    def __init__(self):
        self.errors: list[str] = []

    def syntaxError(self, recognizer, offendingSymbol, line, column, msg, e):
        self.errors.append(f"line {line}:{column} {msg}")

# opencypher_parser.py
def parse(self, query_string: str) -> QueryNode:
    input_stream = InputStream(query_string)
    lexer = CypherLexer(input_stream)
    token_stream = CommonTokenStream(lexer)
    parser = CypherParser(token_stream)

    # Attach error collector
    collector = SyntaxErrorCollector()
    lexer.removeErrorListeners()
    lexer.addErrorListener(collector)
    parser.removeErrorListeners()
    parser.addErrorListener(collector)

    tree = parser.oC_Cypher()

    if collector.errors:
        raise TranspilerSyntaxErrorException(
            f"Parse errors:\n" + "\n".join(collector.errors)
        )

    visitor = CypherVisitor(self._logger)
    result = visitor.visit(tree)
    if isinstance(result, QueryNode):
        return result
    return SingleQueryNode()
```

### Example 2: Pass Manager Interface

Before (in `graph_context.py`):
```python
# Ad-hoc optimization calls
if optimize:
    optimize_plan(plan, enabled=True, pushdown_enabled=True,
                  dead_table_elimination_enabled=self.optimize_dead_tables)
apply_bidirectional_optimization(plan, graph_schema=self._schema, mode=bidirectional_mode)
```

After:
```python
# planner/pass_manager.py
from abc import ABC, abstractmethod
from gsql2rsql.planner.logical_plan import LogicalPlan

class OptimizationPass(ABC):
    """Base class for plan optimization passes."""
    @property
    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    def run(self, plan: LogicalPlan) -> bool:
        """Apply optimization. Returns True if plan was modified."""
        ...

class PassManager:
    def __init__(self):
        self._passes: list[OptimizationPass] = []

    def add(self, p: OptimizationPass) -> "PassManager":
        self._passes.append(p)
        return self

    def run_all(self, plan: LogicalPlan) -> list[str]:
        """Run all passes. Returns names of passes that modified the plan."""
        modified = []
        for p in self._passes:
            if p.run(plan):
                modified.append(p.name)
        return modified

# graph_context.py
pm = PassManager()
if optimize:
    pm.add(SelectionPushdownPass())
    pm.add(SubqueryFlatteningPass())
    if self.optimize_dead_tables:
        pm.add(DeadTableEliminationPass())
pm.add(BidirectionalBFSPass(self._schema, bidirectional_mode))
pm.run_all(plan)
```

### Example 3: Golden-File Test

```python
# tests/transpile_tests/test_01_simple_node_lookup.py
import pytest
from tests.utils.sql_test_utils import TranspilerTestCase
from tests.utils.sql_assertions import assert_has_select, assert_no_join

class TestSimpleNodeLookup(TranspilerTestCase):
    """Test: MATCH (p:Person) RETURN p.name"""

    @pytest.fixture
    def schema(self, movie_graph_schema):
        return movie_graph_schema

    def test_golden(self, schema):
        """Verify SQL output matches golden file."""
        sql = self.transpile(
            "MATCH (p:Person) RETURN p.name AS name",
            schema
        )
        self.assert_golden("01_simple_node_lookup", sql)

    def test_no_join(self, schema):
        """Single-node lookup should not require JOINs."""
        sql = self.transpile(
            "MATCH (p:Person) RETURN p.name AS name",
            schema
        )
        assert_has_select(sql)
        assert_no_join(sql)
```

---

## 9 — Final Recommendations & Appendix

### Top 10 Concrete Refactor Tasks (Priority Order)

1. **Enable ruff + pyright in CI** — 1 hour, zero risk.
2. **Add custom ANTLR error listeners** with line/column — 4 hours, low risk.
3. **Add SourceSpan to AST nodes** — 8 hours, low risk.
4. **Split operators.py into package** — 4 hours, mechanical refactoring.
5. **Split subquery_optimizer.py** into separate optimizer files — 4 hours.
6. **Introduce PassManager interface** — 4 hours, low risk.
7. **Create planner/lowering/ package** — 4 hours, structural only.
8. **Add property-based tests** (Hypothesis) — 16 hours, tests only.
9. **Design SQL AST layer** (`renderer/sql_ast.py`) — 40 hours, high value.
10. **Migrate expression renderer to SQL AST** — 40 hours, high risk/value.

### Risk/Benefit Summary

| Approach | Risk | Benefit | Recommendation |
|----------|------|---------|----------------|
| **Full refactor (v1–v4)** | Medium | Dramatically improved maintainability, testability, and extensibility | Pursue incrementally over 3-6 months |
| **Incremental improvements only** (v1+v2) | Low | Good maintainability gains; no architecture change | Good if resources are limited |
| **Status quo** | None | None | Viable for current scope, but technical debt accumulates |

The codebase is in good shape for v0.9.3. The recommended path is v1 (CI/errors) + v2 (decomposition) immediately, then v3 (SQL AST) when the team is ready for a larger investment.

### Appendix: Glossary

| Term | Meaning |
|------|---------|
| **VLP** | Variable-Length Path — Cypher `[*1..N]` patterns |
| **CTE** | Common Table Expression — SQL `WITH` clause |
| **BFS** | Breadth-First Search — iterative path expansion |
| **AST** | Abstract Syntax Tree — tree representation of parsed query |
| **IR** | Intermediate Representation — the logical operator DAG |
| **DAG** | Directed Acyclic Graph — operator connectivity structure |
| **Golden file** | Expected output file for regression testing |
| **ANTLR** | ANother Tool for Language Recognition — parser generator |
| **ATN** | Augmented Transition Network — ANTLR's internal automaton |

### Appendix: New Contributor Onboarding Checklist

- [ ] Read `README.md` and `CONTRIBUTING.md`
- [ ] Run `make install-dev` and `make test` to verify environment
- [ ] Run `make cli-example` to see a transpilation in action
- [ ] Read `graph_context.py` — the public API and full pipeline in 80 lines
- [ ] Read `parser/ast.py` — the AST node types (846 lines, well-documented)
- [ ] Read `planner/operators.py` — the relational operator types
- [ ] Read `renderer/sql_renderer.py:_render_operator()` — the dispatch point
- [ ] Run `echo "MATCH (p:Person) RETURN p.name" | make cli-transpile` with `--explain-scopes` to see schema propagation
- [ ] Pick a small golden-file test in `tests/transpile_tests/` and trace the query through parser → planner → renderer
- [ ] Read `ARCHITECTURE_AUDIT.md` (this document) Section 2 for the data flow diagram
