# findings/

Reading material, not scaffolding. Everything here exists to answer a question about the
engines — what Sail refuses, where the two disagree, which received wisdom about Spark
survives a look at the physical plan. None of it is needed to run a pipeline.

**A project started from this template should delete this directory on day one.** Nothing
in `shared/` or `backend/` refers to anything in here; the dependency only runs the other
way.

## What is in it

| | |
|---|---|
| `shared/test` | Specs that run against **both** engines: `ClosureSpec`, `PushdownSpec`, `PlanSpec`, `OptimizerSpec`, `DecProbe`, `AnsiModeSpec`, `PracticeSpec`, `TimeZoneSpec`, `PartitionedDivergenceSpec`, `TypedLambdaSpec`, `ExprSpec` |
| `classic/test` | `ClassicPlanSpec` — `queryExecution` exists only on classic, so this cannot compile for connect |
| `connect/test` | `WirePlanSpec`, `ClosureGuardSpec` — both read the Connect request protobuf, which classic does not have |
| `connect/main` | `WirePlan`, `ClosureGuard` — the request reader and the client-side guard |
| `macros` | The `Expr` spike: a typed lambda translated to a `Column` at compile time |

It is a **source directory** rather than an sbt subproject, and that is deliberate. These
specs are worth having precisely because the same assertions run against classic Spark and
against Sail, and a subproject would have to depend on one client or the other —
`spark-sql` and `spark-connect-client-jvm` both ship `org.apache.spark.sql.SparkSession`
and cannot share a classpath. A directory added to both backends compiles twice, exactly
as `shared/` does. `macros` is the one exception: a Scala 2 macro cannot be used in the
compilation unit that defines it, so it has to be its own project wherever it lives.

## Deleting it

Five edits, all in `build.sbt`, plus the directory. Verified by applying them to a copy of
this repository and running the suite.

```
rm -rf findings
```

Then in `build.sbt`:

1. Delete the `def findings(backend: String) = Seq(...)` block and its scaladoc.
2. Delete the `lazy val macros = (project in file("findings/macros"))` block and its scaladoc.
3. Remove `.dependsOn(macros)` from `classic` and from `connect` (two lines).
4. Remove `.settings(findings("classic"))` and `.settings(findings("connect"))` (two lines).
5. Change `.aggregate(macros, classic, connect)` to `.aggregate(classic, connect)`.

And in the top-level `README.md`, drop the sections that describe what is here: *What we
know about Sail from the JVM*, *The typed lambda was never the fast path*, *Best practices,
measured*, *The wire, and what you can do before it leaves*, and *Reading the plan*.

What remains is the template: the flake, the dual-backend build, `Conform`, `Storage`,
`Model`, six ETLs and their specs.
