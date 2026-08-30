# ⛵ dev-nix-sail-scala

A Spark template on **Scala**, sibling to [`template-nix-sail`](https://github.com/davidlghellin/template-nix-sail)
(the Python one, with PySpark and PySail). Same premise and same goal: Nix pins
the environment, CI runs the tests against **both engines**, and the example
code is shaped like a real ETL.

It has since become three things wearing one name, and it is worth knowing which
one you came for.

**A template.** `flake.nix`, `build.sbt`, the two backends, and `MeterEtl` as the
reference ETL to copy. If you are starting a project, this is the part you want,
and you can delete the rest.

**A JVM conformance suite for Sail.** Five specs and eight assertions exist only
to record where the two engines disagree, and *"What we know about Sail from the
JVM"* below collects the findings. This part exists because Sail's own test suite
structurally cannot hold it: that suite is Python and PySpark, and a Scala
`Dataset.map` cannot be expressed from there. If you work on Sail, this is
probably the part you want.

**A research spike**, isolated in `findings/macros/`: can a typed lambda be translated
into a `Column` at compile time? It works, and the section on it explains why
that still does not solve the problem.

## If you read one thing

The axis that matters is **columns against closures**, not DataFrame against
Dataset. Everything else here is a consequence of it.

| | runs on Sail | pushdown and pruning | typed |
|---|---|---|---|
| DataFrame + columns | yes | yes | no |
| **Dataset + columns** | **yes** | **yes** | **yes** |
| Dataset + closures | no | no | yes |

The middle row gives up nothing to the first. It is the first row **plus the
types, for free** — which is worth saying because the obvious summary of this
project ("DataFrames work, Datasets fail") leads to exactly the wrong move.
Datasets do not fail. `map`, `filter`, `flatMap`, `groupByKey` and `reduce` with
a lambda fail, and they fail on Sail while quietly costing a full table scan on
classic, which `PushdownSpec` measures.

So: avoid closures — not because Sail cannot run them, but because they were
never the fast path. Sail is just the first engine that says so out loud. Once
you are off them, the `Dataset` costs nothing.

How livable is that rule? **None of the six ETLs here contains a single Dataset
closure** — not the DataFrame one, not the five typed ones. `BaseCase` "just
works" not because it is a DataFrame but because its style was already columns;
put a typed `map` in it and it would lose the pushdown exactly the same way.

The closures live only in the specs that exist to show what they cost.

```
.
├── versions.json             # the single source of truth: Scala, Spark, pysail
├── flake.nix                 # devshell: JDK 21, sbt, scalafmt and the Sail server
├── build.sbt                 # two subprojects, one per backend
├── shared/                   # the code and tests that know nothing of backends
│   ├── main/scala/devel0pez/ #   Calculator, DataFrames, BaseCase, TypedEtl,
│   │                         #   PipelineEtl, ConformedEtl, MeterEtl, Conform,
│   │                         #   Storage, Model, Demo
│   └── test/scala/devel0pez/ #   the specs, which run twice
├── backend/
│   ├── classic/              # classic Spark on the local JVM
│   └── connect/              # Sail, over Spark Connect
└── findings/                 # DELETE THIS to get the template back
    ├── shared/test/          #   what Sail refuses, where the engines differ,
    │                         #   which best practices survive the plan
    ├── classic/test/         #   plan inspection that only compiles on classic
    ├── connect/{main,test}/  #   the request protobuf, and the client-side guard
    └── macros/               #   the spike: lambda -> Column at compile time
```

## Two halves, and one of them is deletable

This repository is a template with a research project attached, and the second
half has grown larger than the first. So it is fenced off rather than mixed in:
everything that exists to *investigate* the engines lives under `findings/`, and
nothing outside it refers to anything inside it.

Start a project from this and `rm -rf findings` on day one, plus five edits in
`build.sbt` that `findings/README.md` spells out and that have been verified by
applying them to a copy and running the suite. What is left is the flake, the
dual-backend build, `Conform`, `Storage`, `Model`, six ETLs and their specs —
82 tests on classic and 83 on Sail, still green.

Keep it and you get the other 141 and 150, which are the reason most of this
README exists. The split is a source directory rather than a subproject on
purpose: the specs are worth having because the same assertions run against both
engines, and a subproject would have to pick a client.

## Getting started

```bash
nix develop        # JDK, sbt and the Sail server, all in place
t                  # the tests against BOTH backends
tc                 # classic Spark only
ts                 # Sail only
f                  # format with scalafmt
```

With [direnv](https://direnv.net) the first line is not needed: `.envrc` says
`use flake`, so entering the directory *is* entering the devshell.

```bash
direnv allow       # once per clone
cd .               # JDK 21, sbt, scalafmt and sail, already on the PATH
```

### The rest of the commands

The devshell defines eleven, and `menu` lists them at any time. The four above
are the ones you type all day; these are the ones worth knowing about before you
reach for something slower.

```bash
tt BaseCaseSpec    # one suite, against both backends — the inner loop
c                  # compile main and test without running anything
cscala             # Scala REPL with Spark and the project (classic; `cscala connect` too)
run-demo           # run the demo (classic; `run-demo connect` needs a server up)
sail-server        # a Sail server in the foreground on :50051, to poke at by hand
fc                 # check formatting without rewriting (what CI runs)
clean-all          # delete every target/ and the sbt build cache
check-commands     # smoke-test the commands in this menu (CI runs it)
```

`cscala` and `run-demo` name a backend because the root project only aggregates:
it has no sources and no dependencies, so `sbt run` there finds no main class and
its REPL has neither Spark nor the project on the classpath. Both default to
`classic`, which needs nothing else running.

`tt` is the one that changes how the day feels: a full `t` pays for two Spark
sessions and every suite, while `tt ConformSpec` is seconds. `cscala` is the
other one — for the questions that are faster asked than tested, which is how
most of the findings written down in this README were actually found.

Two caveats, both of which look like the environment is broken when they hit:

- Flakes only see files **git knows about**. In a fresh `git init` with nothing
  staged, `flake.nix` is invisible and every `nix develop` dies with *"Path
  'flake.nix' ... is not tracked by Git"*. `git add` is enough; a commit is not.
- `JAVA_HOME` is an **override, not a default** ([flake.nix](flake.nix)). sbt and
  Spark read it before the PATH, so a JDK 11 inherited from SDKMAN would decide
  which JVM runs, and Spark 4 does not even compile on 11.

## IntelliJ

Launch it **from inside the devshell**, or it inherits whatever `JAVA_HOME` the
desktop session has:

```bash
idea .             # with direnv active in this directory
nix develop -c idea .   # without direnv
```

The alternative is the Direnv plugin. Either way, check `⌘;` → Project → SDK
says **21**, and Settings → Build Tools → sbt → JRE too: the sbt importer writes
the JDK it was started with into `.idea/misc.xml` and `.bsp/sbt.json`, and a
project imported once on Java 11 keeps it until you reimport.

One more thing about running the specs from the IDE gutter: IntelliJ's own
ScalaTest runner **ignores** `Test / javaOptions`, so the `--add-opens` flags in
[build.sbt](build.sbt) never reach the test JVM and the first `collect()` fails
with `InaccessibleObjectException`. `CalculatorSpec` does not care — it needs no
session — but everything extending `SparkSuite` does. Either tick Settings →
Build Tools → sbt → *use sbt shell for builds and tests*, or copy those flags
into the run configuration's VM options.

## MeterEtl: the one to copy

The other ETLs each isolate one idea so it can be read alone — `BaseCase` the
DataFrame style, `TypedEtl` the typed one, `PipelineEtl` composable stages,
`ConformedEtl` guarded boundaries. `MeterEtl` is what they add up to, on a
domain of its own so it can be lifted whole: meter readings landing as text,
coming out as daily usage.

```scala
val pipeline: Dataset[RawReading] => Dataset[DailyUsage] =
  parse andThen validate andThen daily

def run(spark: SparkSession)(implicit
    source: Storage[RawReading],
    sink: Storage[DailyUsage],
    deadLetters: Storage[RejectedReading]
): Unit = {
  val parsed = spark.load[RawReading].transform(parse)
  parsed.transform(rejected).saveTo
  parsed.transform(validate).transform(daily).saveTo
}
```

Not a table name, a format or a write mode in the job. The fork is why it is not
simply `pipeline andThen saveTo`: both branches start from the parsed rows, so
the shape is a `val` read twice rather than a single chain — and read twice is
what it does, parsing once per branch. Persisting `parsed` would trade memory
for that second pass, and on real volume it is usually the right trade; it is
left out because a template should show the shape, not a tuning decision that
depends on data you do not have.

### Option, and what it does not buy

`tariff` is `Option[String]` because a meter can genuinely not have one; `kwh` is
not, and what makes it non-null is `validate`, not its type. The intuition to
correct is that `Option` tightens the schema. It does not:

| field | type | `nullable` in the encoder |
|---|---|---|
| `tariff` | `Option[String]` | true |
| `kwh` | `BigDecimal` | true |
| `readings` | `Long` | **false** |

Reference fields are nullable whether or not they are `Option`; **primitives are
not**. That asymmetry is not academic — every column read back from a table is
nullable, so conforming one into a model with a primitive field used to fail
with `NULLABLE_COLUMN_OR_FIELD` on grounds that have nothing to do with shape.
`Conform` therefore relaxes the target's nullability before handing it to `to`,
and leaves non-nullness where this template keeps insisting it belongs: in
validation, not in a type.

## The two backends

This is the same idea as `SPARK_BACKEND=pysail|pyspark` in the Python template,
with one difference: **there the choice is made at run time, here at compile
time**. `spark-sql` and `spark-connect-client-jvm` both ship the class
`org.apache.spark.sql.SparkSession`, so they cannot share a classpath. Hence the
two subprojects.

What is **not** duplicated is the code: `shared/` is compiled twice, once
against each client. Spark 4 moved the common API into `spark-sql-api`, so the
same transformations and **the same specs** serve both. Only where the session
comes from changes:

| | classic | connect |
| --- | --- | --- |
| Dependency | `spark-sql` | `spark-connect-client-jvm` |
| Session | `.master("local[1]")` | `.remote(server.url)` |
| Engine | JVM | Sail (Rust) |

## What `connect` actually depends on

The point of this template is a codebase that can eventually drop classic Spark
entirely, so it is worth checking rather than assuming. The `connect`
subproject's whole Spark-side classpath is:

```
spark-connect-client-jvm  spark-connect-shims  spark-common-utils
spark-sketch  spark-unsafe  spark-variant  spark-tags
```

No `spark-sql`, no `spark-core`, no Hadoop, no Hive. The client jar carries the
API itself — `SparkSession`, `Dataset`, `Column`, `functions` are all inside it —
so it is the only thing the imports need. `classic` exists as an oracle to
compare against, not as something `connect` leans on, and `sail-testkit` is
test-only and adds nothing to that list.

## The Sail server

Sail is a Rust binary distributed as a **Python wheel**, so there is nothing on
the JVM side that can start it. That is
[`sail-testkit`](https://github.com/devel0pez-com/sail-testkit)'s job, and it is
here **as a test-only dependency**:

```scala
"com.devel0pez" %% "sail-testkit" % versionOf("testkit") % Test
```

The `connect` backend's tests mix it in and get a server of their own per suite,
with nothing to set up. The demo (`connect/run`) does **not** use it: it
connects to a server that is already running, because putting a test kit in
`src/main` would drop it on the classpath of everyone who depends on this
template.

```bash
sail-server &                              # inside the devshell
SPARK_REMOTE=sc://localhost:50051 sbt connect/run
```

## Versions: they travel in pairs or not at all

`versions.json` is the single source of truth. It is read by `build.sbt` (the
client's Spark and Scala versions) and by `flake.nix` (what goes into the
server's venv).

The pairing is not cosmetic: **Sail asks the `pyspark` module on its own side**
which Spark version to serve. With a venv carrying only `pysail`,
`spark.version` answers:

```
invalid argument: failed to get PySpark version: ModuleNotFoundError: No module named 'pyspark'
```

That is why the venv installs `pysail` and `pyspark` together, at the versions
from the same file the client reads. And `SailVersionSpec` checks it on every
run: if someone bumps Spark and forgets pysail, the test says so instead of
letting odd symptoms surface later.

## The Java version is not a detail

Spark 4 is compiled with `maven.compiler.release=17`, so **on an older JDK it
does not even link**. It supports 17 and 21; this template runs on **21**, and
`flake.nix` is the single place that decides. And `sbt` reads `JAVA_HOME` ahead
of `PATH`, so a `JAVA_HOME` inherited from outside (SDKMAN, say) decides which
JVM runs. The flake sets it as an **override, not a default**, exactly as the
Python template does: it is the same stumble in both languages.

`build.sbt` also pins `javaHome` from `JAVA_HOME`, because sbt forks with the
JVM that launched it, not with the shell's.

That pin is not enough on its own, and the way it fails is worth writing down.
nixpkgs' `sbt` carries a JDK of its own and exports **its** `JAVA_HOME` on the
way in, which beats the devshell's silently. The symptom is two honest answers
that disagree: `java -version` in the shell reported 17 while sbt's welcome
banner reported 21, and every compile and every test ran on the latter. The
flake fixes it at the root with `pkgs.sbt.override { jre = jdk; }`, so sbt and
the shell are the same derivation and `jdk` is the only place a version is
chosen. If you ever suspect the two have drifted apart again, one command
settles it:

```bash
sbt -batch "print classic/Compile/javaHome"   # must match $JAVA_HOME exactly
```

And the `--add-opens` flags have to be passed by hand: Spark and Arrow reach
into JDK internals by reflection, and those have been sealed since Java 17.
`spark-submit` adds them on your behalf; from sbt, nobody does. They are run-time
flags, not compile-time ones, which is why a build that compiles cleanly can
still die on its first row. Without them:

- on classic, the first `collect()` dies with `InaccessibleObjectException`;
- on connect, Arrow fails with `sun.misc.Unsafe ... not available`.

## The base case

`BaseCase.scala` reproduces the shape of a real ETL, and it is **the same case**
as the Python version: two tables with an explicit `StructType`, a filter with
`CASE` + `DISTINCT`, a `LEFT JOIN` qualified by DataFrame (both tables carry
repeated column names), an aggregate over `DecimalType`, and a positional
conform to the target schema so that `insertInto` works.

The transformations **do not create a session**: they take a `DataFrame` and
return a `DataFrame`. That is why one test file serves both engines.

## Typed Datasets, and the one thing that does not travel

`TypedEtl.scala` models the domain with case classes and works with `Dataset[T]`
throughout. It runs on both backends, which is worth stating plainly because the
folklore says otherwise: encoders are derived on the **client**, so
`Seq[T].toDS()`, `as[T]` and typed `collect()` all work over Connect.

What does not travel is a **closure**. `sales.map(_.amount * 2)` compiles, and
then fails against Connect, because a lambda is JVM bytecode that only the JVM
that produced it can run: Sail is Rust, and there is nothing on the far side to
deserialize it into. The same expression written as a column
(`col("amount") * 2`) crosses as a protobuf plan and runs anywhere.
`TypedEtlSpec` asserts both halves of that — the lambda succeeding on classic
and failing on connect — so the boundary is documented by a test rather than by
a paragraph.

Columns instead of lambdas does not have to mean columns as strings. Each case
class in `Model.scala` carries an implicit class of **column handles**, so the
join reads

```scala
sales.join(products, sales.product === products.code, "left")
```

rather than `sales("product") === products("code")`. The handles are bound to
the dataset, which is what keeps a join unambiguous when both sides carry a
column of the same name, and living in the case class's companion puts them in
the implicit scope of `Dataset[T]` — no import at the call site. A misspelled
field is a compile error instead of a runtime `AnalysisException`; a renamed one
is caught by a test that selects through every handle at once.

A handle is a plain `Column`, so nothing in the column API is lost by going
through one — `sales.branch.cast(IntegerType)` reads a String branch code back
as an Int, and casts go through the type objects rather than their string
spellings for the same reason the columns do.

Casting is also where ANSI mode shows its teeth. The suite runs with
`spark.sql.ansi.enabled = true`, so `sales.product.cast(IntegerType)` over a
column of `"P1"` does not return NULL: it raises `[CAST_INVALID_INPUT]`.
`Column.try_cast` is the tolerant form and composes with a handle the same way,
returning NULL instead. Both are asserted in `TypedEtlSpec`, because which of
the two you get is a property of the *session*, not of the expression.

It is also one of the few places the two engines are visibly different. They
agree on the semantics — both refuse the cast — but not on the identity of the
error. Classic Spark raises its own error class:

```
[CAST_INVALID_INPUT] The value 'P1' of the type "STRING" cannot be cast to "INT"
```

Sail fails on the Rust side with something Connect cannot map to a Spark error
class, so it arrives wrapped:

```
[CONNECT_CLIENT_UNEXPECTED_MISSING_SQL_STATE] Unidentified Error:
Cast error: Cannot cast string 'P1' to value of Int32 type
```

The test asserts each backend's message rather than merely that *something*
failed, so the day Sail reports the Spark error class it goes red and says so.
Worth knowing before writing code that matches on error classes.

The trap worth knowing: a field whose name collides with a member of `Dataset`
(`count`, `schema`, `columns`, `write`) silently resolves to the method, because
an implicit conversion is only tried when the member does not already exist.

## select, not withColumn

Every transformation here projects with `select`. `withColumn` adds one column
and wraps the plan in one more `Project` to do it, so called in a loop it nests.

Where it nests deserves precision, because the obvious guess is wrong: Catalyst
flattens the pile before execution. `ClassicPlanSpec` measures five
chained `withColumn`s against the equivalent `select`:

| plan | chained | one `select` |
|---|---|---|
| analyzed | 6 `Project` | 2 |
| optimized | 1 | 1 |
| physical | 1 | 1 |

(measured in `ClassicPlanSpec`; the physical comparison is in `PlanSpec`, which
runs on both engines)

So the cost is not a worse query — it is a longer walk to the same query, paid
by the analyzer on every operation that follows. Long enough chains are a known
way to blow the analyzer's stack, with a trace that says nothing about the loop
that caused it.

`DataFrames.addColumns` is the single exception, and it is there to show the
escape hatch: `withColumns` takes a whole map and produces **one** projection,
so it costs what a `select` costs. Use it when the column list is computed
rather than written out. When you know the columns, `select` says the same thing
and pins the output order besides — which matters the moment the result meets an
`insertInto`, since that matches by position.

The same rule is why `TypedEtl.byFamily` stamps its audit columns inside the
`agg` instead of appending them afterwards: a literal is foldable, so Catalyst
carries it through the grouping, and the result stays one projection deep.

## Stages that compose

`PipelineEtl.scala` is the same ETL again, built the other way round. Where
`TypedEtl` writes each step as a method, this one writes each as a **value** of
type `Dataset[A] => Dataset[B]`, which changes what you can do with it: a method
can only be called, a function can also be composed, named and tested alone.

```
 spark.table("sales_raw")                    spark.table("products")
        │ as[RawSale]                                  │ as[Product]
        ▼                                              │
  Dataset[RawSale]     everything a String              │
        │ transform(parse)          try_cast -> NULLs   │
        ▼                                              │
  Dataset[Sale]        typed, and maybe null            │
        │ transform(validate)                           │
        ├──────────────► Dataset[RejectedSale]  dead letters, with a reason
        ▼                                              │
  Dataset[ValidSale]   a type that means "checked"      │
        │ transform(enrich(products)) ◄────────────────┘
        ▼
  Dataset[EnrichedSale]
        │ transform(byFamily(audited, day))
        ▼
  Dataset[SalesByFamily]
```

`transform` and `andThen` are related without being the same thing.
`ds.transform(f)` **is** `f(ds)` — it applies, and exists so a chain reads left
to right instead of nesting as `byFamily(enrich(validate(parse(ds))))`.
`f andThen g` is `x => g(f(x))` — it composes, and hands back another function
with no `Dataset` touched yet. They meet here:

```scala
ds.transform(f).transform(g)  ==  ds.transform(f andThen g)
```

which is what lets the whole ETL be one value:

```scala
def pipeline(products: Dataset[Product], audited: Timestamp, day: Date)
    : Dataset[RawSale] => Dataset[SalesByFamily] =
  parse andThen validate andThen enrich(products) andThen byFamily(audited, day)
```

`PipelineEtlSpec` asserts that equality rather than leaving it as a claim, and
exercises one stage on a one-row `Dataset` built in the test — which is the
practical payoff of a stage being a value.

`transform` is also safe over Connect, for a reason worth knowing: it is a
concrete method on the shared API, so the function runs on the **client** while
the plan is built. Nothing is shipped, which is exactly what `map` cannot say.

Two details carry more weight than they look. `parse` uses `try_cast`, so one
malformed row becomes a NULL instead of raising and killing the run — that is
what gives `validate` something to route to the dead-letter branch, and it is
the same ANSI behaviour described above. And `ValidSale` repeats `Sale`'s fields
on purpose: `enrich` accepts only the validated type, so handing it unchecked
rows is a compile error rather than a surprise three stages later.

## Conform: the trap `as[T]` leaves open

`as[T]` decodes **by name** but does not reorder the schema. A frame whose
columns are `(family, name, code)` becomes a `Dataset[Product]` whose schema is
still in that order, while `collect()` hands back perfectly correct `Product`
values — so every assertion you would think to write passes:

```scala
reversed.as[Product].collect().head          // Product(P1, Widget, TOOLS)  ✓
reversed.as[Product].schema.fieldNames       // (family, name, code)        ✗
```

Then `insertInto` matches by **position** and writes the family into the code
column. Nothing raises. `ConformSpec` asserts that damage on purpose, because a
failure mode you cannot see is worth a test that makes you look at it.

`Conform` is the typeclass that closes it. `Dataset.to(StructType)` is the
engine — it reorders, drops extras and type-checks — and the typeclass adds the
guard `to` lacks:

```scala
import Conform._

wide.conformTo[Product]                    // reorders, drops what Product does not declare
short.conformTo[Product]                   // ConformError: missing columns: family
wide.conformTo[Product](Conform.exact)     // ConformError: unexpected columns: junk
```

The guard is not decoration, and the reason is sharper than it first looks: on a
missing column the two engines do not agree. Classic Spark **invents** the field
and fills it with nulls — a schema derived from a case class has every field
`nullable = true`, so that path is always open. Sail refuses the plan with
`field not found in input schema`. So `to` alone is not a contract you can build
on. The check runs before `to`, never after, and that is what makes both engines
answer the same way.

Extra columns fail only under `Conform.exact`, and that split is deliberate.
Dropping extras is the normal case: reading a wide table into a narrow model is
a projection you asked for. The mistake people actually fear, a mistyped column
name, already surfaces on the missing branch — a typo leaves the field absent as
well as leaving a stray one behind. `exact` is for a boundary, a landing zone or
a target table read back, where a column appearing out of nowhere means
something upstream changed and the quiet answer is the wrong one.

Instances derive from `Encoders.product[T]`, which needs only a `TypeTag`, so
`Conform[Sale].schema` is summonable with no session and no
`import spark.implicits._`. The call-site syntax does need `import Conform._`.

## An ETL with both boundaries guarded

`ConformedEtl` is the shortest file of the three ETLs, and deliberately so: its
middle is `PipelineEtl`'s stages, reused unchanged with an `andThen`. All it
adds is the two edges — which is where ETLs actually break.

```scala
def read(spark: SparkSession, table: String): Dataset[Sale] =
  spark.table(table).conformTo[Sale]

def write(result: Dataset[SalesByFamily], table: String): Unit =
  result.conformTo[SalesByFamily].write.insertInto(table)
```

The source table in `ConformedEtlSpec` is hostile on purpose — columns in an
order nobody would choose, plus one the model does not want — because that is
what a table you do not own looks like. `as[Sale]` accepts it quietly and hands
back a `Dataset[Sale]` whose schema is still `(day, amount, product, branch,
country, ingested_at)`; the spec asserts exactly that, then shows `conformTo`
turning it into the model's shape.

The write edge matters for the same reason with the arrow reversed. `insertInto`
matches by **position**, so producing the right columns in the right order by
hand is discipline — and discipline stops working the day somebody adds a column
to an `agg`. Conforming first makes it a guarantee.

The contract that comes with it is worth stating: the case class **is** the
schema. `conformTo[SalesByFamily]` orders columns as `SalesByFamily` declares
them, so the target table must be declared in that same order. One ordering,
written once in Scala, instead of an ordering implied by every `select` on the
way.

The other payoff is where it fails. A source missing a column raises
`ConformError` at the boundary, before a single row is read — not twenty minutes
in, and not as a half-written table.

## Storage: where a T lives, injected

`Conform` guards the edges; `Storage` moves them out of the job entirely. It is
a typeclass over "where a `T` lives", and an ETL written against it names no
table, no format and no write mode:

```scala
implicit val sales: Storage[Sale] = Storage.catalog[Sale]("sales_src")
implicit val sink:  Storage[SalesByFamily] = Storage.catalog[SalesByFamily]("by_family")

spark.load[Sale].transform(stages(...)).saveTo
```

Two things fall out, and the second is the one that pays.

**Conforming stops being optional.** Both directions run `conformTo[T]` inside
the instance, so there is no way to get a `Dataset[Sale]` through this door
without the frame having been checked and reordered, and no way to write one
without it matching the shape `insertInto` assumes. The discipline moves out of
the call site, where it was something to remember, and into the type, where it
is something to satisfy.

**The storage becomes swappable without touching the job.** `StorageSpec` runs
the identical ETL call twice — once with `Storage.catalog`, once with
`Storage.view` — and then asserts the two destinations hold the same thing. If
the job knew anything about where its data lived, that test could not be
written.

The syntax is `load` and `saveTo`, not `read` and `write`, and that is not
taste: `Dataset` already has a `write` member, and a member always beats an
implicit conversion. An extension called `write` would compile, resolve to
Spark's, and quietly do something else — the same shadowing trap the column
handles carry a warning about.

On Hive: whether the catalogue is backed by a metastore is a property of the
**session** — `enableHiveSupport()` on classic, server configuration on Sail —
not something a value can carry. `Storage.catalog` says "this lives in a
catalogue table" and leaves the catalogue's identity to whoever built the
session, which is also why it works unchanged on both engines.

### Partitioned tables, where the ordering rule bites

`Storage.partitioned[T](table, Seq("day"))` is the same contract plus the one
rule partitioning adds and nothing enforces: **a partition column is moved to
the end of the table's schema**, whatever order you declared it in. Both engines
agree on that, and `PartitionedSpec` measures it —

```sql
CREATE TABLE t (day DATE, k STRING, v INT) USING parquet PARTITIONED BY (day)
-- spark.table("t").schema.fieldNames  =>  k, v, day
```

— which matters because `insertInto` matches by **position**. A model listing
`day` first writes the date into whatever column happens to be first. On classic
that surfaces as `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST`, an error
about casting that says nothing about ordering. On Sail it does not surface at
all: the same `INSERT` in declaration order simply succeeds, because Sail keeps
the order you wrote. Friendlier in isolation, and a portability hazard in a
project that runs on both.

So the instance checks the model's field order at write time and refuses with a
message about the actual problem, before anything is written. What it
deliberately does *not* do is set `partitionOverwriteMode` for you — that is a
session-wide setting Sail ignores, in the data-destroying direction described
above, and inheriting it silently from a storage instance is the last way anyone
should meet it.

## What we know about Sail from the JVM

Everything here was measured against `pysail` 0.7.0 with the 4.2.0 JVM client,
and every line is pinned by a test that goes red if it stops being true. Sail is
under active development, so several of these read as gaps rather than as
decisions — which is exactly why they are tripwires and not documentation.

### What does not run

| | on Sail | what it answers |
|---|---|---|
| `ds.map(lambda)` | ✗ | `wildcard with plan ID` |
| `ds.filter(lambda)` | ✗ | `wildcard with plan ID` |
| `ds.flatMap(lambda)` | ✗ | `wildcard with plan ID` |
| `ds.groupByKey(lambda)` | ✗ | `Scala UDF is not supported yet` |
| `ds.queryExecution` | ✗ | `UNSUPPORTED_CONNECT_FEATURE.DATASET_QUERY_EXECUTION` |

The first four are one cause: Connect ships a closure as JVM bytecode and Sail
has no JVM. `queryExecution` is a different one — a Connect limitation that any
Connect server shares, not a Sail gap.

Worth noticing that `groupByKey` is the only one that names the reason. The
other three die earlier, on a wildcard, and the accurate message Sail already
has is never reached — `resolve_map_partitions` resolves the UDF's arguments
before it looks at what kind of UDF it is. That is a small, self-contained fix
in Sail, and the pinned message in `TypedEtlSpec` is what will notice it landing.

### What runs, and is more than folklore suggests

Encoders are derived on the **client**, so the typed API survives almost intact:
`as[T]`, `Dataset[T]`, `Option[T]`, `toDS()`, typed `collect()`. So do joins,
aggregations, window functions, `explode_outer`, temp views, catalogue tables,
`try_cast`, and `Dataset.transform` — which is a concrete method on the shared
API, applied client-side while the plan is built, and therefore never shipped.

The rule that predicts the rest: **anything expressible as a column travels;
anything needing bytecode executed server-side does not.**

### Where the answers differ

| | classic | Sail |
|---|---|---|
| invalid cast | `CAST_INVALID_INPUT` | `Cast error: Cannot cast string ...`, wrapped as `CONNECT_CLIENT_UNEXPECTED_MISSING_SQL_STATE` |
| `to(schema)` missing a column | invents it, filled with NULL | refuses: `field not found in input schema` |
| `DECIMAL(18,2) * 2` | `decimal(20,2)` | `decimal(29,2)` |
| `DECIMAL(38,18) * 2` | `decimal(38,16)` | `decimal(38,18)` |
| `DECIMAL(38,18) + itself` | `decimal(38,17)` | `decimal(38,18)` |
| `DECIMAL(38,18) / 2` | `decimal(38,18)` | `decimal(38,22)` |
| the query plan | Catalyst: `Project`, `Filter` | DataFusion: `ProjectionExec`, `FilterExec` |
| `spark.sql.session.timeZone`, on a timestamp the engine computes | applied | reported back, then ignored: answers in UTC |
| `partitionOverwriteMode=dynamic` | replaces only the partitions written | reported back, then ignored: replaces the whole table |

The decimal rows have a sharper diagnosis than the table shows, and it is in
`DecProbe`: the two agree whenever **both** operands declare their precision.
`DECIMAL(18,2) * DECIMAL(1,0)` is `(20,2)` on both; `* DECIMAL(10,0)` is `(29,2)`
on both. Only the bare literal parts them — Catalyst narrows `2` to its smallest
decimal, Sail keeps it at an `Int`'s width. Values stay equal either way; it is
the schema that moves, which is invisible until the result meets a table.

On the `to(schema)` row, note which engine is stricter: Sail refuses, classic
invents nulls. That is the argument for `Conform` checking before it calls `to`
— the guard is what makes both engines answer the same way.

The last two rows are a different kind of divergence from the rest of the table,
and worse. Everything above them either raises or changes a schema, so something
somewhere notices. These two are **settings that are accepted and then not
applied**: `spark.conf.get` hands the value straight back, nothing raises, and
the answer is quietly different.

`partitionOverwriteMode` is the expensive one. The standard way to reprocess a
day is `mode("overwrite").insertInto(table)` with the mode set to `dynamic`, so
only the partitions being written are replaced. Measured in `PartitionedSpec`:
classic replaces one day and keeps the other; Sail deletes every partition the
job did not write. A daily job that reruns yesterday every morning would truncate
its own history every morning, successfully. Until that is fixed in Sail, the
portable choice is `static` — both engines agree on it, and it is at least
destructive in a way you asked for.

The timezone row is narrower than it looks. A timestamp that travels **in the
data** is read identically by both engines, which is why every fixture in this
project is safe and why nothing here caught this for so long. It is only a
timestamp the engine *computes* — `timestamp_seconds` and friends — where Sail
ignores the session zone. `TimeZoneSpec` measures both halves across four zones.

### What is not fixed here

Nothing in this repository changes what Sail can do. The macro spike lets
Dataset-shaped code run against Sail, but only by **changing the code**, which
is the opposite of Sail's premise: swap the server, leave the job alone. The two
improvements identified for Sail itself — the resolver ordering above, and the
literal narrowing — are written down, not implemented. Both are small and
self-contained.

What is deliberately *not* on that list is making the typed lambda run. The next
section is why that one is worth refusing rather than building.

## The typed lambda was never the fast path

> Every operation Sail refuses is exactly the one that was costing you a full
> scan on classic, silently.

The obvious reading of the table above is that Sail is less capable: classic runs
`ds.map(_.amount * 2)` and Sail does not. Measuring what classic actually *does*
with it changes that reading — and the sentence above is a measurement, not a
slogan. All five typed closures were checked, not the two that were convenient:

| closure, on classic | columns read | column form | columns read |
|---|---|---|---|
| `filter(_.amount > 50)` | 5 of 5 | `filter(col(...))` | 2 of 5 |
| `map(_.amount)` | 5 of 5 | `select(col(...))` | 1 of 5 |
| `groupByKey(_.country)` | 5 of 5 | `groupBy(col(...))` | 1 of 5 |
| `flatMap(...)` | 5 of 5 | `explode(...)` | 1 of 5 |
| `reduce(_ + _)` | 5 of 5 | `sum(col(...))` | 1 of 5 |

`day` is the witness in `PushdownSpec`: not one of those closures mentions it,
and every one of them loads it.

And with a filter in play, the pushdown goes too. Reading a five-column parquet
table and keeping one column, filtered:

| how it is written | filters pushed down | columns read |
|---|---|---|
| `filter(col("amount") > 50)` | `IsNotNull, GreaterThan(amount,50.00)` | 2 of 5 |
| `filterExpr(_.amount > 50)` (macro) | `IsNotNull, GreaterThan(amount,50.00)` | 2 of 5 |
| `filter(_.amount > 50)` (lambda) | **none** | **5 of 5** |
| `map(_.branch)` (lambda) | **none** | **5 of 5** |
| `mapExpr(_.branch)` (macro) | — | 1 of 5 |
| `select(col("branch"))` | — | 1 of 5 |

A closure costs three things at once, and classic charges all three silently. No
predicate reaches the file, so rows are read only to be discarded. No projection
reaches it either, so every column is read to satisfy a lambda that touches one.
And each row is deserialised into a JVM object so the closure has something to
run against. On two rows this is invisible; on a billion it is the difference
between reading 200 GB and reading 12.

Note the first row of that table: it is a `Dataset[Sale]`, fully typed, with
pushdown and pruning intact. **It is not the Dataset that loses the
optimisations — it is the closure.** Catalyst cannot see inside bytecode, so it
cannot reason about it, move it, or push it anywhere.

And Sail does the same optimisations, more visibly. Its plan for the column form
spells them out:

```
DataSourceExec: file_groups={...},
  projection=[branch, amount],
  predicate=amount@3 > Some(5000),18,2,
  pruning_predicate=amount_null_count@1 != row_count@2
                    AND amount_max@0 > Some(5000),18,2
```

Column pruning, predicate pushdown, and row-group pruning from the parquet
statistics — DataFusion writes the last one into the plan, where Catalyst keeps
it to itself.

### So which engine treats you worse?

| | `filter(_.amount > 50)` |
|---|---|
| classic | runs — 5 columns of 5, no pushdown, every row deserialised, **silently** |
| Sail | refuses |

Seen from performance rather than from features, classic is the one being unkind:
it hands you the slow path without mentioning it, and you find out only by
reading a plan, which almost nobody does. A loud failure gets fixed the same
afternoon; a job reading 200 GB instead of 12 can live in production for years.

This is also the argument against building the JVM UDF path into Sail. It would
be months of work — a JNI bridge, a JVM lifecycle, a helper jar — to deliver an
execution mode that is *slower than the alternative that already works*. The
error message is worth fixing. The feature behind it, for anything a column can
express, is worth refusing.

Where the refusal does cost something real: a closure that calls arbitrary Scala
— a library, a lookup, logic no column can spell — has no column form to fall
back on. That is a genuine limit, not a disguised favour, and no macro here
changes it.

## Where the engines differ, and why nothing is skipped

A handful of specs behave differently on the two backends. The tempting move is
to mark those skipped — `ignore`, `pending`, `cancel` — so the build goes green.
The cost is that they go **quiet forever**: the day Sail closes the gap, nothing
tells you, and the skip outlives its reason by years.

So nothing here skips. `EngineDivergence` gives the two shapes a name, and both
arms stay live:

```scala
// The two disagree about how they behave: NULL against a refusal, one error
// class against another.
perEngine {
  short.to(Conform[Product].schema).collect().head.isNullAt(2) shouldBe true
} {
  intercept[Exception](short.to(Conform[Product].schema).collect())
    .getMessage should include("field not found in input schema: family")
}

// Works on classic, expected to fail on Sail — asserted, not tolerated.
failsOnSail()(sales.map(_.amount * 2).collect())
```

`failsOnSail` is the closest thing here to an expected failure, and it is
deliberately stricter than one: it asserts the failure, so if Sail ever runs the
lambda the test turns **red** and says the expectation is stale. That is the
point. A green build that has stopped checking anything is worse than a red one.

Three divergences are pinned this way today: the Scala lambda that Connect
cannot ship, the error class on an invalid cast, and `to` over a frame missing a
column.

## Reading the plan

Nothing substitutes for looking at what the engine actually decided to do, and
this is where the two stop pretending to be the same. Everything else here is
written once and passes on both; a plan is Catalyst's on one side and
DataFusion's on the other, and they do not share a word of vocabulary:

```
classic                          Sail
== Physical Plan ==              == Physical Plan ==
*(1) Project [id AS a, ...]      ProjectionExec: expr=[#1@0 as a, ...]
+- *(1) Filter (id > 1)            FilterExec: #0@0 > 1
   +- *(1) Range (1, 4, ...)         RepartitionExec: RoundRobinBatch(12)
                                       RangeExec
```

`explain` prints on both, so `Plans.of(ds)` captures what it printed and hands
back a string. `queryExecution` — the richer door, which returns the plan as an
object — is **classic only**: over Connect it raises
`UNSUPPORTED_CONNECT_FEATURE.DATASET_QUERY_EXECUTION`. `PlanSpec` pins that with
`failsOnSail`, because it is the first API plan-inspection code reaches for.

That measurement earned its place by correcting this README. The paragraph about
chained `withColumn` used to claim the nesting reached the executed query. It
does not — Catalyst collapses it — and the numbers live in `ClassicPlanSpec` now
rather than in anybody's memory.

It sits in `findings/classic/test` rather than alongside the shared specs, and the
reason is a category of divergence the rest of this README does not cover.
`perEngine` and `failsOnSail` handle engines that *behave* differently at run
time. Connect's `QueryExecution` is a different class without `analyzed` or
`optimizedPlan`, so code touching them does not **compile** for the connect
backend — no runtime branch can rescue that, and a backend-specific source
directory is the only thing that says it.

## Best practices, measured

Most advice about writing fast Spark is a rule about how to phrase a query.
`PracticeSpec` takes eight of them, writes each query the naive way and the
recommended way, and compares the **physical plans** on both engines. If the
optimiser already does it, the two plans are the same and the rule is folklore:
true once, obsolete now, still enforced in code review.

| rule | classic | Sail |
|---|---|---|
| chained `withColumn` → one `select` | same plan | same plan |
| filter before the join, not after | same plan | same plan |
| project before the join, not after | same plan | same plan |
| `distinct` → `dropDuplicates` | same plan | same plan |
| filter before the `union`, not after | same plan | same plan |
| `orderBy().limit()` → don't sort it all | already a top-N | already a top-N |
| `broadcast(small)`, default threshold | same plan | same plan |
| `broadcast(small)`, **auto-broadcast off** | `SortMergeJoin` → `BroadcastHashJoin` | same plan |

Seven of the eight change nothing, on either engine. That is the useful result,
and it is not an argument for writing queries carelessly — `select` over chained
`withColumn` is still the house rule here, because it says the output shape in
one place. It is an argument about *why*: legibility, not speed. The practices
worth arguing over are the ones no optimiser can do for you, and this repository
has one — a closure hides the predicate from the planner entirely, and
`PushdownSpec` measures a full scan where a column would have pushed down.

The last row is the exception, and it is pointed. Once the automatic broadcast
is out of the way, the hint is the one rewrite in this table that still changes a
plan on classic — and it is the one Sail does not implement. Nothing raises;
Sail's plan simply does not move, because it already collects the build side and
consults neither the hint nor the threshold.

Note what the seventh row costs if you stop at it. With the default threshold the
dimension is small enough that classic broadcasts it anyway, so the hint looks
useless — a conclusion about this fixture masquerading as one about hints. The
eighth row exists because the seventh alone would teach the wrong thing.

`Plans.shape` is what makes the comparison possible. Both engines stamp every
expression with an allocation counter — `amount#35` and `[plan_id=101]` on
Catalyst, `#26@0` and `as #26` on DataFusion — so two plans built from two
different `Dataset` values never compare equal as strings, however identical they
are. Stripping the counters is the difference between a comparison that means
something and one that always says "no". DataFusion's positional `@0` is left
alone: that one moves when the shape does, which is exactly what should be
noticed.

## The wire, and what you can do before it leaves

`Plans.of` shows what the engine decided. It cannot show what the client *asked
for*, because by the time `explain` speaks the request has already been
interpreted — and when the engines disagree, which of them invented the
difference is exactly the question. `WirePlan.of(ds)` is the other end: the
request protobuf, read locally, with no server and nothing executed. There is no
classic equivalent and there cannot be, so it lives in `backend/connect`.

It paid for itself immediately. `ClosureSpec` had long recorded that Sail names
`groupByKey` accurately (`Scala UDF is not supported yet`) and reports the other
four closures as an unresolved wildcard, and explained it by reasoning about the
order of Sail's resolver. `WirePlanSpec` stops that being an inference: the cause
is one optional field in the request. `filter` and `map` hand the UDF a wildcard
carrying a `plan_id`; `groupByKey` hands it one without. Sail resolves a UDF's
arguments before it inspects the function, so the first four die on the plan id
and never reach the branch that knows what a Scala UDF is.

The second use is a guard rather than a diagnostic. `ClosureGuard` is a gRPC
interceptor that refuses a plan containing a Scala closure **before it is sent**:

```scala
val spark = ClosureGuard.install(SparkSession.builder().remote(url)).create()

spark.range(10).filter(_ > 5L).count()
// devel0pez.ClosureNotSupported: `filter` with a Scala closure at Job.scala:42.
// Sail has no JVM to run it on. Rewrite it with Columns — which also recovers
// the predicate pushdown and the column pruning the closure gives up on
// classic Spark.
```

Both halves of that message are things no server could say. The operation name
and the **file and line** come from the JVM origin the client attaches to the
expression; Sail has never seen your source and never will. Compare what it
replaces: `[UNRESOLVED_WILDCARD_WITH_PLAN_ID]`, which names neither the closure
nor the reason.

What the guard deliberately does **not** do is rewrite the plan. That idea is
tempting and it does not work: the Connect client sends plans *unresolved*, by
design, so collapsing two projections would mean substituting one expression into
another without knowing whether `col("b")` means the column just added or one
that was already there. That needs the schema, which the client does not have.
The right altitude for a client to optimise at is the API — which is where the
`Expr` macro operates, on the Scala AST, while the types are still attached.

One wrinkle worth recording, since it costs an afternoon to rediscover:
`SparkSession.Builder.interceptor` **cannot be called from Scala source** in
Spark 4.2.0. Spark shades gRPC, rewriting the bytecode so the method really takes
`org.sparkproject.io.grpc.ClientInterceptor`, but the shading step does not
rewrite the `ScalaSignature`, which still names the original `io.grpc` class —
one that is on no classpath anywhere. `scalac` reads the stale signature and
fails. `ClosureGuard.install` binds through reflection instead, against the
descriptor that is actually there.

## Analytics

`AnalyticsSpec` covers what people do once the data has landed, which is where
engines are likeliest to differ: `explode_outer` over an array column,
`countDistinct`, window functions (`row_number`, a running sum, and a moving
average over `rowsBetween(-2, 0)`), and a `createOrReplaceTempView` queried with
SQL. It also round-trips a case class holding a `Seq[String]`. All of it passes
on both engines.

## Dependencies: three bots, because none of them covers everything

| What it updates | Who | Where |
| --------------- | --- | ----- |
| Scala dependencies (Spark, ScalaTest, sbt) | Scala Steward | `.github/workflows/scala-steward.yml` |
| The GitHub Actions themselves | Dependabot | `.github/dependabot.yml` |
| Nix inputs (`flake.lock`) | update-flake-lock | `.github/workflows/update-flake-lock.yml` |

**Dependabot does not support sbt.** Its JVM ecosystems are `maven` and
`gradle`, so in an sbt project it would see neither Spark nor ScalaTest. Scala
Steward, the ecosystem standard, covers that gap. And neither of them looks at
`flake.lock`, which would stay frozen forever without the third.

One caveat: **no bot knows about `versions.json`**. Scala Steward reads
`build.sbt`, and there the version sits behind a function. Bumping Spark means
editing that file by hand, which is precisely what forces pysail to be bumped
alongside it. `SailVersionSpec` is there so it does not get forgotten.

For Scala Steward's PRs to arrive with CI already run you need a PAT in the
`SCALA_STEWARD_TOKEN` secret: PRs opened with the default `GITHUB_TOKEN` do not
trigger workflows. Without the secret the bot still works, but CI has to be
launched by hand on each PR.
