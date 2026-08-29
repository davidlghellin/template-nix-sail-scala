package devel0pez.macros

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

import org.apache.spark.sql.{Column, Dataset, Encoder}

/** A spike: turn a typed lambda into a `Column` at **compile time**.
  *
  * `ds.map(_.amount * 2)` cannot run on Sail, because Spark Connect ships the lambda as JVM
  * bytecode and there is no JVM on the far side. The usual answer is to write the column form by
  * hand. This asks a different question: could the lambda be translated before it is ever sent?
  *
  * {{{
  * Expr.of[Sale](_.amount * 2)   // becomes  col("amount") * lit(2)
  * }}}
  *
  * Note where the work happens. Nothing is decompiled and nothing is analysed at run time: at
  * compile time the typed AST of the lambda is right there, which is a far better starting point
  * than bytecode. Spark has wanted the bytecode version since 2016 (SPARK-14083, still open); this
  * is the cheap half of the same idea.
  *
  * The failure mode is the point of the exercise. A lambda outside the supported subset does not
  * compile, with a message naming the part that could not be translated. It cannot produce a
  * plausible-but-different answer at run time, which is the outcome that would actually hurt.
  *
  * Supported: reading a field of the lambda's parameter, the arithmetic, comparison and boolean
  * operators below, and any subexpression that does not mention the parameter — the last of those
  * carries literals, including the implicit conversions Scala inserts around them.
  *
  * Not supported, on purpose: method calls, conditionals, anything touching the outside world.
  * Those are what make a closure a closure.
  *
  * Two limits that are documented rather than enforced, because neither can be settled from the
  * lambda alone. `&&` and `||` short-circuit over `Boolean` in Scala and work in three-valued logic
  * in Spark, so a NULL operand behaves differently. And the columns emitted are free `col(...)`
  * references, not bound to a dataset the way `Model`'s handles are — so an expression built here
  * cannot disambiguate a join where both sides carry a column of the same name.
  */
object Expr {

  /** Translate `f` into a `Column`, or fail to compile saying why. */
  def of[T](f: T => Any): Column = macro ExprMacro.impl[T]

  implicit final class DatasetOps[T](private val ds: Dataset[T]) extends AnyVal {

    /** `map`'s shape without `map`'s closure: the lambda becomes a projection.
      *
      * Spelled `mapExpr` because `map` is taken. `Dataset` has one, and a member always beats an
      * implicit conversion — an extension called `map` would compile, resolve to Spark's, and go on
      * failing against Sail exactly as before. The same rule that stops `Storage` naming its writer
      * `write`.
      *
      * Produces a **single** column, so `U` has to be something an encoder can read from one: a
      * scalar, not a case class.
      */
    def mapExpr[U](f: T => U)(implicit encoder: Encoder[U]): Dataset[U] =
      macro ExprMacro.mapImpl[T, U]

    /** `filter`'s shape without `filter`'s closure: the predicate becomes a `Column`.
      *
      * The same story as `mapExpr` and, in an ETL, the one reached for more often. `filter` on a
      * typed lambda fails against Sail exactly as `map` does — both die on the same wildcard.
      */
    def filterExpr(f: T => Boolean): Dataset[T] = macro ExprMacro.filterImpl[T]
  }
}

/** A `Dataset` wrapper whose `map` and `filter` are the macros.
  *
  * `Dataset.map` cannot be intercepted: a member always beats an implicit conversion, so an
  * extension named `map` would compile and resolve to Spark's. The way round it is not to extend
  * `Dataset` but to wrap it — on a type of our own, `map` is a name like any other.
  *
  * The call site then reads exactly like the code that fails:
  *
  * {{{
  * val sales = TypedDataset(spark.table("sales").as[Sale])
  * sales.filter(_.amount > 50).map(s => Out(s.country, s.amount * 2)).dataset
  * }}}
  *
  * `dataset` gets the real `Dataset` back, for everything this wrapper deliberately does not try to
  * cover — which is most of the API.
  */
final class TypedDataset[T](val dataset: Dataset[T]) {

  /** `map`, translated to a projection at compile time. */
  def map[U](f: T => U)(implicit encoder: Encoder[U]): TypedDataset[U] =
    macro ExprMacro.typedMapImpl[T, U]

  /** `filter`, translated to a `Column` at compile time. */
  def filter(f: T => Boolean): TypedDataset[T] = macro ExprMacro.typedFilterImpl[T]
}

object TypedDataset {
  def apply[T](dataset: Dataset[T]): TypedDataset[T] = new TypedDataset(dataset)
}

private[macros] object ExprMacro {

  /** Scala operator (decoded) to the `Column` operator that spells the same thing.
    *
    * Spelling the same is not meaning the same, which is why this map is not the whole story. `/`,
    * `%` and `+` are guarded by operand type below, and `==` / `!=` are translated to the null-safe
    * comparisons rather than to `===` / `=!=`. Translating those blind would produce a different
    * answer instead of an error — the one outcome this macro exists to rule out.
    */
  private val Operators: Map[String, String] = Map(
    "*" -> "*",
    "+" -> "+",
    "-" -> "-",
    "/" -> "/",
    "%" -> "%",
    ">" -> ">",
    "<" -> "<",
    ">=" -> ">=",
    "<=" -> "<=",
    "&&" -> "&&",
    "||" -> "||"
  )

  /** Handled separately: Scala's `==` is null-safe, Spark's `===` propagates NULL. */
  private val Equality: Set[String] = Set("==", "!=")

  def impl[T: c.WeakTypeTag](c: blackbox.Context)(f: c.Expr[T => Any]): c.Expr[Column] = {
    import c.universe._
    c.Expr[Column](translateLambda(c)(f.tree))
  }

  def mapImpl[T: c.WeakTypeTag, U: c.WeakTypeTag](
      c: blackbox.Context
  )(f: c.Expr[T => U])(encoder: c.Expr[Encoder[U]]): c.Expr[Dataset[U]] = {
    import c.universe._
    // `c.prefix` is the implicit class wrapping the dataset; unwrap it to get the dataset back.
    val dataset = c.prefix.tree match {
      case Apply(_, List(inner)) => inner
      case other                 => other
    }
    val target = weakTypeOf[U]

    // The field names `U` declares, in declaration order, when it is a case class.
    val fields: Option[List[String]] = target.decls
      .collectFirst { case m: MethodSymbol if m.isPrimaryConstructor => m }
      .map(_.paramLists.head.map(_.name.decodedName.toString))

    // `s => Out(s.country, s.amount * 2)` is the shape typed ETLs are actually written in, so it
    // gets its own case: each constructor argument becomes a column, aliased to the field it
    // fills. Anything else is a single expression and produces a single column.
    val columns: List[Tree] = translateBody(c)(f.tree, target, fields)

    c.Expr[Dataset[U]](q"$dataset.select(..$columns).as[$target]($encoder)")
  }

  def filterImpl[T: c.WeakTypeTag](
      c: blackbox.Context
  )(f: c.Expr[T => Boolean]): c.Expr[Dataset[T]] = {
    import c.universe._
    val dataset = c.prefix.tree match {
      case Apply(_, List(inner)) => inner
      case other                 => other
    }
    val predicate = translateLambda(c)(f.tree)
    c.Expr[Dataset[T]](q"$dataset.filter($predicate)")
  }

  def typedMapImpl[T: c.WeakTypeTag, U: c.WeakTypeTag](
      c: blackbox.Context
  )(f: c.Expr[T => U])(encoder: c.Expr[Encoder[U]]): c.Expr[devel0pez.macros.TypedDataset[U]] = {
    import c.universe._
    val target = weakTypeOf[U]
    val fields = target.decls
      .collectFirst { case m: MethodSymbol if m.isPrimaryConstructor => m }
      .map(_.paramLists.head.map(_.name.decodedName.toString))
    val columns = translateBody(c)(f.tree, target, fields)
    // `dataset` is a public val on the wrapper, so the prefix can be used directly rather than
    // taken apart — which is what made this simpler than the implicit-class version.
    c.Expr[devel0pez.macros.TypedDataset[U]](
      q"""new _root_.devel0pez.macros.TypedDataset(
            ${c.prefix}.dataset.select(..$columns).as[$target]($encoder))"""
    )
  }

  def typedFilterImpl[T: c.WeakTypeTag](
      c: blackbox.Context
  )(f: c.Expr[T => Boolean]): c.Expr[devel0pez.macros.TypedDataset[T]] = {
    import c.universe._
    val predicate = translateLambda(c)(f.tree)
    c.Expr[devel0pez.macros.TypedDataset[T]](
      q"new _root_.devel0pez.macros.TypedDataset(${c.prefix}.dataset.filter($predicate))"
    )
  }

  /** One column, or one per field when the lambda **constructs** the target case class.
    *
    * "Constructs" is checked, not guessed. An earlier version matched any `Apply` whose arity
    * happened to equal the field count, which silently mistranslated a call to any other method
    * returning the same type: `s => swapped(s.a, s.b)` was compiled as `Target(s.a, s.b)`, quietly
    * ignoring what `swapped` did. It compiled, it ran, and it answered something else — the exact
    * failure this macro exists to rule out.
    */
  private def translateBody(
      c: blackbox.Context
  )(lambda: c.Tree, target: c.Type, fields: Option[List[String]]): List[c.Tree] = {
    import c.universe._

    /** Is this the constructor of `target`, or its companion's `apply`? */
    def builds(fn: Tree): Boolean = {
      val symbol = fn.symbol
      symbol != null && symbol.isMethod && {
        val owner = symbol.owner
        val targetSymbol = target.typeSymbol
        (symbol.asMethod.isConstructor && owner == targetSymbol) ||
        (symbol.name == TermName("apply") &&
          (owner == targetSymbol.companion || owner == targetSymbol.companion.asModule.moduleClass))
      }
    }

    lambda match {
      case Function(params, Apply(fn, args))
          if args.nonEmpty && fields.exists(_.length == args.length) && builds(fn) =>
        args.zip(fields.get).map { case (arg, name) =>
          val column = translateLambda(c)(Function(params, arg).asInstanceOf[c.Tree])
          q"$column.as($name)"
        }
      case other => List(translateLambda(c)(other))
    }
  }

  /** The translation itself, shared by both entry points. */
  private def translateLambda(c: blackbox.Context)(lambda: c.Tree): c.Tree = {
    import c.universe._

    // The lambda may arrive wrapped: a block with no statements, or an ascription.
    def unwrap(tree: Tree): Tree = tree match {
      case Block(Nil, inner) => unwrap(inner)
      case Typed(inner, _)   => unwrap(inner)
      case other             => other
    }

    val (param, body) = unwrap(lambda) match {
      case Function(List(ValDef(_, name, _, _)), b) => (name, unwrap(b))
      case other =>
        c.abort(
          c.enclosingPosition,
          s"a lambda of one argument is required, got: ${showCode(other)}"
        )
    }

    def mentionsParam(tree: Tree): Boolean = tree.exists {
      case Ident(name) => name == param
      case _           => false
    }

    // Whether a subtree can be lifted to a literal without changing when it runs.
    //
    // The first version of this asked only whether the tree mentioned the parameter, and that
    // was wrong in the way that matters: `_ => someCall()` mentions nothing, so it was lifted
    // to `lit(someCall())` — evaluated **once on the driver** instead of once per row.
    // A lambda promises per row. Silently turning that into a constant is exactly the class of
    // bug this design exists to avoid, so the rule is now positive: literals, stable
    // identifiers, and implicit conversions wrapped around either.
    def isConstant(tree: Tree): Boolean = unwrap(tree) match {
      case Literal(Constant(_)) => true
      case id: Ident => id.symbol != null && id.symbol.isTerm && id.symbol.asTerm.isStable
      case Apply(fn, List(inner)) if fn.symbol != null && fn.symbol.isImplicit => isConstant(inner)
      case _                                                                   => false
    }

    def translate(tree: Tree): Tree = unwrap(tree) match {
      // Escape hatch: an expression that is already a Column is spliced untouched, which is how
      // anything outside the subset can still be written inline.
      case column if column.tpe != null && column.tpe <:< typeOf[Column] => column

      // `2` in `_.amount * 2` arrives wrapped in whatever implicit conversion the expected type
      // demanded, and this carries it without having to recognise the wrapper.
      case constant if !mentionsParam(constant) && isConstant(constant) =>
        q"_root_.org.apache.spark.sql.functions.lit($constant)"

      // Mentions nothing, but is not obviously constant either — a method call, say. Refusing is
      // the only safe answer: the macro cannot tell "compute this once" from "compute it per row".
      case ambiguous if !mentionsParam(ambiguous) =>
        c.abort(
          c.enclosingPosition,
          s"Expr.of will not lift `${showCode(ambiguous)}` to a literal: it does not read the " +
            s"row, but it is not a literal either, so whether it should run once or once per row " +
            s"is not something this macro can decide. Wrap it in `lit(...)` to say once."
        )

      // Reading a field of the parameter is the whole point.
      case Select(Ident(name), field) if name == param =>
        q"_root_.org.apache.spark.sql.functions.col(${field.decodedName.toString})"

      case Apply(Select(lhs, op), List(rhs))
          if Operators.contains(op.decodedName.toString) ||
            Equality.contains(op.decodedName.toString) =>
        val name = op.decodedName.toString
        val leftType = unwrap(lhs).tpe
        val rightType = unwrap(rhs).tpe
        def left = translate(lhs)
        def right = translate(rhs)
        def columnOp(symbol: String) = TermName(symbol).encodedName.toTermName
        def isIntegral(t: Type) =
          t != null &&
            (t <:< typeOf[Int] || t <:< typeOf[Long] || t <:< typeOf[Short] || t <:< typeOf[Byte])

        name match {
          // Measured, not assumed. Integer division is the only one that parts them:
          //   5 / 2    Scala 2 (Int)      Spark 2.5 (Double)   <- differ
          //   5 / 2.0  Scala 2.5          Spark 2.5            <- agree
          //   5 % 2    Scala 1 (Int)      Spark 1 (Int)        <- agree
          // So the guard needs *both* operands to be integral, and `%` needs no guard at all.
          // Rejecting more than this refuses code that would have been perfectly correct.
          case "/" if isIntegral(leftType) && isIntegral(rightType) =>
            c.abort(
              c.enclosingPosition,
              s"`/` on two integers does not mean the same thing on both sides: Scala divides " +
                s"integers and gives an integer, Spark returns a Double — `5 / 2` is 2 in the " +
                s"lambda and 2.5 in the column. Cast one operand to a fractional type, or write " +
                s"this expression by hand."
            )

          // Spark refuses `+` on strings with DATATYPE_MISMATCH rather than concatenating, so
          // this one is a clearer error rather than a wrong answer — but earlier is better.
          case "+" if leftType != null && leftType <:< typeOf[String] =>
            c.abort(
              c.enclosingPosition,
              "`+` concatenates Strings in Scala but is arithmetic in Spark, which rejects it " +
                "with DATATYPE_MISMATCH. Use `concat` and write this expression by hand."
            )

          // Scala's `==` says `null == null` is true and `null == x` is false. Spark's `===`
          // answers NULL to both. `<=>` is the one that agrees with the lambda, so that is what
          // the lambda gets translated to.
          case "==" => q"$left.${columnOp("<=>")}($right)"
          case "!=" =>
            q"_root_.org.apache.spark.sql.functions.not($left.${columnOp("<=>")}($right))"

          case _ => q"$left.${columnOp(Operators(name))}($right)"
        }

      case unsupported =>
        c.abort(
          c.enclosingPosition,
          s"Expr.of cannot translate `${showCode(unsupported)}` into a Column. " +
            s"Supported: a field of the lambda's parameter, the operators " +
            s"${Operators.keys.toSeq.sorted.mkString(" ")}, and any subexpression that does not " +
            s"mention the parameter. Write this one as a column by hand."
        )
    }

    translate(body)
  }
}
