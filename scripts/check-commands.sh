#!/usr/bin/env bash
#
# Smoke test for the devshell commands the menu advertises.
#
# It exists because three of them were broken and nothing noticed: `run-demo` died
# with "No main class detected", `cscala` opened a REPL with no Spark on the
# classpath, and `clean-all` deleted three of the five target directories. All
# three had been documented and advertised for weeks.
#
# Checking that a command is on PATH would have caught none of them — they were
# all present and all wrong. So the cheap ones are actually run, and their output
# is checked for the specific way each one failed.
#
# Not covered, and deliberately: `t`, `tc` and `ts` are the test suites themselves,
# `sail-server` blocks by design, and `clean-all` would wipe the build. Those are
# existence checks only.

set -uo pipefail

failures=0

ok()   { printf '  \033[32m✓\033[0m %-12s %s\n' "$1" "${2-}"; }
bad()  { printf '  \033[31m✗\033[0m %-12s %s\n' "$1" "$2"; failures=$((failures + 1)); }

echo "Commands on PATH"
for cmd in t tc ts tt c run-demo cscala sail-server f fc clean-all menu; do
  if command -v "$cmd" >/dev/null 2>&1; then ok "$cmd"; else bad "$cmd" "not on PATH"; fi
done

echo
echo "Commands that are cheap enough to run"

# `c` — compiles every module.
if c >/dev/null 2>&1; then ok "c" "compiles"; else bad "c" "compile failed"; fi

# `fc` — the formatting check CI runs.
if fc >/dev/null 2>&1; then ok "fc" "formatting clean"; else bad "fc" "formatting check failed"; fi

# `run-demo` — used to die on the root project, which has no main class.
demo=$(run-demo 2>&1)
if grep -q "No main class" <<<"$demo"; then
  bad "run-demo" "No main class detected — is it running against the root project?"
elif grep -q "calculator: add(2, 3) = 5" <<<"$demo"; then
  ok "run-demo" "ran the demo"
else
  bad "run-demo" "ran but printed nothing recognisable"
fi

# `cscala` — used to open a REPL with neither Spark nor the project on it.
repl=$(printf 'import org.apache.spark.sql.SparkSession\nprintln("SPARK_ON_CLASSPATH")\n:quit\n' \
  | cscala 2>&1)
if grep -q "SPARK_ON_CLASSPATH" <<<"$repl"; then
  ok "cscala" "Spark is on the classpath"
else
  bad "cscala" "no Spark in the REPL — is it running against the root project?"
fi

echo
if [ "$failures" -eq 0 ]; then
  echo "All checked commands work."
else
  echo "$failures command(s) broken."
fi
exit "$failures"
