{
  description = "⛵ dev-nix-sail-scala - Nix-configured development environment for Spark on Scala";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    devshell.url = "github:numtide/devshell";
    devshell.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = { self, nixpkgs, flake-utils, devshell }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ devshell.overlays.default ];
        };

        # Spark 4 needs Java 17 or newer: it compiles with
        # `maven.compiler.release=17`, so on anything older it does not even
        # link. 21 is the other version Spark 4 supports, and the one this
        # template runs on.
        jdk = pkgs.jdk21;

        # nixpkgs' sbt ships a JDK of its own and exports its JAVA_HOME on the
        # way in, which silently beats the devshell's. Without this override the
        # shell reports one version while every compile and every test runs on
        # another — `java -version` said 17 here while sbt's welcome banner said
        # 21, and both were telling the truth. Pointing sbt's `jre` at the same
        # derivation makes `jdk` above the single place the version is decided,
        # and `build.sbt` then pins the forked test JVMs to that same JAVA_HOME.
        #
        # As of this nixpkgs the override rebuilds nothing: sbt already defaults
        # to this very JDK, so the store path is unchanged. What it buys is that
        # the agreement stops being a coincidence — the day nixpkgs bumps sbt's
        # default and not `pkgs.jdk21`, the shell follows instead of drifting
        # apart in silence, which is precisely how this was missed the first time.
        sbt = pkgs.sbt.override { jre = jdk; };

        # A single source of truth for the versions, shared with build.sbt.
        versions = builtins.fromJSON (builtins.readFile ./versions.json);

        python = pkgs.python312;

        # Sail is a Rust binary shipped as a Python wheel, and it is not in
        # nixpkgs, so a venv is the only way to get it. No Python is written
        # here: this only brings up the server the `connect` backend's tests
        # talk to.
        #
        # It carries pyspark too, and not on a whim: Sail asks the `pyspark`
        # module on its own side which Spark version to serve. Without it,
        # `spark.version` answers "No module named 'pyspark'" and expressions
        # fail. That is why both versions come from the same versions.json the
        # JVM client uses: paired by construction.
        venvSail = ''
          venv="$PRJ_ROOT/.venv-sail"
          wanted="pysail==${versions.pysail} pyspark==${versions.spark}"

          # The stamp alone is not enough: a half-written install leaves
          # `sail` in place with the right stamp but a `pyspark` that
          # imports as an empty namespace package, and the failure only
          # shows up much later as "no attribute '__version__'".
          if [ ! -x "$venv/bin/sail" ] \
             || [ "$(cat "$venv/.version" 2>/dev/null)" != "$wanted" ] \
             || ! "$venv/bin/python" -c "import pyspark; pyspark.__version__" >/dev/null 2>&1; then
            echo "Installing the Sail server ($wanted)..."
            rm -rf "$venv"
            ${python}/bin/python -m venv "$venv"
            "$venv/bin/pip" install --quiet --upgrade pip
            if "$venv/bin/pip" install --quiet $wanted; then
              echo "$wanted" > "$venv/.version"
            else
              echo "Could not install Sail: the connect backend will not work." >&2
            fi
          fi

          export PATH="$venv/bin:$PATH"
        '';
      in {
        devShells.default = pkgs.devshell.mkShell {
          name = "dev-nix-sail-scala";

          motd = ''
            {202}⛵ dev-nix-sail-scala{reset} - Spark ${versions.spark} on Scala ${versions.scala}
            $(type -p menu &>/dev/null && menu)
          '';

          packages = [ jdk sbt pkgs.scalafmt pkgs.coursier pkgs.fzf ];

          # An override, not a default. sbt and Spark read JAVA_HOME before the
          # PATH, so a JAVA_HOME inherited from outside (SDKMAN, say) decides
          # which JVM runs and this shell's jdk21 is never used. With an
          # inherited JDK 11, Spark 4 does not even compile.
          env = [
            { name = "JAVA_HOME"; value = jdk.home; }
            # sbt outside the Nix sandbox caches here; pinned so CI knows which
            # directory to save.
            { name = "COURSIER_CACHE"; eval = "\${COURSIER_CACHE:-$HOME/.cache/coursier}"; }
          ];

          commands = [
            {
              category = "test";
              name = "t";
              help = "Run the whole suite against both backends";
              command = ''sbt -batch classic/test connect/test "$@"'';
            }
            {
              category = "test";
              name = "tc";
              help = "Test against classic Spark on the local JVM";
              command = ''sbt -batch classic/test "$@"'';
            }
            {
              category = "test";
              name = "ts";
              help = "Test against Sail over Spark Connect";
              command = ''sbt -batch connect/test "$@"'';
            }
            {
              category = "console";
              name = "sail-server";
              help = "Start a Sail server in the foreground (port 50051)";
              command = ''sail spark server "$@"'';
            }
            {
              category = "test";
              name = "tt";
              help = "Run one suite, e.g. tt BaseCaseSpec";
              command = ''sbt -batch "testOnly *$1"'';
            }
            {
              category = "build";
              name = "c";
              help = "Compile main and test sources";
              command = ''sbt -batch compile Test/compile "$@"'';
            }
            {
              category = "build";
              name = "run-demo";
              help = "Run the demo (Main)";
              command = ''sbt -batch run "$@"'';
            }
            {
              category = "console";
              name = "cscala";
              help = "Scala REPL with Spark and the project on the classpath";
              command = ''sbt console'';
            }
            {
              category = "lint";
              name = "f";
              help = "Format the code with scalafmt";
              command = ''scalafmt "''${@:-.}"'';
            }
            {
              category = "lint";
              name = "fc";
              help = "Check formatting without writing";
              command = ''scalafmt --test "''${@:-.}"'';
            }
            {
              category = "env";
              name = "clean-all";
              help = "Delete target/ and the sbt build cache";
              command = ''rm -rf "$PRJ_ROOT/target" "$PRJ_ROOT/project/target" "$PRJ_ROOT/project/project"'';
            }
          ];

          devshell.startup.sail.text = venvSail;

          devshell.interactive.fzf.text = ''
            eval "$(fzf --bash)"
            export PS1="⛵ \[\e[36m\]\W\[\e[0m\] $ "
          '';
        };
      }
    );
}
