# Changelog

All notable changes to this plugin are documented here. Format loosely follows [Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### Security
- Cluster-side dependency resolution no longer installs an import name just because a PyPI
  package of that name exists; only curated mappings are installed and the rest are
  reported for the operator to add explicitly. The generated availability check follows
  the same rule.
- `aidp_executor` session calls carry a timeout; `run_migration.sh` logs to a private
  temp file and asks for `kill <PID>` instead of `pkill -f`.
- **`check_aws_creds.py` never prints a credential value** (SEC-NEW-DATABRICKS-02). The
  diagnostic reports where a credential is configured -- environment-variable names, Spark /
  Hadoop config keys, file paths, init-script line heads -- and for a credential-like value
  only that it is set and how long it is (`<set, N chars>`). Before this fix the env-var and
  `spark.conf` cells printed the first 80 characters of every `AWS_*` / `*SECRET*` value (a
  complete access key pair and session token) and init-script lines were echoed with their
  values; only the Hadoop cell masked, and the earlier entry here claiming otherwise was
  wrong. Masking is compiled into the cluster cells themselves (names containing KEY, SECRET,
  TOKEN, PASSWORD, PASSPHRASE, CREDENTIAL, ENCRYPT, DECRYPT and any non-allowlisted name
  containing AWS -- `AWS_*`, `AWSPASS`, `S3_AWS_SIGNATURE`; the s3a access / secret /
  session-token keys), so the value never leaves the kernel, and anything shaped like an AWS
  access-key id is scrubbed from the returned text as defence in depth. Init-script lines keep
  only a name-shaped head (`export AWS_SECRET_ACCESS_KEY=<40 chars redacted>`, `echo <47
  chars, rest redacted>`): a value-first `<secret>=key` line, an `echo <secret> > file`
  command and an `aws_secret_access_key: <secret>` fragment print no value. Importing the
  script no longer opens a cluster session.
- **Mandatory notebook sandbox gate** (`engine/aidp_compat/notebook_policy.py`,
  SEC-AIDP-SAMPLES-005). `dbutils.notebook.run` refuses to execute anything until a sandbox
  (`AIDP_SANDBOX_CATALOG` / `AIDP_SANDBOX_SCHEMA` / `AIDP_SANDBOX_PREFIX`, or an explicit
  `SandboxPolicy`) is declared, and AST-checks every non-magic cell before `exec`: process /
  network imports, `os.environ` / `os.getenv` / `os.system` / `os.popen` / `os.exec*`,
  `shutil.rmtree`, `eval` / `exec` / `compile` / `__import__`, `open()` on absolute paths
  outside the prefix, and `dbutils.fs.rm/mv/cp/put`, `saveAsTable` / `insertInto`,
  `df.write.*`, `spark.sql` DDL/DML whose literal target leaves the sandbox are refused with a
  `PermissionError` naming notebook path, cell index, rule id and remediation. Non-literal
  write targets are refused unless the rule id is listed in `AIDP_NOTEBOOK_POLICY_ALLOW`.
  `dbutils.fs.rm/mv/cp/put` and the `safe_io` write helpers assert their target at runtime.
  Refusals and allowed exceptions are recorded in a policy log that `job_migrate.py` renders
  into each task's test report (**Notebook Policy Log**); the cluster bootstrap declares the
  write-redirect schema/bucket as the sandbox automatically.
- **Sandbox gate hardening** (review of SEC-AIDP-SAMPLES-005 / 006). The bootstrap now also
  declares the staging areas the migrator itself generates (`/Volumes/default/default/dbfs/`,
  `/tmp/`) and the output directory as sandbox prefixes, so migrated FUSE / local writes are
  not refused. The policy is a one-shot snapshot per kernel: later `AIDP_SANDBOX_*` changes are
  ignored and redeclaring / clearing it is refused (`NBP-POLICY-TAMPER`). `path_in_sandbox`
  canonicalises paths, refuses `..` segments and (POSIX) symlinks leading outside. The AST gate
  follows import / shim aliases and single-assignment string constants across cells, refuses
  `importlib` / `builtins` / `sys.modules` / `getattr(os, ..)` (`NBP-INDIRECT`), imports and
  attribute rebinding of the compat shims and policy module (`NBP-POLICY-TAMPER`),
  `os.remove/rename/mkdir...`, `shutil.move/copy*` and `pathlib` file operations
  (`NBP-FS-PATH` / `NBP-FS-DYNAMIC`), `writeTo`, `option("path")` / `options(path=)`,
  double-quoted `LOCATION` and `OPTIONS (path ...)`, `dbutils.fs.mkdirs`, `os.fork`,
  `asyncio` subprocess helpers and more network modules; `open()` with an unresolvable path is
  refused in any mode and `spark.sql()` with an unresolvable statement is `NBP-SQL-DYNAMIC`.
  `dbutils.fs.mkdirs` asserts its target at runtime; `dbutils.fs.mount` no longer writes
  `extra_configs` to the process environment.
- **Sandbox gate: deny-by-default for indirect routes** (SEC-AIDP-SAMPLES-005 re-verification).
  The reviewer's probes `__builtins__['__import__']('os').system('id')`,
  `__builtins__['open']('/etc/passwd')`, `globals()['__builtins__'][...]`,
  `get_ipython().system('id')`, `spark._jvm...` and `sc._jvm...` all passed the AST gate at
  the first revision of PR #146. The scanner now resolves the builtins namespace through every door (`__builtins__`,
  `builtins`, `globals()` / `locals()` / `vars()`, `__dict__`, `getattr`) and checks the resolved
  builtin exactly like the direct form (`NBP-BUILTIN-EXEC` / `NBP-OPEN-PATH`), refusing any
  non-literal key and any other use of the frame namespace (`NBP-INDIRECT`); refuses `getattr` /
  `setattr` / `delattr` with a non-literal name, `pkgutil` / `runpy` / `code` / `zipimport` /
  private `_modules`, `sys.settrace` / `meta_path`, `inspect` frames and `gc.get_objects`;
  refuses dunder / frame introspection chains (`__subclasses__`, `__globals__`, `__dict__`,
  `__code__`, `f_globals`, `tb_frame`, ...) under the new `NBP-DUNDER-ESCAPE` (dunder
  definitions in class bodies stay allowed); refuses the IPython shell API (`get_ipython` call
  or name, `InteractiveShell`, `IPython` imports, `%magic` / `!cmd` lines inside a cell body)
  under `NBP-OS-PROCESS` for shell / magic execution and the new `NBP-IPYTHON-ESCAPE`
  otherwise, and logs the magic-led cells `dbutils.notebook.run` skips (`skipped_magic`; no
  line of such a cell is executed); refuses the Spark JVM / py4j bridge (any `_private`
  attribute of `spark` / `sc` / `SparkSession` / `SparkContext`, `_jvm` / `_jsc` / `_gateway` /
  `_jdf` / ... on any object, `py4j`, `JavaGateway`, `registerJavaFunction`, `addPyFile` /
  `addJar`) under `NBP-POLICY-TAMPER` / `NBP-BUILTIN-EXEC`; refuses further process / exec
  routes (`concurrent.futures.ProcessPoolExecutor`, `os.posix_spawn` / `forkpty` / `kill`,
  `cffi`, `signal`, `resource`, `pdb`, `timeit`, `cProfile`, `doctest`, `webbrowser`,
  `types.FunctionType`, `breakpoint()`), code-executing deserialisers (`pickle` / `marshal` /
  `shelve` / `dill` / `joblib` loads, `read_pickle`, `yaml.load` without a safe loader,
  `torch.load` without `weights_only=True`, `numpy.load(allow_pickle=True)`; new
  `NBP-DESERIALIZE`), the stdlib open-equivalents (`io.open`, `os.open`, `codecs` / `gzip` /
  `bz2` / `lzma` / `tarfile` / `zipfile` / `sqlite3` / `linecache`) on paths outside the sandbox
  and `os.chdir` away from it; and follows aliases of all of these across cells (`sp = spark`,
  `f = getattr`, `b = __builtins__`), refusing a name that was rebound to something it cannot
  resolve (fail closed). The test suite adds the reviewer's six probes verbatim plus per-class
  refused and clean cells; every new test fails against the first revision of PR #146 scanner. The gate remains a
  static mitigation layer over a full Python runtime; the platform-level control is the IAM
  scope of the cluster (documented in `SUPPORTED_OPERATIONS.md` section 8 together with the
  known false refusals).
- **Sandbox gate: round-2 gaps closed by generalising the rules** (static review of the
  SEC-AIDP-SAMPLES-005 gate at the round-2 baseline gate). `posix` / `nt`, the platform modules `os` re-exports,
  are tracked as aliases of `os`, so `import posix; posix.system(..)`, `from posix import
  environ`, `import nt as n; n.environ` meet every os rule. The os / shutil / sys.modules /
  importlib / asyncio / aidp_compat attribute rules moved into the one rule table that already
  served `getattr(mod, "literal")`, `from m import n` aliases and imports, so the indirect
  route (`asyncio.run(getattr(asyncio, "create_subprocess_exec")(..))`) is judged exactly like
  the direct one; `loop.subprocess_exec` / `subprocess_shell` added. `str.format` templates are
  resolved like any other target (a literal, a name bound once in this or an earlier cell,
  `"a" + "b"`, `textwrap.dedent(..)`, `.strip()`) and every replacement field is walked
  (`format_map`, `str.format(t, ..)`, `string.Formatter().format / vformat`, nested format-spec
  fields) against the dunder read allowlist, so `t = "{0.__class__.__mro__}"; t.format(obj)`
  and `"{x.__init__.__globals__[os].environ}".format_map(..)` are `NBP-DUNDER-ESCAPE` while
  `"{0.__class__.__name__}".format(obj)` is no longer refused; an unresolvable template
  formatted with a run-time argument fails closed. Local file reads through library readers
  (`pandas` / `polars` / `pyarrow` / `xarray` `read_*`, `numpy.load*`, `spark.read.<fmt>` /
  `load` / `option("path")`, `sc.textFile`, `PIL.Image.open`, `h5py.File`,
  `ElementTree.parse`, ...) with a literal local path (`/etc`, `/dbfs`, `/Workspace`, `/tmp`,
  `~`, `C:\`, `file://`) outside the prefix are `NBP-OPEN-PATH`; scope decision: a reader path
  the gate cannot resolve is allowed (reading source data through variables is the normal
  migration case; the cluster's IAM scope is the control) and recorded in the policy log as
  `unverified_read_path`, and object-storage / catalog reads are never path-checked. Scope
  decision for dynamic SQL (superseded in review round 3, see **the SQL gate is an allowlist**
  below: a `WITH` head is NOT a read, and a dynamic read head is admitted only under the runtime
  SQL guard): a `spark.sql` statement whose literal head (after whitespace and comments) was
  `SELECT` / `WITH` / `SHOW` / `DESCRIBE` / `DESC` / `EXPLAIN` (or a TEMP VIEW) was allowed and
  recorded as `unverified_sql_read`; every other dynamic head (DDL/DML, `USE`, empty, a
  variable) is `NBP-SQL-DYNAMIC`, which also closes `spark.sql(f"{verb} TABLE prod.t")`.
  Constants bound once inside `for` / `if` / `with` / `try` bodies are resolved (the loop-bound
  `q = "SELECT .." + str(cid); spark.sql(q)` false refusal); `getattr(spark, "version")` and
  other literal public names on Spark objects are allowed (dynamic, private and dunder names,
  and the interpreter / compat roots, stay refused); `~`, Windows drive / UNC and `file://`
  paths count as absolute on every host. Every new refused / allowed cell is in the test suite
  and fails against the round-2 baseline gate scanner.
- **Sandbox gate: review-derived gap classes closed as classes** (automated review of the
  round-2 commits plus a probe run against the round-2 baseline gate: 49 of the original 80 probe cells passed
  the gate there). (A) A gated module, interpreter root or Spark object may only be the root of
  an attribute chain, the whole right-hand side of a plain name assignment, the callee of a
  call or an argument of `getattr` / `setattr` / `vars` / `isinstance` / `print`; handed around
  as a value (`[os][0]`, `(os if c else sys)`, `run(os)`, `{'a': os}['a']`, `return os`,
  `lambda: os`, `dict(m=os)`, `for m in (os,)`, `with os as m`, a default, a walrus, `*[os]`)
  it is `NBP-INDIRECT`. (B) A process / environment / introspection sink name (`system`,
  `popen`, `fork`, `execv*`, `spawn*`, `putenv`, `pipepager`, `getattr_static`, `getmembers`,
  the JVM bridge names) on a receiver the gate cannot resolve (a call result, a subscript, an
  unknown name) is refused; `load` / `remove` / `mkdir` / `open` are kept out of that set, and
  `<object>.open(path)` path-checks a literal and records a dynamic path. (C) `RDD.pipe(cmd)` on
  a Spark / RDD-rooted chain or an RDD alias, and `.pipe(<string>)` on any receiver, are
  `NBP-OS-PROCESS`; `DataFrame.pipe(func)` with a lambda / `def` / attribute stays allowed and a
  bare name the gate cannot type is allowed and recorded (`unverified_pipe_callable`). (D)
  `operator.attrgetter` / `methodcaller`, `functools.reduce(getattr, ..)`,
  `inspect.getattr_static` / `getmembers` are judged like `getattr` (non-literal name refused;
  every dotted segment of a literal checked). (E) A chain segment naming a gated module
  re-roots the chain (`shutil.os.system`, `pathlib.os`, `numpy.os`, `pd.io.common.os.environ`,
  `self.spark._jvm`); `vars(<gated module>)` is refused. (F) The pickle family is
  deny-by-default except the dump side; `_`-prefixed attributes of every gated module,
  `Unpickler(..).load()` at any depth, `torch.load` at any depth under `torch` unless
  `weights_only=True` is a literal, `np.load` unless `allow_pickle` is absent or the literal
  `False`, and for yaml a `Loader` is safe only when it resolves through aliases and
  from-imports to exactly `yaml.SafeLoader` / `CSafeLoader` / `BaseLoader` / `CBaseLoader`
  (`from yaml import Loader as SafeLoader` is refused); `yaml.loader.*` / `yaml.constructor.*`,
  `Full*` / `Unsafe*`, `load_all` / `full_load*` / `unsafe_load*`, `add_constructor` /
  `add_multi_constructor` and classes subclassing any yaml loader or constructor are refused.
  (G) `globals()[k]` / `locals()[k]` / `vars()[k]` with a tainted, alias, protected or Spark
  key, or whose result is dereferenced, is refused; only a plain read of a literal
  non-protected key (and `'k' in globals()`) is tolerated. (H) `open(*args)` / `open(**kw)` and
  the same on `io.open` / `codecs.open` / `Path(..).open` are `NBP-OPEN-DYNAMIC`; `open(path_var)`
  stays refused in read mode too (the library-reader scope decision does not extend to
  `open()`), while folded literals (`"/etc/" + "passwd"`, `"/".join([..])`,
  `os.path.join(..)`) and the literal head of an f-string are path-checked. (I) `pydoc` joins
  the process modules. Every probe cell is in the test suite verbatim (first refusal as the
  runner reports it) and the legitimate cells of the same review stay allowed; the new tests
  fail against the round-2 baseline gate scanner.
- **Sandbox gate: coverage round 1 on the review-probe fix closed as classes** (an analyzer coverage test
  found ten gaps and no false refusals; 11 of 119 probe cells passed the gate at the review-probe fix). (J)
  A list literal bound to a name is folded into a path (`"/".join(parts)`) only while the name
  is used for nothing but the join: an in-place mutation (`p[0] = '/etc'`, `.insert` /
  `.append` / `.extend` / `.pop`, `del p[0]`, an alias `q = p`, a helper call `mutate(p)`,
  `*p`) in this or an earlier cell makes the join dynamic and the sink fails closed
  (`NBP-OPEN-DYNAMIC`, `NBP-PATH-WRITE-DYNAMIC`, `NBP-DBFS-DYNAMIC`, `NBP-FS-DYNAMIC`). (K)
  Keyword-gated loaders fail closed when their switch is positional or hidden: `np.load(p,
  None, True)`, `np.load(p, **kw)` / `*args`, `torch.load(.., pickle_module=..)` also with
  `weights_only=True`, `torch.load(p, None, custom)`, `torch.load(p, weights_only=True, **kw)`,
  `torch.serialization.add_safe_globals` / `safe_globals` / `clear_safe_globals`, and
  `.load(p, **kw)` / three positionals on a receiver the gate cannot resolve (`np.load` with
  the import out of view, `loader.load`); Spark reader chains and resolved modules
  (`json.load(fh, **kw)`) are untouched. (L) The state of a gated module is read-only: item or
  attribute assignment, `del`, `setattr`, `getattr(..)[k] =` and the registry methods `update`
  / `setdefault` / `pop` / `popitem` / `clear` / `append` / `insert` / `extend` on a chain
  rooted at a gated module (`yaml.SafeLoader.yaml_constructors['!x'] = h`, also through an
  alias `L = yaml.SafeLoader`, `yaml.SafeLoader.yaml_constructors.update(..)`,
  `yaml.safe_load = yaml.unsafe_load`, `operator.attrgetter = x`) are `NBP-DESERIALIZE` for
  deserialiser modules and `NBP-POLICY-TAMPER` otherwise; `sys.path.insert` / `append` (the
  migrator's own module mirror) and `torch` flag attributes outside `torch.serialization`
  (`torch.backends.cudnn.benchmark = True`) stay allowed. (M) A plain `import mymod` no longer
  vouches for the module's attributes: a process sink name on it (`mymod.system(..)`,
  `mymod.popen(..)`, `helpers.fork()`) is `NBP-INDIRECT` like `obj.popen(..)`, with
  `platform.system()` the documented stdlib homonym. (N) `_private` attributes of ANY imported
  module (`mymod._run`, `np._core`, `from mymod import _helper`) are `NBP-INDIRECT`; a
  `_private` segment makes a receiver opaque for the sink rule (`pd._libs.system`), and the
  re-rooting rule keeps judging `tempfile._os` / `mymod._subprocess` / `glob.os` by the module
  they name. (O) `site` (`addsitedir` executes `.pth` files), `pip`, `ensurepip`,
  `sitecustomize` are code-load modules refused at import and as `site.addsitedir(..)` /
  `pip.main(..)` chains (`NBP-BUILTIN-EXEC`, the `addPyFile` class; `sys.path.insert` is
  deliberately not in it). (P) `<object>.open(path_var)` and `<object>.pipe(name)` are allowed
  and recorded only when the receiver is a name the notebook bound (an import, an assignment,
  a parameter, a loop / with target, in this or an earlier cell); a receiver the notebook
  never bound (`thing.open(p)`, `cfg.fs.open(name)`, `thing.pipe(cmd)`) is refused
  (`NBP-OPEN-DYNAMIC` / `NBP-OS-PROCESS`): the gate cannot tell it from the builtin or an RDD.
  `open()` and its stdlib / pathlib twins with a dynamic path stay refused in every mode (no
  read allowance). The ten gap cells are in the test suite verbatim with per-class refused and
  allowed cells; the new tests fail against the review-probe fix scanner, and the 146-cell / 31-cell
  probe files are green.
- **Sandbox gate: coverage round 2 on the coverage-round-1 fix closed as classes** (an analyzer coverage test
  found ten gaps and no false refusals). (Q) Local-filesystem writes through library writers,
  the mirror of the reader table: `np.save` / `savez` / `savetxt`, `torch.save` /
  `torch.jit.save`, `joblib.dump`, `scipy.io.savemat`, `cv2.imwrite`, `plt.savefig`,
  `imageio.imwrite`, `pyarrow.parquet.write_table`, `pd.ExcelWriter` / `HDFStore`,
  `plotly.io.write_*`, `mlflow.<flavor>.save_model` and the rest of the table, also via
  `from numpy import save` / `import matplotlib.pyplot as plt` / with the import out of view.
  (R) Receiver-typed frame writers on any receiver: `df.to_csv` / `to_parquet` / `to_pickle` /
  `to_json` / `to_hdf` / `to_excel` / ..., polars `write_*`, xarray `to_netcdf`, numpy
  `tofile`, `savefig`, plotly `write_html` / `write_image` (`pd.DataFrame(x).to_csv(..)`
  included). For both: a literal local path outside the prefix (`/etc`, `~`, `file://`, an
  `oci://` path outside it, a name bound once, a literal head `'/dbfs/mnt/prod/' + name`) is
  `NBP-PATH-WRITE`; a path the gate cannot resolve is `NBP-PATH-WRITE-DYNAMIC` like
  `df.write.parquet(path_var)` (writes fail closed at every sink; the read-side
  `unverified_read_path` allowance does not extend to writers), except an in-memory buffer /
  writer handle the notebook bound (`buf = io.StringIO()`, `BytesIO()`, `with
  pd.ExcelWriter(p) as w`) or `sys.stdout`; relative paths and the no-path forms
  (`df.to_csv()`) stay allowed. (S) Generic `.save` / `.save_model` / `.save_pretrained` /
  `.save_weights` / `.export` on a receiver the gate cannot type (`wb.save('/etc/x.xlsx')`,
  `model.save`, `img.save`, `doc.save`) judge a literal path the same way and leave a dynamic
  one alone. (T) `tempfile` creators with a literal `dir=` (keyword or positional slot) or an
  absolute `prefix=` outside the prefix, a literal head outside it, and `tempfile.tempdir =
  '/etc'` are `NBP-FS-PATH` (`tempdir = d` is `NBP-FS-DYNAMIC`); the default directory, a
  `dir=` the gate cannot resolve and a relative `dir` stay allowed. (U) Live frame dumps are
  introspection routes (`NBP-INDIRECT`): `faulthandler` at import and as a chain,
  `traceback.walk_stack` / `walk_tb`, `capture_locals=<not the literal False>` on any
  receiver and `.get_stack()` on a receiver the gate cannot resolve; `traceback.print_exc` /
  `format_exc` / `extract_stack` stay allowed. The ten gap cells are in the test suite
  verbatim with per-class refused and allowed cells; the new tests fail against the coverage-round-1 fix
  scanner, and the probe files (now 183 refused / 40 allowed cells) are green.
- **Sandbox gate: coverage round 3 on the coverage-round-2 fix closed as classes** (an analyzer coverage test
  found three gaps and no false refusals). (V) A re-export back to the chain's own root:
  `os.path.os.system` / `os.path.genericpath.os.environ`, also through `import os.path as op`
  (`op.os.system`, `op.os.environ`, `op.os.popen`), `from os import path` (`path.os.system`) and
  `from os.path import os`; the re-rooting rule no longer skips a segment that names the root
  module (`shutil.os`, `glob.os`, `posixpath.os`, `platform.os`, `tarfile.os` and the private
  `tempfile._os` / `random._os` were already judged). (W) A `DataFrameWriter` / `DataFrameReader`
  held in a variable: a name bound to a Spark IO builder chain (`wr = df.write.mode('overwrite')`,
  `.option(..)` / `.format(..)` / `.partitionBy(..)`, `wr = df.coalesce(1).write`,
  `rd = spark.read.format('csv')`, `wr = df.writeTo(t).using(..)`) or to such a name (`w2 = wr`)
  is followed as the chain, in the same cell and across cells, so `wr.parquet('/etc/x')` /
  `wr.csv` / `wr.save` / `wr.option('path', ..)` are `NBP-PATH-WRITE`, `wr.parquet(dest)` is
  `NBP-PATH-WRITE-DYNAMIC`, `wr.saveAsTable('otherc.s.t')` is `NBP-TABLE-TARGET` and
  `rd.load('/etc/passwd')` is `NBP-OPEN-PATH`; `writer.format(fmt_var).save(..)` on such a name is
  no longer a false refusal. Structured-streaming writers are writers: `df.writeStream...start(p)`,
  `.toTable(t)` and `.option('path' | 'checkpointLocation', p)` / `.options(..)` are judged like
  `df.write` (`.start()` without a path and `thread.start()` are untouched). (X) Servers bind a
  listening socket: `http.server`, `wsgiref.simple_server`, `smtpd` and the common Python web
  servers / app frameworks (`flask`, `fastapi`, `starlette`, `uvicorn`, `gunicorn`, `hypercorn`,
  `tornado`, `werkzeug`, `bottle`, `cherrypy`, `waitress`, `twisted`, `sanic`, `gradio`,
  `streamlit`, `dash`, `zmq`) are `NBP-IMPORT-NETWORK` like `socketserver`; the dotted network
  modules are judged as a class at every door: `import http.server`, `from http.server import
  HTTPServer`, `from http import server` / `client`, `import wsgiref.simple_server as w`, and the
  chain under a harmless parent import (`import http; http.client.HTTPSConnection(..)` /
  `http.server.HTTPServer(..)`, `import wsgiref; wsgiref.simple_server.make_server(..)`);
  `import http` / `http.HTTPStatus` / `wsgiref.util` stay allowed and
  `AIDP_SANDBOX_ALLOW_NETWORK=1` lifts all of it. The three gap cells are in the test suite
  verbatim with per-class refused and allowed cells; the new tests fail against the coverage-round-2 fix
  scanner, and the probe files (now 225 refused / 62 allowed cells) are green.
- **Sandbox gate: `Path.open()` / `.rename()` / `.replace()` on a derived Path receiver was
  fail-open** (regression review on the coverage-round-3 fix; present since the first revision of PR #146). `_check_pathlib` only
  recognised a Path receiver on the bare constructor (`Path(x).open()`) or a name bound once to
  one, so `Path(p).resolve().open('r')`, `q = Path(p).resolve(); q.open('r')`,
  `Path(p).parent.open('r')`, `Path('/etc/passwd').resolve() / .absolute() / .expanduser()
  .open()`, `(Path('/etc') / 'passwd').open('r')`, `Path('/etc').joinpath('passwd').open('r')`,
  `Path(..).with_name('shadow').open()`, `Path('/etc/cron.d/job').absolute().open('w')`,
  `Path('/etc/passwd').absolute().rename('/tmp/x')`, `Path('/tmp/x').absolute()
  .replace('/etc/passwd')` and `Path(p).resolve().rename(t)` fell through to the generic
  `<object>.open(path)` rule, which read the MODE string as the path and allowed the call
  (`.read_text()` / `.write_text()` on the same chains were already refused). Every receiver
  rooted at a Path is now a Path: the identity steps (`.resolve()` / `.absolute()` /
  `.expanduser()` on an absolute literal) keep the constructor literal and are judged on it
  (`NBP-OPEN-PATH` / `NBP-FS-PATH`); a derived Path (`.parent` / `.parents[i]` / `.joinpath()` /
  `.with_name()` / `.with_suffix()` / `Path(..) / x`), `Path()` / `Path.cwd()` / `Path.home()`,
  `.expanduser()` on a `~` literal, a `Path`-annotated parameter, an `iterdir` / `glob` loop or
  comprehension target and a name bound to any of these (bound once, rebound, or in an earlier
  cell through `ScanContext.paths`) is an unresolvable Path and fails closed (`NBP-OPEN-DYNAMIC`
  / `NBP-FS-DYNAMIC`) in every mode. Only a receiver that is not a Path at all (`str.replace`,
  `df.rename`, `zf.open(member)`, `fs.open(p)`) reaches the generic rule. The thirteen review
  cells are in the test suite verbatim with the rest of the class (REVIEW_GAP_CELLS_6, a
  cross-cell test, CLEAN_CELLS_9 for the adjacent everyday code); 40 of the new cases fail
  against the coverage-round-3 fix scanner, and the probe files (now 253 refused / 62 allowed cells) are green.
- Python 3.9 floor: `cluster_lifecycle.py` and `fuse_package_analyzer.py` used PEP 604
  `X | None` annotations evaluated at import time; `from __future__ import annotations` added.
- **Sandbox gate: the SQL gate is an allowlist** (review round 3 of SEC-AIDP-SAMPLES-005). The
  reviewer's point: the `spark.sql` handling was a blocklist of write verbs that assumed a
  statement starting with `WITH` is a read, so `SELECT reflect('java.lang.Runtime', ..)` and
  `java_method(..)` (the SQL route to the JVM the gate blocks in Python), `SELECT * FROM
  text.\`/etc/passwd\`` (path tables read any local file), `ADD JAR` and `WITH x AS (..) INSERT
  INTO prod.table ..` all passed. One classifier, `notebook_policy.classify_sql`, now serves the
  static gate and a new runtime guard: the statement is masked (comments, string literals and
  backtick identifiers neutralised), a text with more than one statement is refused, the
  TOP-LEVEL verb is resolved behind the `WITH` CTE list (parenthesis depth) and `EXPLAIN`, and
  only known read forms (`SELECT` / `VALUES` / `TABLE` / `WITH .. SELECT` / `SHOW` / `DESCRIBE` /
  `EXPLAIN` / `CACHE` / `UNCACHE` / `CLEAR CACHE` / `REFRESH TABLE` / `CREATE TEMP VIEW` without
  `LOCATION` / `USE` of the sandbox / `SET TIME ZONE` / `SET` of a short conf allowlist) or known
  write forms with a target inside `catalog.schema` (`CREATE` / `REPLACE` / `INSERT` / `MERGE` /
  `UPDATE` / `DELETE` / `DROP` / `ALTER` / `TRUNCATE` / `OPTIMIZE` / `VACUUM` / `RESTORE` / `MSCK
  REPAIR` / `ANALYZE` / `COPY INTO` / `CLONE`; `LOCATION` / `OPTIONS (path)` under the prefix) are
  accepted. Everything else is the new `NBP-SQL-REFUSED` with a precise detail: `reflect(` /
  `java_method(` and `TRANSFORM .. USING '<cmd>'` anywhere in a statement (CTEs, subqueries,
  expressions, case-insensitive), `ADD` / `LIST` `JAR` / `FILE` / `ARCHIVE`, `CREATE` / `DROP
  FUNCTION`, `GRANT` / `REVOKE`, `CALL`, `DECLARE` / `EXECUTE IMMEDIATE` / `BEGIN .. END`,
  `INSERT OVERWRITE DIRECTORY`, `LOAD DATA INPATH`, `SET` / `RESET` outside the allowlist (and any
  conf matching `credential` / `secret` / `key` / `token` / `password` / `hadoop` / `fs.` /
  `spark.databricks` / `spark.driver` / `spark.executor` / `extraJavaOptions` / `extraClassPath`
  / `spark.sql.extensions`), `FROM .. INSERT` and every unknown verb; `USE` outside the sandbox and
  write targets outside it stay `NBP-SQL-TARGET`. Path tables (`<format>.\`<path>\``), `COPY INTO
  .. FROM '<path>'` and `OPTIONS (path)` on a read naming a local path outside the prefix are
  `NBP-OPEN-PATH` (object-storage URIs stay allowed for reads; writes must sit under the prefix).
  Dynamic statements fail closed (`NBP-SQL-DYNAMIC`) unless the runtime SQL guard is active for
  the run: `dbutils.notebook.run` now wraps, for the duration of the run only, `spark.sql` /
  `sqlContext.sql` on every session object in the executing namespace, the `pyspark`
  `SparkSession` / `SQLContext` classes, `DataFrame.selectExpr` / `filter` / `where` and
  `functions.expr`, re-classifies the FINAL string before Spark executes it and records a
  `runtime_refused` entry; with the guard active, a dynamic statement whose literal head verb is
  a read verb and whose literal fragments carry no refused token is allowed and recorded as
  `unverified_sql_read`. `spark.newSession()`, `SparkSession.builder..getOrCreate()` and
  `getActiveSession()` are refused statically (`NBP-POLICY-TAMPER`) because a fresh session would
  not carry the guard. `%sql` cells reach the same classifier through `sql_magic`. The old "a
  `WITH` head is a read" assumption is gone from code, tests and docs. Every NEW refusal fails
  against the round-2 tree; the forms the round-2 blocklist already refused (its write verbs on an
  out-of-sandbox target, comment-led writes, dynamic statements with a non-read literal head) are
  kept in a separately labelled list as regression guards and claim nothing about this round.
- **Sandbox gate: SQL allowlist, adversarial round 2** (SEC-AIDP-SAMPLES-005). The round-1
  allowlist still let through: a backtick-quoted function name (`` SELECT `reflect`('java.lang.Runtime', ..)
  ``: the masker hid it), a quoted or class-named provider in a path table (`` `text`.`/outside/f` ``,
  `` org.apache.spark.sql.execution.datasources.text.TextFileFormat.`/outside/f` ``: the rule was a list
  of unquoted format names), a raw string literal (`r'\'` has no escape processing, so the rest of the
  text was live SQL hidden inside what the masker took for one literal), `spark.sql` keyword arguments
  and `IDENTIFIER(:t)` (pyspark renders `{name}` kwargs and binds `args=` after the guard ran), `ALTER
  TABLE sbx.t RENAME TO prod.t` (the destination was never judged), `functions.reflect` / `java_method`
  / `call_function("reflect", ..)` (a Java call with no SQL string), `spark.table("text.`/outside/f`")`,
  `spark.catalog.setCurrentCatalog("prod")` + a 2-part `INSERT`, `spark.conf.set("spark.sql.extensions",
  ..)`, `spark.catalog.createTable(.., path="/outside/f")`, `sc.addFile`, `USING jdbc` / `kafka` /
  `socket` and `spark.read.format("jdbc")` with network access off, `spark.sql(**kw)` (static
  fail-open), `DESCRIBE DETAIL '/outside/f'` and the `dbfs:` / `hdfs:` / `local:` schemes; it falsely
  refused `TRIM(LEADING '/' FROM '/tmp/x')` and `(SELECT 1) UNION ALL (SELECT 2)`. Closed as classes:
  the masker lexes `r'..'` / `R".."` raw literals without escape processing and the token scan resolves
  backtick-quoted function names (`try_reflect` added); the path-table rule is structural (any provider
  in front of a backtick identifier that is a path, in statements, write targets and run-time
  fragments); `IDENTIFIER(` is `NBP-SQL-DYNAMIC`; a literal `{name}` template with literal keyword
  values is rendered as pyspark renders it and classified, non-literal values make it a dynamic
  statement, `*args` / `**kwargs` are `NBP-SQL-DYNAMIC`, and the runtime guard renders the final
  values before classifying; `ALTER .. RENAME TO <name>` judges the destination; `COPY INTO .. FROM
  '<path>'` is anchored on the `COPY` verb and a parenthesised query term is judged by the verb inside;
  `DESCRIBE DETAIL` / `HISTORY '<path>'` are path reads and `dbfs:` / `hdfs:` / `local:` are local
  schemes everywhere a local path is judged; `CREATE .. USING <provider>`, `spark.read` / `write.format`
  / `using` / `jdbc` refuse network providers and dotted provider classes while
  `AIDP_SANDBOX_ALLOW_NETWORK` is off (`NBP-IMPORT-NETWORK`; a provider the gate cannot resolve is
  recorded as `unverified_format`); `pyspark.sql.functions.reflect` / `java_method` / `try_reflect` and
  `call_function` by name are refused statically (import, attribute, call) and wrapped by the runtime
  guard, as are `spark.table` / `DataFrameReader.table`; `spark.table` / `read.table` literals,
  `spark.catalog.setCurrentCatalog` / `setCurrentDatabase` (the `USE` rule), `spark.conf.set` / `unset`
  / `setConf` (the `SET` allowlist), `spark.catalog.createTable` / `createExternalTable` (name, `path=`,
  `source=`) and `sc.addFile` are gated like their SQL forms. Tests: 99 refused cells, 61 allowed cells,
  10 runtime-guard cases, a stub-pyspark test for the function / reader patches and six `%sql` cells;
  96 of the refused cells pass the round-1 gate and 8 of the runtime cases reach the fake session there.
- **Sandbox gate: SQL allowlist, regression review** (SEC-AIDP-SAMPLES-005). The test record no
  longer overstates its proof of failure: the eleven refused cells the round-2 blocklist already
  refused with the same verdict live in a separately labelled regression-guard list, the two
  runtime cases that fail closed statically are marked as such, and the two entries above read
  accordingly. Three collapsed line continuations and a lost space in `notebook_policy.py`
  (cosmetic: the parser accepted both) are restored, and a source-hygiene test keeps both shapes
  out of the gate modules.
- **Secrets shim is Vault-only by default** (`engine/aidp_compat/secrets.py`,
  SEC-AIDP-SAMPLES-006). `AIDP_SECRET_*` environment scanning and the JSON file fallback
  (now only via an explicit `AIDP_SECRETS_FILE`) happen only with
  `AIDP_ALLOW_PLAINTEXT_SECRETS=1`; the file must be owner-only (`0600`) on POSIX (skipped
  with a note on Windows); plaintext mode logs one `insecure plaintext secrets mode active`
  line without values; `AIDP_SECRET_SCOPES` restricts `get` / `list` / `listScopes` to
  allowlisted scopes and list operations never reveal values. The allowlist is created during
  planning: `job_migrate.py` collects the `dbutils.secrets.get(scope, key)` literals of each
  task and declares `AIDP_SECRET_SCOPES` / `AIDP_SECRET_KEYS` (`scope/key`, `scope/*`) in the
  cluster bootstrap (`none` before the first task; an operator-provided value wins); the shim
  reads both per call and logs refusals as `NBP-RUNTIME-SECRET`.
- **`get_tool_output` is confined to the tool output directory**
  (`engine/scripts/context_compactor.py`, `agent_migrate.py`, SEC-AIDP-SAMPLES-DBX-NEW-01).
  The model-callable tool passed its `filename` argument straight to `os.path.join` + `open`,
  so an indirect prompt injection in customer notebook content could read any file the
  consultant's account can read (`../../../oci_api_key.pem`, absolute paths) into model
  context. `ContextCompactor.get_saved_output` now accepts only the bare
  `tool_<NNN>_<tool>.txt` names it generates itself, additionally resolves the path with
  `os.path.realpath` and requires it to be a direct child of the compactor directory (so a
  planted symlink cannot escape either), logs refusals and returns a
  `[context_compactor] Refused` string; `_handle_get_tool_output` validates before consulting
  any compactor and stops walking the compactor history on a refusal. "File not found" and
  read errors no longer echo the host path or non-output files. The name allow-list is
  anchored with `\A`/`\Z`, applied with `fullmatch` and restricted to ASCII digits, so a
  newline-terminated name or one spelt with non-ASCII decimal digits is refused before any
  filesystem access instead of reaching `open()`.
- **The Databricks PAT no longer travels on the command line**
  (`engine/scripts/extract_catalog_databricks.py`, SEC-NEW-DATABRICKS-03). `--token <value>`
  is refused with exit code 2 and the message "Do not pass tokens in argv. Use
  DATABRICKS_TOKEN or --token-file." -- the flag is kept only so an old command fails loudly
  instead of being re-parsed, and the value is never stored or echoed. `--token-file <path>`
  reads the PAT from a file only its owner can read (mode & 0o077 must be 0 on POSIX; on
  Windows, where `st_mode` carries no such bits, the check is skipped with a note).
  `DATABRICKS_TOKEN` keeps working and the file wins when both are present. A `--host` /
  `DATABRICKS_HOST` of the form `https://user:token@workspace` is rejected. Output names the
  token's *source*, never the token, and the token is redacted (`***`) from error text the
  script prints or stores in the catalog pack. Mistyped forms are not echoed either:
  abbreviated long options are off and unknown arguments are reported by count, so
  `--tok=<value>`, `-t <value>` or a bare value exit 2 with the remediation text instead of
  argparse's own "ambiguous option: ..." / "unrecognized arguments: ..." diagnostics, which
  repeat the value; every parser error message is also scrubbed of the argv values as defence
  in depth. `extract_catalog_databricks.py` is the only entry point that talks to Databricks;
  the other `engine/scripts/*.py` entry points take no credential on argv (they talk to AIDP
  with the OCI profile, or read the model API key, `OPENAI_API_KEY`, from the environment).
- Added `engine/tests/` (pytest) covering these controls; `pytest.ini` scopes collection to
  them.

## [0.2.0] - 2026-06-24

- Bundled the OpenAI-based Python migration engine under `engine/`.
- Added a Codex SessionStart hook that stages the engine to `~/.aidp-migrator/engine`.
- Updated skills and references to invoke `~/.aidp-migrator/engine/scripts/...` instead of requiring a separate migrator clone.
- Replaced legacy provider-specific setup language with `OPENAI_API_KEY` / `OPENAI_MODEL` guidance.
- Kept customer-specific coordinates and secrets out of the plugin; users provide them through local env vars and gitignored env-coords files.

## [0.1.0] — 2026-06-23 (initial release)

First public release of the **Codex CLI** plugin for the Oracle AIDP Databricks Migration Toolkit. Codex has no separate "commands" or "agents" abstraction at the plugin layer - both are folded into the `skills/` directory.

### Skills (16)

**Core toolkit (10) — identical to the source toolkit guidance:**

- `aidp-migrator-overview` — router / lay of the toolkit
- `aidp-migrator-bootstrap` — environment readiness check (Python deps, OCI auth, cluster state, env-coords)
- `aidp-build-dag` — build migration manifest from a Databricks workspace path
- `aidp-check-data` — pre-migration data-availability scan
- `aidp-migrate-job` — Pass-1 deps + Pass-2 cell-by-cell execute/verify/fix on a live AIDP cluster
- `aidp-fixup-cell` — targeted rewind: re-execute cells from a history index
- `aidp-resume-migration` — resume an interrupted run
- `aidp-migrate-catalog` — Unity Catalog / HMS DDL → 18-rule rewriter → batched replay
- `aidp-bucket-mapping` — `s3://` → `oci://` bucket/namespace mapping config
- `aidp-acceptance-contract` — consecutive-zero-window convergence for batch / streaming

**Guided flows (4) — translated from the guided command flows:**

- `migrate-job-flow` — guided full-job migration with phase checkpoints
- `migrate-catalog-flow` — guided catalog migration with dry-run preview
- `check-data-flow` — pre-migration data-availability scan wrapper
- `migration-status` — parse + summarize a `JOB_REPORT.md`

**Specialist reviewers (2) — translated from the specialist reviewers:**

- `databricks-notebook-analyzer` — pre-migration single-notebook readiness report
- `migration-reviewer` — post-Pass-2 migrated-notebook correctness review (catches cell-execute drift that PASS verdicts miss)

### References (5)

- DDL rewrite rules (18 rules with examples)
- 15 Databricks → AIDP gotchas + fix recipes
- Env-coords scaffold
- `JOB_REPORT.md` parsing format
- CLI map (every migrator entrypoint → purpose + canonical invocation)
