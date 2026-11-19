@echo off
rem Windows wrapper for the aidp-fusion-autopilot CLI.
rem
rem Counterpart to the POSIX `bin/aidp-fusion-autopilot`. Runs the package FROM
rem SOURCE via `python -m`; first invocation lazily installs deps (see
rem __main__.py). Interpreter selection is version-gated (>=3.10) because
rem run-from-source bypasses pip's requires-python guard and the code crashes on
rem 3.9 (PEP 604 `X | None` in Pydantic models).
rem
rem The `py -3` launcher lives here (Windows-only), not in the POSIX wrapper.
rem NOTE: not yet validated on a real Windows host (see plan Testing Strategy).
setlocal enableextensions

rem Resolve the plugin root from this script's own location (bin\..).
for %%I in ("%~dp0..") do set "ROOT=%%~fI"

set "PYEXE="
py -3 -c "import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)" >nul 2>&1
if not errorlevel 1 (
    set "PYEXE=py -3"
) else (
    python -c "import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)" >nul 2>&1
    if not errorlevel 1 set "PYEXE=python"
)

if not defined PYEXE (
    echo [aidp-fusion-autopilot] needs Python ^>= 3.10 on PATH ^(none found^). 1>&2
    exit /b 1
)

if defined PYTHONPATH (
    set "PYTHONPATH=%ROOT%\scripts;%PYTHONPATH%"
) else (
    set "PYTHONPATH=%ROOT%\scripts"
)

%PYEXE% -m oracle_ai_data_platform_fusion_autopilot %*
exit /b %errorlevel%
