@echo off
rem ---------------------------------------------------------------------------
rem One script for the things you do all day in this project.
rem   run.bat            compile, then run the pipeline on the configured dataset
rem   run.bat compile    compile only
rem   run.bat serve      compile, then start the HTTP API (POST /runs); port from application.properties
rem   run.bat graph ID   print one stored run's four tin_* tables; starts no job and writes no rows
rem   run.bat tins       compile, then run the TIN prototype's main (com.fyp.model.TINs), no database
rem   run.bat test       compile and run the unit and integration tests
rem   run.bat clean      delete backend\target
rem   run.bat reset      delete the development database and every stored upload (stop serve first)
rem
rem It delegates to Maven on purpose. The compile classpath here is 85 jars (171 MB),
rem longer than the 1023 characters cmd.exe will hold in a variable, so a hand-written
rem javac command line cannot carry it. Maven reads the same classpath
rem from pom.xml without routing it through a shell variable, and nothing is written
rem into target/, so `mvn clean` cannot break this script.
rem
rem The exit code is Maven's, so `run.bat test && echo ok` behaves as expected.
rem ---------------------------------------------------------------------------

setlocal enableextensions
cd /d "%~dp0"

set "JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-17.0.21.9-hotspot"
set "PATH=%JAVA_HOME%\bin;%PATH%"

set "MVNCMD=mvn"
for /d %%d in ("%USERPROFILE%\.m2\wrapper\dists\apache-maven-3.9.9\*") do set "MVNCMD=%%~d\bin\mvn.cmd"

echo java:  %JAVA_HOME%
echo maven: %MVNCMD%

if "%~1"=="compile" goto :compile
if "%~1"=="serve" goto :serve
if "%~1"=="graph" goto :graph
if "%~1"=="tins" goto :tins
if "%~1"=="test" goto :test
if "%~1"=="clean" goto :clean
if "%~1"=="reset" goto :reset
goto :job

:compile
call "%MVNCMD%" compile
goto :eof

:serve
rem ApiApplication, not AttributionJob: the same --add-opens flags the job needs for Flink
rem reflection are already what exec:exec passes, so this route is the CLI route with a
rem different main class. Ctrl-C stops it.
call "%MVNCMD%" compile exec:exec "-DmainClass=com.fyp.ApiApplication"
goto :eof

:graph
rem Reads one stored run back out of ../data/attribution.duckdb and prints the four tin_* tables.
rem The id arrives as a Maven property because exec:exec takes no program argument; see pom.xml.
if "%~2"=="" goto :graph_usage
set "PORT=8081"
for /f "usebackq tokens=2 delims==" %%p in (`findstr /b "server.port=" "src\main\resources\application.properties" 2^>nul`) do set "PORT=%%p"
netstat -ano | findstr /c:":%PORT% " | findstr /i /c:"LISTENING" >nul
if not errorlevel 1 goto :graph_busy
call "%MVNCMD%" compile exec:exec "-DmainClass=com.fyp.GraphReport" "-DrunId=%~2"
goto :eof

:graph_usage
echo usage: run.bat graph ^<runId^>
echo        the id is the first word a "run.bat" job printed, or one of GET /runs.
goto :eof

:graph_busy
rem DuckDB allows one process per file and its own error names neither the server nor the lock, so this
rem says it before Maven starts. The port check is the same idiom `reset` uses.
echo A server is listening on port %PORT% and it holds the only open handle on the database.
echo Either Ctrl-C that run.bat serve window and run this again, or read the same rows over HTTP:
echo   curl.exe http://localhost:%PORT%/runs/%~2/graph
goto :eof

:tins
rem The prototype reads no config, no dataset and no database: it is the reference schema's four
rem tables modelled in memory. This route exists so that editing the model has a one-command answer.
call "%MVNCMD%" compile exec:exec "-DmainClass=com.fyp.model.TINs"
goto :eof

:job
call "%MVNCMD%" compile exec:exec
goto :eof

:test
call "%MVNCMD%" test
goto :eof

:clean
call "%MVNCMD%" clean
goto :eof

:reset
rem Wipe the development state: the DuckDB file (so every run, every window output and every dataset
rem row goes with it) and the uploaded csvs those rows named. Nothing in the API does this -- there is
rem no DELETE for runs, and /datasets/{id} deletes one upload, not the history.
rem
rem It moves instead of deleting, into ..\data\cleared-<when>\. Nothing in the project reads that
rem folder: schema.sql recreates the seven tables and the sequence on the next start, and a fresh
rem POST /datasets makes new ids, so no code can reach the archive. Delete the folder yourself when
rem you stop wanting the old results.
rem
rem Refuses while a server is listening. That process holds the only open handle on the database, and
rem clearing the uploads under it would leave a server happily serving dataset ids whose files are
rem gone. The port comes from application.properties, so a server started on another one is still
rem caught. A CLI run.bat job holds the same handle for about two seconds and is not detected; if the
rem move reports a sharing violation, that is why.
set "PORT=8081"
for /f "usebackq tokens=2 delims==" %%p in (`findstr /b "server.port=" "src\main\resources\application.properties" 2^>nul`) do set "PORT=%%p"
netstat -ano | findstr /c:":%PORT% " | findstr /i /c:"LISTENING" >nul
if not errorlevel 1 goto :reset_busy

set "DATA=%~dp0..\data"
set "STAMP="
for /f "usebackq delims=" %%t in (`powershell -NoProfile -Command "Get-Date -Format yyyyMMdd-HHmm" 2^>nul`) do set "STAMP=%%t"
set "ARCHIVE=%DATA%\cleared-%STAMP%"
set "MOVED="
for %%f in ("%DATA%\*.duckdb" "%DATA%\*.duckdb.wal" "%DATA%\uploads\ds-*.csv") do (
  if not defined MOVED md "%ARCHIVE%"
  set "MOVED=1"
  move /y %%f "%ARCHIVE%" >nul
  echo   archived %%~nxf
)
if defined MOVED (
  echo   into "%ARCHIVE%"
  echo   that folder is outside the project's view -- delete it when you no longer want it.
) else (
  echo   nothing to clear: no database and no uploads.
)
goto :eof

:reset_busy
rem The port check is the only thing standing between this route and a half-wipe, so it says plainly
rem that nothing was touched.
echo A server is listening on port %PORT%, and it holds the only open handle on the database.
echo Ctrl-C the run.bat serve window and run this again. Nothing was moved.
exit /b 1
