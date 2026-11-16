@echo off
rem ---------------------------------------------------------------------------
rem One script for the four things you do all day in this project.
rem   run.bat            compile, then run the pipeline on the configured dataset
rem   run.bat compile    compile only
rem   run.bat smoke      compile, then run the toolchain check (JDK + Flink + DuckDB)
rem   run.bat test       compile and run the unit and integration tests
rem   run.bat clean      delete backend\target
rem
rem It delegates to Maven on purpose. The compile classpath here is 108 jars
rem (278 MB), longer than the 1023 characters cmd.exe will hold in a variable, so a
rem hand-written javac command line cannot carry it. Maven reads the same classpath
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
if "%~1"=="smoke" goto :smoke
if "%~1"=="test" goto :test
if "%~1"=="clean" goto :clean
goto :job

:compile
call "%MVNCMD%" compile
goto :eof

:job
call "%MVNCMD%" compile exec:exec
goto :eof

:smoke
call "%MVNCMD%" compile exec:exec "-DmainClass=com.fyp.ToolchainSmokeCheck"
goto :eof

:test
call "%MVNCMD%" test
goto :eof

:clean
call "%MVNCMD%" clean
goto :eof
