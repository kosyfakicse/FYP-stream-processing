# The pipeline is Java and the real entry point is backend\run.bat, which locates the JDK and the
# Maven wrapper by itself. This file only forwards, so the same words work from the repository root.
#
# Read this before trusting it: `make` is NOT installed on the machine this project was developed on
# (checked 2026-11-16: `make --version` is not found in Git Bash or on PATH), so nothing here has been
# executed. It is Windows-only -- the recipes assume cmd.exe and call .bat files. If you are on
# macOS or Linux, or have no make at all, run backend\run.bat directly; it takes the same one word.

.PHONY: help compile serve job test clean reset client kafka-up kafka-down

help:
	@echo compile     compile only - backend\run.bat compile
	@echo serve       start the HTTP API - backend\run.bat serve
	@echo job         run the pipeline on the configured dataset - backend\run.bat
	@echo test        the Java test suite - backend\run.bat test
	@echo clean       delete backend\target - backend\run.bat clean
	@echo reset       archive the dev database and uploads - backend\run.bat reset
	@echo client      exercise the API from Python - python -m frontend.test
	@echo kafka-up    start the Kafka stack - docker compose -f docker\docker-compose.yml
	@echo kafka-down  stop it again

compile:
	cd backend && run.bat compile

serve:
	cd backend && run.bat serve

job:
	cd backend && run.bat

test:
	cd backend && run.bat test

clean:
	cd backend && run.bat clean

reset:
	cd backend && run.bat reset

client:
	.venv\Scripts\python.exe -m frontend.test

kafka-up:
	docker compose -f docker\docker-compose.yml up -d

kafka-down:
	docker compose -f docker\docker-compose.yml down
