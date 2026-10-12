# Curate Lambda image: pyarrow + pyiceberg (Glue catalog) + duckdb, plus this package.
# Built and pushed by .github/workflows/deploy.yml; built (not pushed) by CI.
#   docker build -f infra/lambda/curate.Dockerfile -t dx-curate .
FROM public.ecr.aws/lambda/python:3.14-arm64

# Same resolved versions as uv.lock (exported by CI/deploy: uv export --no-dev --no-hashes ...).
COPY requirements-curate.txt /tmp/requirements-curate.txt
RUN pip install --no-cache-dir -r /tmp/requirements-curate.txt

COPY src/dx_productivity ${LAMBDA_TASK_ROOT}/dx_productivity

CMD ["dx_productivity.lambdas.handlers.curate_handler"]
