# Executing pipeline

Ensure you have followed the setup steps in the [README](../README.md) before proceeding.

Once you have done that, you can execute the pipeline from the `pipeline` folder as:

```bash
poetry run ploomber build
```

To add more components to the pipeline, edit the `pipeline.yaml` file and run the above command again.

You can easily Dockerize this pipeline by adding a `Dockerfile` and a `docker-compose.yaml` file. See the [Docker guide with JupySQL and Ploomber](https://ploomber-sql.readthedocs.io/en/latest/packaging-your-sql-project/etl-eda-pipeline-with-ploomber.html) for more details.