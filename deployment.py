from prefect import flow

@flow
def run_dlt_ingestion():
    pass

if __name__ == "__main__":
    run_dlt_ingestion.from_source(
        source="https://github.com/caiohmb/mdsstack.git",
        entrypoint="flow.py:run_dlt_ingestion"
    ).deploy(
        name="ingestao-dlt-clima",
        work_pool_name="local-pool",
        schedule={"cron": "0 6 * * *", "timezone": "America/Sao_Paulo"}
    )