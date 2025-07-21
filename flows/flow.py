from prefect import flow, get_run_logger

@flow(name="ingestao-dlt-clima")
def run_dlt_ingestion():
    logger = get_run_logger()
    logger.info("==== INICIOU O FLOW DE TESTE ====")
    print("==== INICIOU O FLOW DE TESTE ====")
    logger.info("Prefect está executando o flow de teste com sucesso!")
    print("Prefect está executando o flow de teste com sucesso!")
    return True

if __name__ == "__main__":
    run_dlt_ingestion()