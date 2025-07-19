from prefect import flow
import subprocess
from prefect.blocks.system import Secret
import os

@flow(name="ingestao-dlt-clima")
def run_dlt_ingestion():
    os.environ["DLT_SECRETS__DESTINATION__POSTGRES__DATABASE"] = Secret.load("pg-database").get()
    os.environ["DLT_SECRETS__DESTINATION__POSTGRES__USERNAME"] = Secret.load("pg-username").get()
    os.environ["DLT_SECRETS__DESTINATION__POSTGRES__PASSWORD"] = Secret.load("pg-password").get()
    os.environ["DLT_SECRETS__DESTINATION__POSTGRES__HOST"] = Secret.load("pg-host").get()
    os.environ["DLT_SECRETS__DESTINATION__POSTGRES__PORT"] = Secret.load("pg-port").get()
    # Executa o script de ingestão do DLT via subprocess
    result = subprocess.run(
        ["python", "dlt/ingestion.py"],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError("Falha na ingestão DLT")

if __name__ == "__main__":
    run_dlt_ingestion()