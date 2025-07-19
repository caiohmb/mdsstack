from prefect import flow
import subprocess

@flow(name="ingestao-dlt-clima")
def run_dlt_ingestion():
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