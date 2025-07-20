from prefect import flow, get_run_logger
import subprocess
import os
import sys

@flow(name="ingestao-dlt-clima")
def run_dlt_ingestion():
    logger = get_run_logger()
    
    try:
        logger.info("🔧 Iniciando execução do flow...")
        
        # Verificar se o arquivo existe
        script_path = "dlt/ingestion.py"
        if not os.path.exists(script_path):
            logger.error(f"❌ Arquivo não encontrado: {script_path}")
            logger.info(f"📁 Diretório atual: {os.getcwd()}")
            logger.info(f"📁 Conteúdo do diretório: {os.listdir('.')}")
            raise FileNotFoundError(f"Arquivo {script_path} não encontrado")
        
        logger.info(f"🚀 Executando script: {script_path}")
        
        # Executa o script de ingestão do DLT via subprocess
        result = subprocess.run(
            [sys.executable, script_path],  # Usa o Python correto
            capture_output=True,
            text=True,
            cwd=os.getcwd()  # Garante que está no diretório correto
        )
        
        # Log da saída
        if result.stdout:
            logger.info("📤 STDOUT:")
            logger.info(result.stdout)
        
        if result.stderr:
            logger.warning("⚠️  STDERR:")
            logger.warning(result.stderr)
        
        if result.returncode != 0:
            logger.error(f"❌ Script falhou com código de saída: {result.returncode}")
            raise RuntimeError(f"Falha na ingestão DLT - Código: {result.returncode}")
        
        logger.info("✅ Ingestão DLT concluída com sucesso!")
        return result.returncode
        
    except Exception as e:
        logger.error(f"❌ Erro crítico no flow: {str(e)}")
        raise

if __name__ == "__main__":
    run_dlt_ingestion()