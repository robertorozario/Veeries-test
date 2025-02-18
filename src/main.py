import sys
import time
from scraping import WebScraper
from transformation import TransformateData
from enrichment import DataEnrichment

def task():
    print("A tarefa foi executada após 24 horas!")

def run_etl(arg):
    """
    Função principal que executa o pipeline ETL com base no argumento.
    Argumento:
        arg: Caso seja '1', executa as etapas de ETL.
    """
    print("Iniciando o processo ETL...")
    print("Iniciando a coleta de dados...")
    if arg == "1" or arg == "":
        while True:
            print("Coletando dados dos Portos...")
            WebScraper.run()
            print("Coleta dos Portos concluída.")

            print("Iniciando a transformação dos dados...")
            TransformateData.run()
            print("Transformação dos dados concluída.")

            print("Gerando relatório de movimentação de carga...")
            DataEnrichment.run()
            print("Relatório gerado com sucesso.")
            if arg == '1':
                break
            else:
                time.sleep(24*60*60)
            

if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else ""
    run_etl(arg)
