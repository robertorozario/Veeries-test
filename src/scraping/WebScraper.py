from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from io import StringIO
from pathlib import Path
import pandas as pd

PORTS = [
    {
        "url": "https://www.portodesantos.com.br/informacoes-operacionais/operacoes-portuarias/navegacao-e-movimento-de-navios/navios-esperados-carga/",
        "name": "PORTO_DE_SANTOS",
    },
    {
        "url": "https://www.appaweb.appa.pr.gov.br/appaweb/pesquisa.aspx?WCI=relLineUpRetroativo",
        "name": "PORTO_DE_PARANAGUA",
    }
]

def run():
    scraper = WebScraper()
    for port in PORTS:
        x = scraper.fetch_tables(port["url"], port["name"])
    scraper.close()

class WebScraper:
    """ Classe responsável por extrair tabelas de portos e salvar os dados. """
    def __init__(self):
        self.browser = webdriver.Chrome()

    def fetch_tables(self, url: str, folder_name: str):
        self.browser.get(url)
        elements = WebDriverWait(self.browser, 10).until(
            EC.presence_of_all_elements_located((By.TAG_NAME, "table"))
        )

        for i, item in enumerate(elements):
            if folder_name == "PORTO_DE_PARANAGUA":
                item = elements[i + 1] if i + 1 < len(elements) else elements[i]
            table_html = item.get_attribute('outerHTML')
            table_data = pd.read_html(StringIO(table_html))[0]
            table_name = (table_data.columns[0][0]).replace("/","=")
            if table_name == 'LEGENDA':
                continue
            self.save_table(table_data, folder_name, table_name)
            

    def save_table(self, data: pd.DataFrame, folder: str, filename: str):
        path = Path(f"data/bronze/{folder}/{filename}.parquet")
        path.parent.mkdir(parents=True, exist_ok=True)
        data.to_parquet(path, index=False)
        print(f"Arquivo salvo: {path}")

    def close(self):
        """Fecha o navegador."""
        self.browser.quit()
