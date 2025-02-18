from WebScraper import WebScraper

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

if __name__ == "__main__":
    scraper = WebScraper()
    for port in PORTS:
        x = scraper.fetch_tables(port["url"], port["name"])
    
    scraper.close()
