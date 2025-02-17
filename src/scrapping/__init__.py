from WebScraper import WebScraper

PORTS = [
    {
        "url": "https://www.portodesantos.com.br/informacoes-operacionais/operacoes-portuarias/navegacao-e-movimento-de-navios/navios-esperados-carga/",
        "name": "PORTO_DE_SANTOS",
        "tables": ['LIQUIDO A GRANEL', 'TRIGO', 'GRANEIS DE ORIGEM VEGETAL', 'GRANEIS SOLIDOS - IMPORTACAO',
                   'GRANEIS SOLIDOS - EXPORTACAO', 'ROLL-IN-ROLL-OFF', 'LASH', 'CABOTAGEM', 'CONTEINERES',
                   'PRIORIDADE C3', 'PRIORIDADE C5', 'SEM PRIORIDADE']
    },
    {
        "url": "https://www.appaweb.appa.pr.gov.br/appaweb/pesquisa.aspx?WCI=relLineUpRetroativo",
        "name": "PORTO_DE_PARANAGUA",
        "tables": ['ATRACADOS', 'PROGRAMADOS', 'AO LARGO PARA REATRACAÇÃO', 'AO LARGO', 'ESPERADOS', 'APOIO PORTUÁRIO / OUTROS', 'DESPACHADOS']
    }
]

if __name__ == "__main__":
    scraper = WebScraper()
    for port in PORTS:
        x = scraper.fetch_tables(port["url"], port["tables"], port["name"])
    
    scraper.close()
