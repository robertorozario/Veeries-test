# TESTE VEERIES - Desenvolvimento Python

Este projeto tem como objetivo desenvolver um processo automatizado em Python para coletar, processar e armazenar dados relacionados ao movimento de navios nos portos de Paranaguá e Santos. As informações coletadas são sobre volumes diários transportados pelos navios, com base em produtos, sentido (exportação ou importação) e porto (Paranaguá ou Santos). A arquitetura utilizada para a organização dos dados segue o modelo **Medallion**.

## Arquitetura Medallion

O fluxo de dados é estruturado em três camadas:

- **Camada Bronze**: Dados brutos, coletados diretamente das fontes.
- **Camada Prata**: Dados processados e transformados, prontos para análise e relatórios. Aqui, as informações são limpas, padronizadas e agrupadas.
- **Camada Ouro**: Dados enriquecidos e agregados, prontos para análise detalhada e relatórios mais profundos.

## Fontes de Dados

Os dados são extraídos das seguintes fontes de informações sobre os navios esperados nos portos:

1. [Porto de Paranaguá - APPaweb](https://www.appaweb.appa.pr.gov.br/appaweb/pesquisa.aspx?WCI=relLineUpRetroativo)
2. [Porto de Santos - Movimento de Navios](https://www.portodesantos.com.br/informacoes-operacionais/operacoes-portuarias/navegacao-e-movimento-de-navios/navios-esperados-carga/)

## Funcionalidades

- **Coleta de Dados**: O script coleta informações diárias sobre os navios esperados em cada porto.
- **Processamento (Camada Prata)**:
  - Remoção de colunas nulas.
  - Padronização dos cabeçalhos para uniformidade entre fontes.
  - Criação de tabelas agrupadas por tempo de chegada, mercadoria e sentido (exportação, importação).
  - Geração de tabelas unindo dados dos dois portos com os agrupamentos mencionados.
- **Armazenamento de Dados**: Os dados são armazenados de forma incremental, garantindo que os históricos não sejam sobrescritos.
- **Estrutura Modular**: O código está organizado de maneira modular, facilitando a manutenção e possíveis expansões futuras.

## Como Rodar o Projeto

1. **Clone o Repositório**

   `git clone https://github.com/seu-usuario/lineup-navios.git`

2. **Instale as Dependências**

    Crie um ambiente virtual e instale as dependências necessárias
    ```
    cd veeries-test
    python3 -m venv venv
    source venv/bin/activate  # Para Linux/Mac
    venv\Scripts\activate     # Para Windows
    pip install -r requirements.txt
    ```

3. **Execute o Script**

    Para coletar e processar os dados periodicamente:
    `python src/main.py`

    Para coletar e processar os dados uma única vez:
    `python src/main.py 'teste'`

4. **Visualize os Resultados**

    Os dados processados serão armazenados nas pastas apropriadas para cada camada de dados. 
    Consulte os arquivos gerados em data/bronze/, data/prata/ e data/ouro/ para visualizar os resultados.


**Considerações e possíveis melhorias**

    - O script foi desenvolvido para ser executado diariamente, realizando a coleta e o processamento incremental dos dados, portanto ao executar o script pela primeira vez ele executará normalmente após 24h, A persistência do histórico causou problemas durante o desenvolvimento do código, devido a isso optei pelo foco no webscraping e tratamento dos dados.

    - A visualização dos dados na camada bronze por questão de velocidade de acesso e manter a estrutura dos dataframes foi utilizado o formato parquet para uma melhor compressão dos dados e a velocidade para leitura, permitindo a modularidade do código.

    - Na camada ouro, devido a não tão consistência dos dados, optei pelo cálculo de toneladas por estágio do navio por porto (Atracado, Despachado, Sem prioridade e afins), mercadoria transportada, Sentido (Exportação, Importação, Importação/Exportação), e por final o cálculo de toneladas presentes em cada porto