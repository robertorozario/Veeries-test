from pathlib import Path
import pandas as pd
import re

class DataEnrichment :
    def __init__(self, input_file, output_folder="data/gold"):
        self.input_file = input_file
        self.output_folder = output_folder
        self.df = None
        self.dict_data = {'Total_Paranagua': 0, 'Total_Santos': 0, 'Importacoes': 0,
                           'Exportacoes': 0, 'Importacoes / exportacoes': 0}

    def extract_two_floats(self, s):
        matches = re.findall(r'\d+\.?\d*', str(s))
        return [float(num) for num in matches] if matches else [0]
    
    def load_data(self):
        self.df = pd.read_csv(self.input_file)

    def process_data(self):
        for _, row in self.df.iterrows():
            peso = sum(self.extract_two_floats(row['Peso']))
            if row['Status'] not in self.dict_data:
                self.dict_data[row['Status']] = 0
            if row['Mercadoria'] not in self.dict_data:
                self.dict_data[row['Mercadoria']] = 0

            if peso != 1 and 'Movs.' not in str(row['Peso']):
                self.dict_data[row['Status']] += peso  
                self.dict_data[row['Mercadoria']] += peso  

            if peso != 1 and 'Movs.' not in str(row['Peso']) and row['Porto'] == 'PORTO_DE_PARANAGUA' and row['Status'] != 'DESPACHADOS':
                self.dict_data['Total_Paranagua'] += peso  
            if peso != 1 and 'Movs.' not in str(row['Peso']) and row['Porto'] == 'PORTO_DE_SANTOS':
                self.dict_data['Total_Santos'] += peso  
            if (peso != 1 and 'Movs.' not in str(row['Peso']) and row['Sentido'] == 'Imp'):
                self.dict_data['Importacoes'] += peso  
            if (peso != 1 and 'Movs.' not in str(row['Peso']) and row['Sentido'] == 'Exp'):
                self.dict_data['Exportacoes'] += peso  
            if (peso != 1 and 'Movs.' not in str(row['Peso']) and row['Sentido'] == 'Imp/Exp'):
                self.dict_data['Importacoes / exportacoes'] += peso 

            

    def generate_markdown_report(self, df_gold, output_file="data/gold/data_report.md"):
        path = Path(output_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, "w") as f:
            f.write("# Relatorio de Movimentacao de Carga\n\n")
            f.write(f"**Data da analise:** {pd.Timestamp.now().date()}\n\n")
            f.write("## Resumo da Movimentacao\n\n")
            f.write(df_gold.to_markdown(index=False))
        
        print(f"Relatório salvo em {output_file}")

    def save_results(self):
        df_result = pd.DataFrame(list(self.dict_data.items()), columns=["Categoria", "Peso total em toneladas"])
        self.generate_markdown_report(df_result)


def run():
    input_file = "data/silver/TOTAL/SEM_AGRUPAR.csv"
    enrichment = DataEnrichment(input_file)
    enrichment.load_data()
    enrichment.process_data()
    enrichment.save_results()
