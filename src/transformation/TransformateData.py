from pathlib import Path
import pandas as pd
from ast import literal_eval

class PortoDataProcessor:
    def __init__(self, porto_name: str, base_path: str):
        self.porto_name = porto_name
        self.base_path = Path(base_path)
        self.df_total = pd.DataFrame()
    
    def process_files(self):
        parquet_files = [file_p for file_p in self.base_path.rglob('*.parquet') if file_p.is_file()]
        df_total_porto = pd.DataFrame()

        for data in parquet_files:
            df = pd.read_parquet(str(data))
            df = self._process_columns(df)
            table_name = list(df.columns.levels[0])[0]
            
            if self.porto_name == "PORTO_DE_PARANAGUA" and table_name == 'APOIO PORTUÁRIO / OUTROS':
                continue
            
            df_grouped = self._filter_and_transform(df, table_name, data.parent.name)
            if df_grouped is not None:
                df_total_porto = pd.concat([df_total_porto, df_grouped], ignore_index=True)
                self.df_total = pd.concat([self.df_total, df_grouped], ignore_index=True)

        self._save_grouped_data(df_total_porto, self.porto_name)
    
    def _process_columns(self, df):
        if isinstance(df.columns[0], str) and df.columns[0].startswith("("):
            df.columns = pd.MultiIndex.from_tuples(
                [literal_eval(col) if isinstance(col, str) and col.startswith("(") else col for col in df.columns]
            )
        return df
    
    def _filter_and_transform(self, df, table_name, porto):
        if self.porto_name == "PORTO_DE_PARANAGUA":
            columns = [
                (table_name, 'Mercadoria'),
                (table_name, 'Sentido'),
                (table_name, 'Chegada' if table_name != 'ESPERADOS' else 'ETA')
            ]
        else:  # PORTO_DE_SANTOS
            columns = [
                (table_name, 'Mercadoria Goods'),
                (table_name, 'Operaç Operat'),
                (table_name, 'Cheg/Arrival d/m/y')
            ]

        selected_columns = [col for col in columns if col in df.columns]
        if not selected_columns:
            return None
        
        df_grouped = df[selected_columns].copy()
        
        if self.porto_name == "PORTO_DE_SANTOS":
            df_grouped[(table_name, 'Operaç Operat')] = df_grouped[(table_name, 'Operaç Operat')].replace(
                {'EMB DESC': 'Imp/Exp', 'DESC': 'Imp', 'EMB': 'Exp'})
        
        df_grouped['Status'] = table_name
        df_grouped['Quantidade'] = 0
        df_grouped['Porto'] = porto
        df_grouped.columns = ['Mercadoria', 'Sentido', 'Chegada', 'Status', 'Quantidade', 'Porto']
        return df_grouped
    
    def _save_grouped_data(self, df, porto_name):
        self._save_to_csv(df, ['Mercadoria', 'Status', 'Porto'], f"data/silver/{porto_name}/AGRUPADO_MERCADORIA_STATUS.csv")
        self._save_to_csv(df, ['Chegada', 'Status', 'Porto'], f"data/silver/{porto_name}/AGRUPADO_CHEGADA.csv")
        self._save_to_csv(df, ['Sentido', 'Status', 'Porto'], f"data/silver/{porto_name}/AGRUPADO_SENTIDO.csv")

    def _save_to_csv(self, df, group_by_cols, file_path):
        if "CHEGADA" in file_path:
            df_grouped = df.groupby(group_by_cols).agg({
                'Sentido': lambda x: '; '.join(str(i) for i in x if pd.notna(i)),
                'Quantidade': 'size'
            }).reset_index()
        elif "SENTIDO" in file_path:
            df_grouped = df.groupby(group_by_cols).agg({
                'Chegada': lambda x: '; '.join(str(i) for i in x if pd.notna(i)),
                'Quantidade': 'size'
            }).reset_index()
        else:
            df_grouped = df.groupby(group_by_cols).agg({
                'Sentido': lambda x: '; '.join(str(i) for i in x if pd.notna(i)),
                'Chegada': lambda x: '; '.join(str(i) for i in x if pd.notna(i)),
                'Quantidade': 'size'
            }).reset_index()
        
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        df_grouped.to_csv(path, index=False)

    def save_total_data(self):
        path = Path(f"data/silver/TOTAL/SEM_AGRUPAR.csv")
        path.parent.mkdir(parents=True, exist_ok=True)
        self.df_total.to_csv(path, index=False)
        
        self._save_to_csv(self.df_total, ['Mercadoria', 'Status', 'Porto'], "data/silver/TOTAL/AGRUPADO_MERCADORIA_STATUS.csv")
        self._save_to_csv(self.df_total, ['Chegada', 'Status', 'Porto'], "data/silver/TOTAL/AGRUPADO_CHEGADA.csv")
        self._save_to_csv(self.df_total, ['Sentido', 'Status', 'Porto'], "data/silver/TOTAL/AGRUPADO_SENTIDO.csv")


