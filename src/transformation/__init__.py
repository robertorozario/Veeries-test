from TransformateData import PortoDataProcessor
import pandas as pd

def main():
    paranagua_processor = PortoDataProcessor("PORTO_DE_PARANAGUA", "./data/bronze/PORTO_DE_PARANAGUA")
    paranagua_processor.process_files()

    santos_processor = PortoDataProcessor("PORTO_DE_SANTOS", "./data/bronze/PORTO_DE_SANTOS")
    santos_processor.process_files()
    
    total_processor = PortoDataProcessor("TOTAL", "")
    total_processor.df_total = pd.concat([paranagua_processor.df_total, santos_processor.df_total], ignore_index=True)
    total_processor.save_total_data()


if __name__ == "__main__":
    main()
