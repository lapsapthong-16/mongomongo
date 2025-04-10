
class data_loader:
    @staticmethod
    def from_json(file_path):
        import json
        with open(file_path) as f:
            return json.load(f)

    @staticmethod
    def from_parquet(file_path):
        import pandas as pd
        return pd.read_parquet(file_path).to_dict("records")
