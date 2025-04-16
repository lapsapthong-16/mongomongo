# Author: Edwina Hon Kai Xin

class data_loader:
    @staticmethod
    def from_json_lines(file_path):
        import json
        data_list = []
        with open(file_path, 'r') as file:
            for line in file:
                if line.strip():
                    try:
                        data_list.append(json.loads(line))
                    except json.JSONDecodeError as e:
                        print(f"Skipping line: {line[:50]}... - {e}")
        return data_list

    @staticmethod
    def preview_json_lines(file_path, limit=5):
        import json
        from pprint import pprint
        with open(file_path, "r") as file:
            for i, line in enumerate(file):
                if i >= limit:
                    break
                if line.strip():
                    try:
                        obj = json.loads(line)
                        pprint(obj)
                    except json.JSONDecodeError as e:
                        print(f"Skipping invalid JSON line: {e}")
