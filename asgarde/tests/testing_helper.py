import json
from typing import List, Dict


def log_element(elem):
    print(elem)
    return elem


def remove_technical_field(elem):
    elem.pop('dwhUpdateDate')
    return elem


def load_file_as_string(file_path: str):
    elements: List[Dict] = load_file_as_dict(file_path)

    return list(map(lambda el: json.dumps(el), elements))


def load_file_as_dict(file_path: str):
    with open(file_path) as file:
        return json.load(file)
