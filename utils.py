import re
from datetime import datetime

def transform_date(date: datetime) -> dict:
    """Break a datetime into day/month/year/week/weekyear."""
    return {
        'day': date.day,
        'month': date.month,
        'year': date.year,
        'week': date.isocalendar()[1],
        'weekyear': date.isocalendar()[0]
    }

def clean_html(text: str) -> str:
    """Remove HTML tags."""
    return re.sub(r'<.*?>', '', text or "")

def normalize_liters(name: str) -> str:
    """Normalize volume notations."""
    replacements = {'½L': '500ml', '¼L': '250ml', '1L': '1000ml'}
    for old, new in replacements.items():
        name = name.replace(old, new)
    return name

def infer_product_type(name: str) -> str:
    """Infer product type from product name."""
    name = name.lower()
    keyword_map = [
        (["øl", "beer", "tuborg", "ale"], "Øl"),
        (["whisky", "vodka", "gin", "rum"], "Hård spiritus"),
        (["vin", "cider"], "Spiritus"),
        (["cola", "fanta", "soda"], "Sodavand"),
        (["kaffe"], "Kaffe"),
        (["energi", "red bull"], "Energidrik"),
        (["vand", "water"], "Vitamin Vand"),
        (["mælk", "milk", "cocio"], "Mælk"),
        (["brød", "chips", "slik"], "Mad"),
        (["event", "fest"], "Events"),
    ]
    for keys, typ in keyword_map:
        if any(k in name for k in keys):
            return typ
    return "Ukategoriseret"

def product_type_to_category(typ: str) -> str:
    """Map product type to category."""
    mapping = {
        "Sodavand": "Sodavand",
        "Vitamin Vand": "Andet",
        "Øl": "Øl",
        "Kaffe": "Koffein",
        "Hård spiritus": "Spiritus",
        "Spiritus": "Spiritus",
        "Mad": "Mad",
        "Energidrik": "Koffein",
        "Mælk": "Andet",
        "Events": "Events",
        "Andet": "Andet",
        "Ukategoriseret": "Ukategoriseret"
    }
    return mapping.get(typ, "Ukategoriseret")
