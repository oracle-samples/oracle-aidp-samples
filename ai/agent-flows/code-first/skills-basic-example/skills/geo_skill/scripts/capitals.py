CAPITALS = {
    "india": "New Delhi",
    "france": "Paris",
    "japan": "Tokyo"
}

def get_capital(country: str):
    key = country.strip().lower()
    return {"country": country, "capital": CAPITALS.get(key, "Unknown")}
