import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time

# URL base della pagina da cui effettuare lo scraping
base_url = "https://www.bologna-airport.it/innovability/sostenibilita/ambiente-ed-energia/rumore/lva/"

# ID della stazione di rilevamento NMT 6 - Centro Sportivo Pizzoli
stazione_id = "12051"

# Data di inizio e fine per il periodo di interesse
start_year = 2013
end_year = datetime.now().year
end_month = datetime.now().month

# Lista per memorizzare tutte le tabelle
all_data = []

# Iterazione per ogni anno
for year in range(start_year, end_year + 1):
    print(year)
    # Effettua una richiesta iniziale per trovare i mesi disponibili
    params = {'idC': '61716', 'periodo': f'012{year}', 'stazione': stazione_id, 'Settimana': '0', 'Cerca': '1'}
    response = requests.get(base_url, params=params)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')

    # Trova i mesi disponibili nel filtro
    month_options = soup.find('select', {'name': 'periodo'}).find_all('option')
    available_months = [opt['value'][:2] for opt in month_options if opt['value'].endswith(str(year))]

    for month in available_months:
        print(month)
        periodo = f"{month}{year}"
        params['periodo'] = periodo

        response = requests.get(base_url, params=params)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        settimana_options = soup.find('select', {'name': 'Settimana'})
        if settimana_options:
            settimana_options = settimana_options.find_all('option')
        else:
            settimana_options = []

        for option in settimana_options:
            settimana_value = option['value']
            if settimana_value == '0':
                continue

            params['Settimana'] = settimana_value
            week_response = requests.get(base_url, params=params)
            week_response.raise_for_status()
            week_soup = BeautifulSoup(week_response.text, 'html.parser')

            table = week_soup.find('table', {'class': 'o-table c-table'})
            if not table:
                continue

            rows = table.find_all('tr')
            headers = [header.text.strip() for header in rows[0].find_all('th')]

            for row in rows:
                cols = row.find_all('td')
                data = [col.text.strip() for col in cols]
                if data:
                    if data[0] == 'Mensile':
                        data.insert(1, None)
                    else:
                        data[0] = data[0].split(' ')[1]
                    all_data.append(data)

                    data.append(settimana_value)
            time.sleep(1)  # Evita di sovraccaricare il server

# Creazione DataFrame pandas
df = pd.DataFrame(all_data, columns=['DATA', 'EVENTI', 'LVA DBA', 'LVA TOT DBA', 'LVA BG DBA', 'SETTIMANA'])

cols_to_convert = ["LVA DBA", "LVA TOT DBA", "LVA BG DBA"]
df["EVENTI"] = pd.to_numeric(df["EVENTI"], errors="coerce", downcast="integer")
for col in cols_to_convert:
    df[col] = df[col].astype(str).str.replace(",", ".", regex=True)
    df[col] = pd.to_numeric(df[col], errors="coerce")

df_settimanale = df[df["DATA"].astype(str).str.lower() == "mensile"].copy()
df_giornaliero = df[df["DATA"].astype(str).str.lower() != "mensile"].copy()
df_giornaliero["DATA"] = pd.to_datetime(df_giornaliero["DATA"], format="%d/%m/%Y", errors="coerce")
df_giornaliero["ANNO"] = df_giornaliero["DATA"].dt.year
df_giornaliero["MESE"] = df_giornaliero["DATA"].dt.month
df_giornaliero["GIORNO"] = df_giornaliero["DATA"].dt.day

agg_dict = {
    "EVENTI": "sum",
    "LVA DBA": "mean",
    "LVA TOT DBA": "mean",
    "LVA BG DBA": "mean"
}
# Calcolo delle aggregazioni settimanali
df_settimanale_calcolato = df_giornaliero.groupby("SETTIMANA").agg(agg_dict).reset_index()
df_settimanale_calcolato = df_settimanale_calcolato.sort_values(by="SETTIMANA")

# Ordinamento per data crescente
df_giornaliero = df_giornaliero.sort_values(by="DATA")
# Creazione della colonna MESE_GIORNO
df_giornaliero["MESE_GIORNO"] = df_giornaliero["DATA"].dt.strftime("%m-%d")

# Pivot con MESE_GIORNO come indice e nomi colonna corretti, aggregando EVENTI in somma e il resto in media
df_pivot_gg = df_giornaliero.pivot_table(index="MESE_GIORNO", columns="ANNO", values=["EVENTI", "LVA DBA", "LVA TOT DBA", "LVA BG DBA"], aggfunc=agg_dict)
df_pivot_gg.columns = [f"{col[0].replace(' ', '_')}_{col[1]}" for col in df_pivot_gg.columns]
df_pivot_gg.reset_index(inplace=True)
df_pivot_gg = df_pivot_gg.sort_values(by="MESE_GIORNO")

df_pivot_mensile = df_giornaliero.pivot_table(index="MESE", columns="ANNO", values=["EVENTI", "LVA DBA", "LVA TOT DBA", "LVA BG DBA"], aggfunc=agg_dict)
df_pivot_mensile.columns = [f"{col[0].replace(' ', '_')}_{col[1]}" for col in df_pivot_mensile.columns]
df_pivot_mensile.reset_index(inplace=True)
df_pivot_mensile = df_pivot_mensile.sort_values(by="MESE")

output_file = "dati_rumore.xlsx"
with pd.ExcelWriter(output_file) as writer:
    df_giornaliero.to_excel(writer, sheet_name="Giornaliero", index=False)
    df_settimanale.to_excel(writer, sheet_name="Settimanale", index=False)
    df_settimanale_calcolato.to_excel(writer, sheet_name="Settimanale_Calcolato", index=False)
    df_pivot_gg.to_excel(writer, sheet_name="Giornaliero_Pivot", index=False)
    df_pivot_mensile.to_excel(writer, sheet_name="Mensile_Pivot", index=False)
