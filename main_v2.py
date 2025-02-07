import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import numpy as np

# URL base della pagina da cui effettuare lo scraping
base_url = "https://www.bologna-airport.it/innovability/sostenibilita/ambiente-ed-energia/rumore/lva/"

# ID della stazione di rilevamento NMT 6 - Centro Sportivo Pizzoli
stazione_id = "12051"

# Define the start and end years
start_year = 2013
end_year = 2024

# Function to create a list of years from start year to end year
def create_year_list(start_year, end_year):
    return [str(year) for year in range(start_year, end_year + 1)]

# Anni da considerare
years_list = create_year_list(start_year, end_year)

# Lista per memorizzare tutte le tabelle
all_data = []

params = {'idC': '61716', 'periodo': '0', 'stazione': stazione_id, 'Settimana': '0', 'Cerca': '1'}
response = requests.get(base_url, params=params)
response.raise_for_status()
soup = BeautifulSoup(response.text, 'html.parser')

month_options = soup.find('select', {'name': 'periodo'}).find_all('option')
available_months_years = [opt['value'] for opt in month_options]

for month in available_months_years:
    print(month)
    if month[2:] not in years_list:
        continue
    params['periodo'] = month
    response = requests.get(base_url, params=params)
    response.raise_for_status()
    soup_month = BeautifulSoup(response.text, 'html.parser')

    table = soup_month.find('table', {'class': 'o-table c-table'})
    if not table:
        continue
    rows = table.find_all('tr')
    headers = [header.text.strip() for header in rows[0].find_all('th')]

    for row in rows[1:]:
        cols = row.find_all('td')
        data = [col.text.strip() for col in cols]
        if data:
            if data[0] == 'Mensile':
                continue
            else:
                all_data.append(data)

def determine_quadrimestre(week_number):
    if week_number >= 40 or week_number <= 4:
        return 1
    elif week_number >= 5 and week_number <= 22:
        return 2
    elif week_number >= 23 and week_number <= 39:
        return 3
    
df_giornaliero = pd.DataFrame(all_data, columns=['Data', 'Eventi', 'Lva dBA', 'Lva Tot dBA', 'Lva bg dBA'])
df_giornaliero[['WeekDay', 'Data']] = df_giornaliero['Data'].str.split(' ', n=1, expand=True)
df_giornaliero['Data'] = pd.to_datetime(df_giornaliero['Data'], format='%d/%m/%Y')
df_giornaliero['Year'] = df_giornaliero['Data'].dt.isocalendar().year
df_giornaliero['Week_Number'] = df_giornaliero['Data'].dt.isocalendar().week
df_giornaliero = df_giornaliero.sort_values(by='Data')
cols_to_convert = ['Lva dBA', 'Lva Tot dBA', 'Lva bg dBA']
df_giornaliero["Eventi"] = pd.to_numeric(df_giornaliero["Eventi"], errors="coerce", downcast="integer")
for col in cols_to_convert:
    df_giornaliero[col] = df_giornaliero[col].astype(str).str.replace(",", ".", regex=True)
    df_giornaliero[col] = pd.to_numeric(df_giornaliero[col], errors="coerce")
df_giornaliero['Quadrimestre'] = df_giornaliero['Week_Number'].apply(determine_quadrimestre)
df_giornaliero.loc[df_giornaliero['Quadrimestre'] == 1, 'Adjusted_Year'] = df_giornaliero.apply(lambda row: row['Year'] if row['Week_Number'] >= 40 else row['Year'] - 1, axis=1)
df_giornaliero.loc[df_giornaliero['Quadrimestre'] != 1, 'Adjusted_Year'] = df_giornaliero['Year']




df_settimanale = df_giornaliero.groupby(['Year', 'Week_Number']).agg({'Eventi': 'sum', 'Lva dBA': 'mean', 'Lva Tot dBA': 'mean', 'Lva bg dBA': 'mean'}).reset_index()
df_settimanale['Quadrimestre'] = df_settimanale['Week_Number'].apply(determine_quadrimestre)
df_settimanale.loc[df_settimanale['Quadrimestre'] == 1, 'Adjusted_Year'] = df_settimanale.apply(lambda row: row['Year'] if row['Week_Number'] >= 40 else row['Year'] - 1, axis=1)
df_settimanale.loc[df_settimanale['Quadrimestre'] != 1, 'Adjusted_Year'] = df_settimanale['Year']


# Create a new dataframe to store the weeks with the highest number of events for each Quadrimestre of every Year
df_LVAQuadrimestri = df_settimanale.loc[df_settimanale.groupby(['Adjusted_Year','Quadrimestre'])['Eventi'].idxmax(), ['Adjusted_Year', 'Week_Number', 'Eventi', 'Lva Tot dBA']]
df_LVAQuadrimestri.columns = ['Year', 'Week', 'Eventi', 'Lva Tot dBA']

# Medie Annuali
df_avg_LVA = df_LVAQuadrimestri.groupby('Year')['Lva Tot dBA'].mean().reset_index()

# Merge df_giornaliero with df_LVAQuadrimestri to get the data for the selected weeks
df_selected_weeks = df_giornaliero.merge(df_LVAQuadrimestri, left_on=['Adjusted_Year', 'Week_Number'], right_on=['Year', 'Week'], how='inner')
df_selected_weeks = df_selected_weeks.drop(['Year_y', 'Week'], axis=1)
df_selected_weeks = df_selected_weeks.rename(columns={'Eventi_y': 'Tot Week Events', 'Lva Tot dBA_y': 'WEEK AVG Lva Tot dBA'})

# Function to calculate the total LVA for a given year
def calculate_total_LVA(df):
    return 10 * np.log10((1/21) * np.sum(10**(df['Lva Tot dBA_x']/10)))

# Group by year and apply the function to calculate total LVA
df_LVA_Year = df_selected_weeks.groupby('Adjusted_Year').apply(calculate_total_LVA).reset_index()

# Rename the columns
df_LVA_Year.columns = ['Year', 'Total LVA']

output_file = "dati_rumore_marco.xlsx"
with pd.ExcelWriter(output_file) as writer:
    df_giornaliero.to_excel(writer, sheet_name="Giornaliero", index=False)
    df_settimanale.to_excel(writer, sheet_name="Settimanale", index=False)
    df_LVAQuadrimestri.to_excel(writer, sheet_name="LVA Quadrimestri", index=False)
    df_avg_LVA.to_excel(writer, sheet_name="Media LVA Annuale", index=False)
    df_selected_weeks.to_excel(writer, sheet_name="Settimane Selezionate", index=False)
    df_LVA_Year.to_excel(writer, sheet_name="Total LVA Annuale", index=False)