import pandas as pd
import numpy as np

df_giornaliero = pd.read_excel("./dati_rumore.xlsx", sheet_name="Giornaliero")


def determine_quadrimestre(week_number):
    if week_number >= 40 or week_number <= 4:
        return 1
    elif 5 <= week_number <= 22:
        return 2
    elif 23 <= week_number <= 39:
        return 3


# Function to calculate the total LVA for a given year
def calculate_total_LVA(df):
    return 10 * np.log10((1 / 21) * np.sum(10 ** (df['LVA TOT DBA_x'] / 10)))


df_giornaliero['DATA'] = pd.to_datetime(df_giornaliero['DATA'], format='%d/%m/%Y')
df_giornaliero['Year'] = df_giornaliero['DATA'].dt.isocalendar().year
df_giornaliero['Week_Number'] = df_giornaliero['DATA'].dt.isocalendar().week
df_giornaliero = df_giornaliero.sort_values(by='DATA')
cols_to_convert = ['LVA DBA', 'LVA TOT DBA', 'LVA BG DBA']
df_giornaliero["EVENTI"] = pd.to_numeric(df_giornaliero["EVENTI"], errors="coerce", downcast="integer")
for col in cols_to_convert:
    df_giornaliero[col] = df_giornaliero[col].astype(str).str.replace(",", ".", regex=True)
    df_giornaliero[col] = pd.to_numeric(df_giornaliero[col], errors="coerce")
df_giornaliero['Quadrimestre'] = df_giornaliero['Week_Number'].apply(determine_quadrimestre)
df_giornaliero.loc[df_giornaliero['Quadrimestre'] == 1, 'Adjusted_Year'] = df_giornaliero.apply(
    lambda row: row['Year'] if row['Week_Number'] >= 40 else row['Year'] - 1, axis=1)
df_giornaliero.loc[df_giornaliero['Quadrimestre'] != 1, 'Adjusted_Year'] = df_giornaliero['Year']

df_settimanale = df_giornaliero.groupby(['Year', 'Week_Number']).agg(
    {'EVENTI': 'sum', 'LVA DBA': 'mean', 'LVA TOT DBA': 'mean', 'LVA BG DBA': 'mean'}).reset_index()
df_settimanale['Quadrimestre'] = df_settimanale['Week_Number'].apply(determine_quadrimestre)
df_settimanale.loc[df_settimanale['Quadrimestre'] == 1, 'Adjusted_Year'] = df_settimanale.apply(
    lambda row: row['Year'] if row['Week_Number'] >= 40 else row['Year'] - 1, axis=1)
df_settimanale.loc[df_settimanale['Quadrimestre'] != 1, 'Adjusted_Year'] = df_settimanale['Year']

# Create a new dataframe to store the weeks with the highest number of events for each Quadrimestre of every Year
df_LVAQuadrimestri = df_settimanale.loc[
    df_settimanale.groupby(['Year', 'Quadrimestre'])['EVENTI'].idxmax(), ['Year', 'Week_Number',
                                                                                   'EVENTI', 'LVA TOT DBA']]
df_LVAQuadrimestri.columns = ['Year', 'Week_Number', 'EVENTI', 'LVA TOT DBA']

# Medie Annuali
df_avg_LVA = df_LVAQuadrimestri.groupby('Year')['LVA TOT DBA'].mean().reset_index()

# Merge df_giornaliero with df_LVAQuadrimestri to get the data for the selected weeks
df_selected_weeks = df_giornaliero.merge(df_LVAQuadrimestri, on=['Year', 'Week_Number'], how='inner')
df_selected_weeks = df_selected_weeks.rename(
    columns={'EVENTI_y': 'Tot Week Events', 'LVA TOT DBA_y': 'WEEK AVG LVA TOT DBA'})

# Group by year and apply the function to calculate total LVA
df_LVA_Year = df_selected_weeks.groupby('Adjusted_Year').apply(calculate_total_LVA).reset_index()

# Rename the columns
df_LVA_Year.columns = ['Year', 'Total LVA']

output_file = "dati_rumore_v4.xlsx"
with pd.ExcelWriter(output_file) as writer:
    df_giornaliero.to_excel(writer, sheet_name="Giornaliero", index=False)
    df_settimanale.to_excel(writer, sheet_name="Settimanale", index=False)
    df_LVAQuadrimestri.to_excel(writer, sheet_name="LVA Quadrimestri", index=False)
    df_avg_LVA.to_excel(writer, sheet_name="Media LVA Annuale", index=False)
    df_selected_weeks.to_excel(writer, sheet_name="Settimane Selezionate", index=False)
    df_LVA_Year.to_excel(writer, sheet_name="Total LVA Annuale", index=False)
