# Source code generated by Amphi
# Date: 2025-02-07 07:11:34
# Additional dependencies: psycopg2-binary
import pandas as pd
import sqlalchemy
import psycopg2
from typing import Dict, Any, Optional
import logging
import os
# Connection constants for Postgres
POSTGRES_HOST = "172.17.0.3"
POSTGRES_PORT = "5432"
POSTGRES_DATABASE_NAME = "ecdwh"
POSTGRES_USERNAME = "bruno"
POSTGRES_PASSWORD = "bruno"
POSTGRES_SCHEMA = "vw"



# ID&File

# Connect to the PostgreSQL database
postgresInput1_Engine = sqlalchemy.create_engine(f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE_NAME}")


# Execute SQL statement
try:
    with postgresInput1_Engine.connect() as conn:
        postgresInput1 = pd.read_sql(
            """
            select id, excel_file from excel_poc_test
where excel_file =  '001_22_STD_20220422_Prüfung.xlsx'
and excel_worksheet = 'Allgemeine Informationen'
order by id
limit 1;
            """,
            con=conn.connection
        ).convert_dtypes()
finally:
    postgresInput1_Engine.dispose()


# Filter and order columns
filterColumn1 = postgresInput1[["id", "excel_file"]]



# Connect to the Postgres database
filterColumn1Engine = sqlalchemy.create_engine(
  f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE_NAME}"
)


# Rename columns based on the mapping
filterColumn1 = filterColumn1.rename(columns={"excel_file": "datei_name"})

# Only keep relevant columns
filterColumn1 = filterColumn1[["id", "datei_name"]]

# Write DataFrame to Postgres
try:
    filterColumn1.to_sql(
        name="dateien",
        con=filterColumn1Engine,
        if_exists="append",
        index=False,
  schema="tarife"
    )
finally:
    filterColumn1Engine.dispose()


# abgabe_ev_austausch_nbs

# Connect to the PostgreSQL database
postgresInput2_Engine = sqlalchemy.create_engine(f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE_NAME}")


# Execute SQL statement
try:
    with postgresInput2_Engine.connect() as conn:
        postgresInput2 = pd.read_sql(
            """
            select * from 
(select 
id, abteilung, excel_file, excel_worksheet, excel_file_hash,
excel_row->>'0' as a,  
excel_row->>'1' as b,
excel_row->>'2' as c,
excel_row->>'3' as d,
excel_row->>'4' as e,
excel_row->>'5' as f,
excel_row->>'6' as g,
excel_row->>'7' as h,
excel_row->>'8' as i,
excel_row->>'9' as j,
excel_row->>'10' as k,
excel_row->>'11' as l,
excel_row->>'12' as m,
excel_row->>'13' as n,
excel_row->>'14' as o,
excel_row->>'15' as p
from excel_poc_test) 
--excel_row as u 
where 
-- id > 0
 excel_file = 'UC_004_VW_Wiener_Netze_GmbH_Gas_Netzbetreiber_20240921020525.xlsx' 
 and (excel_worksheet = 'MM_Bil' and a != '') or (excel_worksheet='U' and a='Kalenderjahr')
--and A in ('Unternehmen', 'EC-Nummer') 
order by id;
            """,
            con=conn.connection
        ).convert_dtypes()
finally:
    postgresInput2_Engine.dispose()




def transform(df):
 
    
    # Initialize empty lists to store transformed data
    transformed_data = []
    year_value = ''
    month_values = {}
    
    # First pass to get year and collect monthly values
    for _, row in df.iterrows():
        worksheet = row.get('excel_worksheet')
        
        # Get year from sheet 'U'
        if worksheet == 'U' and row.get('a') == 'Kalenderjahr':
            year_value = row.get('b')
            continue
            
        if worksheet == 'MM_Bil':
            description = row.get('a')
            
            # Skip header rows and empty rows
            if not description or description in [
                'Bilanzposition', 'DVR-Nr. 1069683',
                'Monatserhebung Netzbetreiber Erdgas 2024',
                'Unternehmen', 'Monatsbilanz insgesamt'
            ]:
                continue
                
            # Store values for each metric by month
            month_values[description] = {
                'jan': row.get('c'), 'feb': row.get('d'),
                'mar': row.get('e'), 'apr': row.get('f'),
                'may': row.get('g'), 'jun': row.get('h'),
                'jul': row.get('i'), 'aug': row.get('j'),
                'sep': row.get('k'), 'oct': row.get('l'),
                'nov': row.get('m'), 'dec': row.get('n')
            }
    
    # Create monthly records
    months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    month_names = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']
    
    for month_idx, month in enumerate(months):
        try:
            record = {
                'monat': month_names[month_idx],
                'jahr': year_value,
                'file_id': df['excel_file_hash'].iloc[0],
                'netz_abgabe_endverbraucher': parse_float_safe(month_values['Abgabe an Endverbraucher'][month]),
                'netz_eigenverbrauch_fernleitung': parse_float_safe(month_values['Eigenverbrauch im Fernleitungsnetz'][month]),
                'netz_eigenverbrauch_verteiler': parse_float_safe(month_values['Eigenverbrauch im Verteilernetz'][month]),
                'netz_verluste': parse_float_safe(month_values['Netzverlusten einschließlich Messdifferenzen und Restsaldo'][month]),
                'netz_einspeisung_biogen': parse_float_safe(month_values['Einspeisung biogener Gase'][month]),
                'netz_abgabe_geschuetzt': parse_float_safe(month_values['Abgabe an geschützte Kunden'][month]),
                'netz_abgabe_haushalt': parse_float_safe(month_values['davon Abgabe Haushaltskunden, die an ein Erdgasverteilernetz angeschlossen sind'][month]),
                'netz_abgabe_sozial': parse_float_safe(month_values['davon Abgabe an grundlegende soziale Dienste, die nicht den Bereichen Bildung und öffentliche Verwaltung angehören und die an ein Erdgasverteilernetz angeschlossen sind'][month]),
                'netz_abgabe_leistungsgemessen': parse_float_safe(month_values['Abgabe an leistungsgemessene Endverbraucher Übertag Summe von Blatt "MM_LPZ"'][month])
            }
            transformed_data.append(record)
        except Exception as e:
            logging.error(f"Error processing month {month}: {str(e)}")
    
  # customTransformations1 = pd.DataFrame(transformed_data)

    # Convert to DataFrame and return
    return pd.DataFrame(transformed_data)

customTransformations1 = transform(postgresInput2)
    # customTransformations1 = transform(df, ctx)

def parse_float_safe(value):
    if not value or value == '':
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


# Connect to the Postgres database
customTransformations1Engine = sqlalchemy.create_engine(
  f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE_NAME}"
)


# Only keep relevant columns
customTransformations1 = customTransformations1[["netz_abgabe_endverbraucher", "netz_eigenverbrauch_fernleitung", "jahr", "netz_eigenverbrauch_verteiler", "netz_verluste", "netz_einspeisung_biogen", "netz_abgabe_geschuetzt", "netz_abgabe_haushalt", "netz_abgabe_sozial", "netz_abgabe_leistungsgemessen", "file_id", "monat"]]

# Write DataFrame to Postgres
try:
    customTransformations1.to_sql(
        name="mm_bil",
        con=customTransformations1Engine,
        if_exists="append",
        index=False,
  schema=POSTGRES_SCHEMA
    )
finally:
    customTransformations1Engine.dispose()
