from dagster import op, job, get_dagster_logger
import pandas as pd
import sqlalchemy
from typing import Dict, Any, Optional
import logging
import os

# Connection constants for Postgres
POSTGRES_HOST = "172.31.0.4"
POSTGRES_PORT = "5432"
POSTGRES_DATABASE_NAME = "ecdwh"
POSTGRES_USERNAME = "bruno"
POSTGRES_PASSWORD = "bruno"
POSTGRES_SCHEMA = "vw"

@op
def extract_mm_wechsel_data():
    """
    Extracts all data relevant for transformation from Postgres table excel_poc_test.
    """
    postgresInput2_Engine = sqlalchemy.create_engine(
        f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE_NAME}"
    )

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
                where 
                excel_file = 'UC_004_VW_Wiener_Netze_GmbH_Gas_Netzbetreiber_20240921020525.xlsx' 
                and excel_worksheet = 'MM_Wechsel' 
                order by id;
                """,
                con=conn.connection
            ).convert_dtypes()
    finally:
        postgresInput2_Engine.dispose()
    
    return postgresInput2

@op
def transform_mm_wechsel_data(postgresInput2):
    """
    Transforms the extracted MM_Wechsel data.
    """
    def has_value(obj):
        for key in obj:
            value = obj[key]
            if value and len(str(value)) > 0:
                return True
        return False
    
    # Define months
    months = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12']
    
    # Initialize data structure
    categorized_data = {
        'verbraucherkategorie': {},
        'versorger': {}
    }
    
    # Process records
    temp_category = ""
    temp_sub_category = ""
    
    logger = get_dagster_logger()
    logger.info("Starting MM_Wechsel transformation")
    
    for _, row in postgresInput2.iterrows():
        # Skip if not MM_Wechsel worksheet
        if row['excel_worksheet'] != 'MM_Wechsel' or pd.isna(row['a']):
            continue
            
        category = row['a']
        main_category = row['b']
        sub_category = row['c']
        kunden_typ = row['d']
        
        # Skip headers, totals and empty rows
        if (pd.isna(category) or
            category == 'durchgeführte Versorgerwechsel (bezogen auf Zählpunkte)' or
            category == 'DVR-Nr. 1069683' or
            category == 'Monatserhebung Netzbetreiber Erdgas 2024' or
            category == 'Unternehmen' or
            category == 'Versorgerwechsel' or
            str(category).startswith('Versorger Eingabeart') or
            str(category).startswith('Versorger EC-Nummer') or
            str(main_category).startswith('Gesamt') or
            sub_category == 'Ingesamt'):
            continue
            
        # Handle Verbraucherkategorie section
        if (category == 'Verbraucherkategorie' or
            str(kunden_typ).startswith('bis') or
            str(kunden_typ).startswith('über') or
            str(kunden_typ).startswith('von') or
            str(sub_category).startswith('bis') or
            str(sub_category).startswith('über') or
            str(sub_category).startswith('von')):
                
            if sub_category and not pd.isna(sub_category):
                column_name = f"vk_{sub_category.replace('/[^a-zA-Z0-9]/g', '_').lower().replace('/', '_')}"
                categorized_data['verbraucherkategorie'][column_name] = {
                    'e': row['e'], 'f': row['f'], 'g': row['g'],
                    'h': row['h'], 'i': row['i'], 'j': row['j'],
                    'k': row['k'], 'l': row['l'], 'm': row['m'],
                    'n': row['n'], 'o': row['o'], 'p': row['p']
                }
                
        # Handle Versorger (AT*) sections
        elif (str(category).startswith('AT') or
              kunden_typ == "Haushalte" or
              kunden_typ == "Nicht-Haushalte"):
            
            if temp_category != category and category != "":
                temp_category = category
                
            if temp_sub_category != sub_category and sub_category != "":
                temp_sub_category = sub_category
                
            logger.info(f"Processing versorger: {kunden_typ}")
            
            versorger_key = f"{temp_category}_{temp_sub_category.lower()}_{kunden_typ.lower().replace('-', '_')}"
            logger.info(f"Generated versorger key: {versorger_key}")
            
            if versorger_key not in categorized_data['versorger']:
                categorized_data['versorger'][versorger_key] = {
                    'e': row['e'], 'f': row['f'], 'g': row['g'],
                    'h': row['h'], 'i': row['i'], 'j': row['j'],
                    'k': row['k'], 'l': row['l'], 'm': row['m'],
                    'n': row['n'], 'o': row['o'], 'p': row['p']
                }
    
    # Create monthly records
    transformed_data = []
    
    if has_value(categorized_data):
        for month_idx, month in enumerate(months):
            month_record = {}
            month_key = chr(101 + month_idx)  # 'e' through 'p'
            
            # Add month name and file_id
            month_record['monat'] = month
            if not postgresInput2.empty:
                month_record['file_id'] = postgresInput2['excel_file_hash'].iloc[0]
                
            # Add Verbraucherkategorie values
            for vk_key in categorized_data['verbraucherkategorie']:
                month_record[vk_key] = categorized_data['verbraucherkategorie'][vk_key][month_key]
                
            # Add Versorger values
            for versorger_key in categorized_data['versorger']:
                month_record[versorger_key] = categorized_data['versorger'][versorger_key][month_key]
                
            transformed_data.append(month_record)
    
    return pd.DataFrame(transformed_data)

@op
def load_transformed_data(customTransformations1):
    """
    Loads the transformed data into the Postgres table.
    """
    customTransformations1Engine = sqlalchemy.create_engine(
        f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DATABASE_NAME}"
    )

    # Column renaming mapping
    rename_mapping = {
        "vk_bis 5.600 kwh": "vk_bis_5600_kwh",
        "vk_über 55.600 kwh": "vk_uber_5600_kwh",
        "vk_über 5.600 kwh bis 55.600 kwh": "vk_uber_5600_kwh_bis_55600_kwh",
        # ... add all other column renames here
    }

    # Rename columns
    customTransformations1 = customTransformations1.rename(columns=rename_mapping)

    try:
        customTransformations1.to_sql(
            name="mm_wechsel",
            con=customTransformations1Engine,
            if_exists="append",
            index=False,
            schema=POSTGRES_SCHEMA
        )
    finally:
        customTransformations1Engine.dispose()

@job
def mm_wechsel_job():
    """
    Dagster job that extracts MM_Wechsel data, transforms it, and loads the transformed data.
    """
    extracted_data = extract_mm_wechsel_data()
    transformed_data = transform_mm_wechsel_data(extracted_data)
    load_transformed_data(transformed_data)