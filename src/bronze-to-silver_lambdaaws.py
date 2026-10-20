import os
import json
import re
import csv
import boto3
import pandas as pd
from datetime import datetime
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq

BUCKET                   = "etl-compras-portfolio"
BRONZE_PREFIX            = "bronze/"
BRONZE_PROCESADOS_PREFIX = "bronze/procesados/"
SILVER_PREFIX            = "silver/"

s3_client = boto3.client("s3")

# =================================================================================================
# HELPERS
# =================================================================================================

def list_pending_files(bucket: str) -> list:
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=BRONZE_PREFIX)
    pending = []
    
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                p = obj['Key']
                if p.lower().endswith(".xlsx") and "/procesados/" not in p:
                    pending.append(p)
    return sorted(pending)


def build_output_name(xlsx_key: str) -> str:
    base = xlsx_key.rsplit("/", 1)[-1].replace(".xlsx", "")
    if " - " in base:
        dates_part = base.split(" - ")[-1].replace(" to ", " - ")
        return f"Sales_table {dates_part}.parquet"
    safe_base = re.sub(r"\s+", "_", base)
    return f"{safe_base}.parquet"


def move_to_processed(bucket: str, key: str):
    filename   = key.rsplit("/", 1)[-1]
    dest_key   = f"{BRONZE_PROCESADOS_PREFIX}{filename}"
    s3_client.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": key},
        Key=dest_key,
    )
    s3_client.delete_object(Bucket=bucket, Key=key)
    print(f"   [MOVIDO] s3://{bucket}/{key} -> s3://{bucket}/{dest_key}", flush=True)


def parse_date_to_iso(val):
    """Convierte fechas crudas directamente a YYYY-MM-DD para evitar el parser C de Pandas."""
    if pd.isna(val) or val == "": return ""
    try:
        return datetime.strptime(str(val).strip(), "%d/%m/%y").strftime("%Y-%m-%d")
    except:
        return ""


def clean_excel_to_parquet(bucket: str, key: str, out_parquet_path: str):
    tmp_excel = "/tmp/raw.xlsx"
    tmp_csv   = "/tmp/clean.csv"
    
    print(f"   [1/4] Descargando Excel a disco...", flush=True)
    s3_client.download_file(bucket, key, tmp_excel)
    
    print(f"   [2/4] Extrayendo datos crudos con motor nativo openpyxl...", flush=True)
    df_raw = pd.read_excel(tmp_excel, header=None, engine="openpyxl")

    print(f"   [3/4] Procesando fechas y números a CSV...", flush=True)
    current_group = current_item_code = current_item_desc = current_supplier_name = ""
    
    with open(tmp_csv, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            "group", "item_code", "description", "date", "unit", 
            "quantity", "rate", "value", "lc", "invoice_number", 
            "supplier_code", "supplier_name", "reference_no", "reference_date"
        ])
        
        for row in df_raw.itertuples(index=False, name=None):
            col0_val = row[0]
            col0 = "" if pd.isna(col0_val) else str(col0_val).strip()

            if pd.isna(row[1]) and col0 and not col0.startswith((
                "===", "---", "Inflair", "Purchase", "Group Code", "Item Code",
                "Supplier Code", "Location Code", "Invoice date", "Total", "Date",
            )):
                match = re.match(r"^([A-Z0-9]{2})\s+([A-Z0-9]+)\s+(.*)", col0)
                if match:
                    current_group     = match.group(1).strip()
                    current_item_code = match.group(2).strip()
                    desc = match.group(3).strip()
                    current_item_desc = desc[:-1].strip() if desc.endswith("|") else desc
                continue

            if pd.notna(row[0]) and pd.notna(row[1]) and pd.notna(row[2]) and not col0.startswith((
                "---", "===", "Date", "Total", "--------",
            )):
                supp_name = "" if pd.isna(row[8]) else str(row[8]).strip()
                if supp_name:
                    current_supplier_name = supp_name

                qty = "" if pd.isna(row[2]) else str(row[2]).replace(',', '').strip()
                rate = "" if pd.isna(row[3]) else str(row[3]).replace(',', '').strip()
                val = "" if pd.isna(row[4]) else str(row[4]).replace(',', '').strip()

                writer.writerow([
                    current_group, current_item_code, current_item_desc,
                    parse_date_to_iso(row[0]),
                    "" if pd.isna(row[1]) else str(row[1]).strip(),
                    qty, rate, val,
                    "" if pd.isna(row[5]) else str(row[5]).strip(),
                    "" if pd.isna(row[6]) else str(row[6]).strip(),
                    "" if pd.isna(row[7]) else str(row[7]).strip(),
                    current_supplier_name,
                    "" if pd.isna(row[9]) else str(row[9]).strip(),
                    parse_date_to_iso(row[10])
                ])

    del df_raw
    if os.path.exists(tmp_excel):
        os.remove(tmp_excel)

    print(f"   [4/4] Bypass -> Creando Parquet con PyArrow nativo...", flush=True)
    # Ignoramos a Pandas por completo. PyArrow lee el CSV directamente.
    convert_options = pv.ConvertOptions(
        column_types={
            "quantity": pa.float64(),
            "rate": pa.float64(),
            "value": pa.float64()
        }
    )
    table = pv.read_csv(tmp_csv, convert_options=convert_options)
    pq.write_table(table, out_parquet_path)
    
    if os.path.exists(tmp_csv):
        os.remove(tmp_csv)


# =================================================================================================
# HANDLER
# =================================================================================================

def lambda_handler(event, context):
    bucket = BUCKET
    pending_files = list_pending_files(bucket)

    if not pending_files:
        print("No hay archivos nuevos en bronze/ para procesar.")
        return {
            "statusCode": 200,
            "body": json.dumps("Sin archivos nuevos. Nada que hacer."),
        }

    print(f"Archivos pendientes: {len(pending_files)}", flush=True)
    resultados = {"exitosos": [], "fallidos": []}

    for key in pending_files:
        print(f"\n=== Procesando: {key} ===", flush=True)
        try:
            out_name = build_output_name(key)
            out_key = f"{SILVER_PREFIX}{out_name}"
            tmp_parquet = "/tmp/salida.parquet"

            clean_excel_to_parquet(bucket, key, tmp_parquet)

            print(f"   [Subiendo] s3://{bucket}/{out_key}", flush=True)
            s3_client.upload_file(tmp_parquet, bucket, out_key)

            move_to_processed(bucket, key)
            resultados["exitosos"].append(key)
            
            if os.path.exists(tmp_parquet):
                os.remove(tmp_parquet)

        except Exception as e:
            print(f"    ERROR procesando {key}: {e}", flush=True)
            resultados["fallidos"].append({"archivo": key, "error": str(e)})

    status_code = 200 if not resultados["fallidos"] else 207 
    print(f"\nResumen -> exitosos: {len(resultados['exitosos'])} | fallidos: {len(resultados['fallidos'])}", flush=True)

    return {
        "statusCode": status_code,
        "body": json.dumps(resultados),
    }