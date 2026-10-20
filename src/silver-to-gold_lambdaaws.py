import os
import json
import boto3
import awswrangler as wr
import pandas as pd

BUCKET = "etl-compras-portfolio"
SILVER_PREFIX = "silver/"
SILVER_PROCESADOS_PREFIX = "silver/procesados/"
GOLD_PREFIX = "gold/"

s3_client = boto3.client("s3")

def list_pending_files(bucket: str) -> list:
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=SILVER_PREFIX)
    pending = []
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                p = obj['Key']
                if p.endswith(".parquet") and "/procesados/" not in p:
                    pending.append(p)
    return sorted(pending)

def move_to_processed(bucket: str, key: str):
    filename = key.rsplit("/", 1)[-1]
    dest_key = f"{SILVER_PROCESADOS_PREFIX}{filename}"
    s3_client.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": key},
        Key=dest_key,
    )
    s3_client.delete_object(Bucket=bucket, Key=key)
    print(f"   [MOVIDO] s3://{bucket}/{key} -> s3://{bucket}/{dest_key}", flush=True)

def process_silver_to_gold(bucket: str, key: str):
    print(f"   [1/6] Leyendo datos limpios desde Silver...", flush=True)
    df_new = wr.s3.read_parquet(f"s3://{bucket}/{key}")
    
    df_new['item_code'] = df_new['item_code'].astype(str).str.strip()
    df_new['supplier_code'] = df_new['supplier_code'].astype(str).str.strip()
    df_new['unit'] = df_new['unit'].astype(str).str.strip()

    # ==========================================
    # 1. dim_customer
    # ==========================================
    print(f"   [2/6] Modelando dim_customer...", flush=True)
    df_cust = df_new[['supplier_code', 'supplier_name']].copy()
    df_cust.rename(columns={'supplier_code': 'customer_code', 'supplier_name': 'customer_name'}, inplace=True)
    df_cust = df_cust[(df_cust['customer_code'] != "") & (df_cust['customer_code'] != "nan")].drop_duplicates()
    
    # NUEVO: Guardando dentro de la carpeta /dim_customer/
    cust_path = f"s3://{bucket}/{GOLD_PREFIX}dim_customer/dim_customer.parquet"
    try:
        df_cust_old = wr.s3.read_parquet(cust_path)
        df_cust = pd.concat([df_cust_old, df_cust]).drop_duplicates(subset=['customer_code'], keep='last')
    except: pass
    wr.s3.to_parquet(df=df_cust, path=cust_path, index=False)

    # ==========================================
    # 2. dim_item
    # ==========================================
    print(f"   [3/6] Modelando dim_item...", flush=True)
    df_item = df_new[['item_code', 'group', 'description']].copy()
    df_item = df_item[(df_item['item_code'] != "") & (df_item['item_code'] != "nan")].drop_duplicates()
    
    # NUEVO: Guardando dentro de la carpeta /dim_item/
    item_path = f"s3://{bucket}/{GOLD_PREFIX}dim_item/dim_item.parquet"
    try:
        df_item_old = wr.s3.read_parquet(item_path)
        df_item = pd.concat([df_item_old, df_item]).drop_duplicates(subset=['item_code'], keep='last')
    except: pass
    wr.s3.to_parquet(df=df_item, path=item_path, index=False)

    # ==========================================
    # 3. dim_unit
    # ==========================================
    print(f"   [4/6] Modelando dim_unit...", flush=True)
    df_unit = df_new[['unit']].copy()
    df_unit = df_unit[(df_unit['unit'] != "") & (df_unit['unit'] != "nan")].drop_duplicates()
    
    # NUEVO: Guardando dentro de la carpeta /dim_unit/
    unit_path = f"s3://{bucket}/{GOLD_PREFIX}dim_unit/dim_unit.parquet"
    try:
        df_unit_old = wr.s3.read_parquet(unit_path)
        df_unit = pd.concat([df_unit_old, df_unit]).drop_duplicates(subset=['unit'], keep='last')
    except: pass
    wr.s3.to_parquet(df=df_unit, path=unit_path, index=False)

    # ==========================================
    # 4. dim_date
    # ==========================================
    print(f"   [5/6] Modelando dim_date...", flush=True)
    df_date = df_new[['date']].dropna().drop_duplicates().copy()
    df_date['date'] = pd.to_datetime(df_date['date'])
    df_date['year'] = df_date['date'].dt.year
    df_date['month'] = df_date['date'].dt.month
    df_date['day'] = df_date['date'].dt.day
    df_date['quarter'] = df_date['date'].dt.quarter
    
    # NUEVO: Guardando dentro de la carpeta /dim_date/
    date_path = f"s3://{bucket}/{GOLD_PREFIX}dim_date/dim_date.parquet"
    try:
        df_date_old = wr.s3.read_parquet(date_path)
        df_date = pd.concat([df_date_old, df_date]).drop_duplicates(subset=['date'], keep='last')
    except: pass
    wr.s3.to_parquet(df=df_date, path=date_path, index=False)

    # ==========================================
    # 5. fact_sales
    # ==========================================
    print(f"   [6/6] Modelando fact_sales...", flush=True)
    df_facts = df_new.copy()
    df_facts.rename(columns={'supplier_code': 'customer_code'}, inplace=True)
    df_facts = df_facts.drop(columns=['group', 'description', 'supplier_name'], errors='ignore')

    # NUEVO: Guardando dentro de la carpeta /fact_sales/
    fact_path = f"s3://{bucket}/{GOLD_PREFIX}fact_sales/fact_sales.parquet"
    try:
        df_facts_old = wr.s3.read_parquet(fact_path)
        df_facts = pd.concat([df_facts_old, df_facts]).drop_duplicates()
    except: pass
    wr.s3.to_parquet(df=df_facts, path=fact_path, index=False)


def lambda_handler(event, context):
    bucket = BUCKET
    pending_files = list_pending_files(bucket)

    if not pending_files:
        return {"statusCode": 200, "body": json.dumps("Sin archivos nuevos en Silver.")}

    print(f"Archivos Silver pendientes: {len(pending_files)}", flush=True)
    resultados = {"exitosos": [], "fallidos": []}

    for key in pending_files:
        try:
            process_silver_to_gold(bucket, key)
            move_to_processed(bucket, key)
            resultados["exitosos"].append(key)
        except Exception as e:
            print(f"    ERROR procesando {key}: {e}", flush=True)
            resultados["fallidos"].append({"archivo": key, "error": str(e)})

    status_code = 200 if not resultados["fallidos"] else 207 
    return {"statusCode": status_code, "body": json.dumps(resultados)}