"""Run one complete bounded MySQL -> MinIO Bronze/Silver/Gold batch, then commit watermark.
Dependencies on Windows: pip install pymysql minio. Existing Docker image ecommerce-spark:local.
"""
import json
import os
import subprocess
import sys
import uuid
from datetime import datetime, date
from decimal import Decimal
from getpass import getpass
from io import BytesIO
from pathlib import Path

import pymysql
from pymysql.cursors import DictCursor
from minio import Minio
from minio.error import S3Error

BASE = Path(__file__).resolve().parent.parent
RUN_ROOT = BASE / 'data' / 'auto_runs'
BUCKET = 'ecommerce-batch-lake'
PIPELINE = 'ecommerce_orders'
IMAGE = 'ecommerce-spark:local'

def secret(name, prompt):
    val = os.getenv(name)
    return val if val else getpass(prompt)

def convert(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f'Cannot encode {type(value)}')

def download_snapshot(client, dst):
    try:
        response = client.get_object(BUCKET, 'silver/orders/current.json')
        try:
            pointer = json.loads(response.read().decode('utf8'))
        finally:
            response.close(); response.release_conn()
        prefix = pointer['prefix']
        objects = [obj for obj in client.list_objects(BUCKET, prefix=prefix, recursive=True)
                   if obj.object_name.endswith('.parquet')]
        if not objects:
            raise RuntimeError('MinIO Silver pointer exists but no Parquet objects found')
        dst.mkdir(parents=True, exist_ok=True)
        for obj in objects:
            client.fget_object(BUCKET, obj.object_name, str(dst / Path(obj.object_name).name))
        print('Previous Silver from MinIO:', prefix)
    except S3Error as exc:
        if exc.code != 'NoSuchKey':
            raise
        # Legacy initial snapshot; only use if MinIO has not yet published a current snapshot.
        legacy = BASE / 'data' / 'parquet' / 'silver_orders'
        if legacy.exists() and list(legacy.glob('*.parquet')):
            dst.mkdir(parents=True, exist_ok=True)
            for src in legacy.glob('*.parquet'):
                import shutil
                shutil.copy2(src, dst / src.name)
            print('Previous Silver from initial local snapshot')
        else:
            print('No previous Silver; building initial snapshot')

def upload_folder(client, directory, prefix):
    parts = sorted(directory.rglob('*.parquet'))
    if not parts:
        raise RuntimeError(f'No Parquet files at {directory}')
    for file in parts:
        rel = file.relative_to(directory).as_posix()
        key = prefix + rel
        client.fput_object(BUCKET, key, str(file), content_type='application/octet-stream')
        result = client.stat_object(BUCKET, key)
        if result.size != file.stat().st_size:
            raise RuntimeError(f'Upload size mismatch: {key}')
    return len(parts)

def write_json(client, key, obj):
    data = json.dumps(obj, default=convert, indent=2).encode('utf8')
    client.put_object(BUCKET, key, BytesIO(data), len(data), content_type='application/json')

def main():
    os.chdir(BASE)
    pw = secret('MYSQL_PASSWORD', 'MySQL app password: ')
    minio_user = os.getenv('MINIO_ROOT_USER', 'batchadmin')
    minio_pwd = secret('MINIO_ROOT_PASSWORD', 'MinIO password: ')
    client = Minio('127.0.0.1:9100', access_key=minio_user, secret_key=minio_pwd, secure=False)
    if not client.bucket_exists(BUCKET):
        client.make_bucket(BUCKET)
    conn = pymysql.connect(host='127.0.0.1', port=3306, user='ecommerce_app',
        password=pw, database='ecommerce_api_db', cursorclass=DictCursor,
        autocommit=False, charset='utf8mb4')
    run_id = datetime.now().strftime('%Y%m%d_%H%M%S') + '_' + uuid.uuid4().hex[:8]
    run = RUN_ROOT / run_id
    run.mkdir(parents=True, exist_ok=False)
    count = 0
    last = None
    old = None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT last_modified_at, last_order_id FROM pipeline_watermark WHERE pipeline_name=%s", (PIPELINE,))
            old = cur.fetchone()
            if old is None:
                raise RuntimeError('Missing production watermark')
            # Read one consistent source snapshot; the 2-second buffer avoids boundary races.
            cur.execute('SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ')
            cur.execute('START TRANSACTION WITH CONSISTENT SNAPSHOT')
            cur.execute('SELECT DATE_SUB(CURRENT_TIMESTAMP(6), INTERVAL 2 SECOND) AS upper_time')
            upper_time = cur.fetchone()['upper_time']
            cur.execute('''SELECT * FROM sales_orders WHERE
              (last_modified_at > %s OR (last_modified_at = %s AND OrderID > %s))
              AND last_modified_at < %s
              ORDER BY last_modified_at ASC, OrderID ASC''',
              (old['last_modified_at'],old['last_modified_at'],old['last_order_id'],upper_time))
            with (run / 'source.jsonl').open('w', encoding='utf8') as f:
                while True:
                    batch = cur.fetchmany(500)
                    if not batch: break
                    for row in batch:
                        f.write(json.dumps(row, default=convert, ensure_ascii=False) + '\n')
                        count += 1
                        last = (row['last_modified_at'], row['OrderID'])
        conn.rollback()  # close read-only snapshot, no watermark changes
        print('Extracted:', count, 'upper_time:', upper_time, 'run_id:', run_id)
        if not count:
            print('NO CHANGES: Nothing uploaded, watermark unchanged.')
            return

        download_snapshot(client, run / 'previous_silver')
        command = ['docker','run','--rm','--entrypoint','python',
                   '-v',f'{BASE}:/app',IMAGE,'/app/scripts/spark_batch.py',run_id]
        print('Running PySpark Bronze + Silver + Gold...')
        subprocess.run(command, cwd=BASE, check=True)

        upload_folder(client, run / 'bronze', f'bronze/orders/batch_id={run_id}/')
        silver_prefix = f'silver/orders/snapshot_id={run_id}/'
        upload_folder(client, run / 'silver', silver_prefix)
        for name in ('daily_sales','category_sales','customer_summary'):
            upload_folder(client, run / 'gold' / name, f'gold/{name}/snapshot_id={run_id}/')
        manifest = {'run_id':run_id, 'silver_prefix':silver_prefix,
                    'new_watermark':{'last_modified_at':convert(last[0]),'last_order_id':last[1]},
                    'gold_prefix':f'gold/', 'source_count':count}
        write_json(client, f'manifests/{run_id}.json', manifest)
        # Pointer before watermark: if DB commit fails, next run safely replays old changes.
        write_json(client, 'silver/orders/current.json', {'prefix':silver_prefix, 'run_id':run_id})
        for name in ('daily_sales','category_sales','customer_summary'):
            write_json(client, f'gold/{name}/current.json',
                {'prefix':f'gold/{name}/snapshot_id={run_id}/', 'run_id':run_id})
        # CAS update prevents concurrent workers from advancing a changed watermark.
        with conn.cursor() as cur:
            cur.execute('''UPDATE pipeline_watermark
              SET last_modified_at=%s, last_order_id=%s
              WHERE pipeline_name=%s AND last_modified_at=%s AND last_order_id=%s''',
              (last[0],last[1],PIPELINE,old['last_modified_at'],old['last_order_id']))
            if cur.rowcount != 1:
                raise RuntimeError('Watermark changed concurrently; rollback and replay required')
        conn.commit()
        print('SUCCESS: Bronze, Silver, Gold published to MinIO.')
        print('WATERMARK COMMITTED:',last[0],last[1])
        print('MinIO console: http://127.0.0.1:9101')
    except Exception:
        conn.rollback()
        print('FAILED: production watermark not advanced. See error above.', file=sys.stderr)
        raise
    finally:
        conn.close()

if __name__ == '__main__':
    main()
