import os
import sys
import pandas as pd
from datetime import datetime
import configparser
import psycopg2
from sqlalchemy import create_engine
from psycopg2.extras import execute_values

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(BASE_DIR)

RAW_DATA_DIR = os.path.join(BASE_DIR, "data")
CLEAN_DATA_DIR = os.path.join(BASE_DIR, "DataStaging")

os.makedirs(CLEAN_DATA_DIR, exist_ok=True)

CONFIG_FILE_PATH = os.path.join(BASE_DIR, 'config.ini')

def load_config():
    if not os.path.exists(CONFIG_FILE_PATH):
        print(f"LỖI: Không tìm thấy file '{CONFIG_FILE_PATH}'", file=sys.stderr)
        sys.exit(1)
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE_PATH)
    if 'database' not in config:
        print(f"LỖI: File 'config.ini' phải có mục [database]", file=sys.stderr)
        sys.exit(1)
    
    return dict(config['database'])

db_settings = load_config()

def get_db_config(db_name):
    settings = db_settings.copy()
    settings['dbname'] = db_name
    return settings

def get_control_connection():
    try:
        conn_settings = get_db_config('db_control')
        conn = psycopg2.connect(**conn_settings)
        conn.autocommit = False
        return conn
    except Exception as e:
        print(f"LỖI KẾT NỐI db_control: {e}", file=sys.stderr)
        raise

def get_staging_engine():
    try:
        conn_settings = get_db_config('db_staging')
        connection_string = (
            f"postgresql+psycopg2://{conn_settings['user']}:{conn_settings['password']}"
            f"@{conn_settings['host']}:{conn_settings['port']}/{conn_settings['dbname']}"
        )
        engine = create_engine(connection_string)
        return engine
    except Exception as e:
        print(f"LỖI KẾT NỐI db_staging (engine): {e}", file=sys.stderr)
        raise

def transform_chunk(chunk_df):
    numeric_cols = ['longitude', 'latitude', 'baro_altitude', 'velocity', 
                    'true_track', 'vertical_rate', 'geo_altitude']
    for col in numeric_cols:
        chunk_df[col] = pd.to_numeric(chunk_df[col], errors='coerce')
    
    chunk_df['position_source'] = pd.to_numeric(chunk_df['position_source'], errors='coerce').astype('Int64')

    chunk_df['time_position'] = pd.to_datetime(chunk_df['time_position'], unit='s', errors='coerce')
    chunk_df['last_contact'] = pd.to_datetime(chunk_df['last_contact'], unit='s', errors='coerce')

    bool_map = {'true': True, 'false': False}
    chunk_df['on_ground'] = chunk_df['on_ground'].astype(str).str.lower().map(bool_map)
    chunk_df['spi'] = chunk_df['spi'].astype(str).str.lower().map(bool_map)
    
    final_columns = [
        'load_timestamp', 'file_source', 'icao24', 'callsign', 'origin_country',
        'time_position', 'last_contact', 
        'longitude', 'latitude', 'baro_altitude', 
        'on_ground', 
        'velocity', 'true_track', 'vertical_rate',
        'sensors', 'geo_altitude', 'squawk', 
        'spi', 'position_source'
    ]
    
    for col in final_columns:
        if col not in chunk_df.columns:
            chunk_df[col] = None
            
    return chunk_df[final_columns]

def get_processed_files(conn):
    processed_files = set()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT file_name FROM file_log WHERE status != 'NEW'")
            for row in cursor.fetchall():
                processed_files.add(row[0])
    except Exception as e:
        print(f"LỖI đọc file_log: {e}")
    return processed_files

def register_new_files(conn, new_files):
    if not new_files: return
    print(f"Đang đăng ký {len(new_files)} file mới...")
    try:
        with conn.cursor() as cursor:
            args_list = [(f,) for f in new_files]
            sql = "INSERT INTO file_log (file_name) VALUES %s ON CONFLICT (file_name) DO NOTHING;"
            execute_values(cursor, sql, args_list)
        conn.commit()
    except Exception as e:
        print(f"LỖI đăng ký file: {e}")
        conn.rollback()

def update_file_log(conn, file_name, status, row_count=None, msg=None):
    try:
        with conn.cursor() as cursor:
            sql = """
            UPDATE file_log SET 
                status = %s, row_count = %s, error_message = %s, last_updated = %s 
            WHERE file_name = %s
            """
            cursor.execute(sql, (status, row_count, msg, datetime.now(), file_name))
        conn.commit()
    except Exception as e:
        print(f"LỖI cập nhật log: {e}")
        conn.rollback()

def process_single_file(file_name, staging_engine):
    input_path = os.path.join(RAW_DATA_DIR, file_name)
    output_path = os.path.join(CLEAN_DATA_DIR, f"clean_{file_name}")
    
    print(f"Đang xử lý: {file_name}")

    chunk_size = 100000
    total_rows = 0
    is_first_chunk = True
    
    if os.path.exists(output_path):
        os.remove(output_path)

    with pd.read_csv(input_path, chunksize=chunk_size) as reader:
        for chunk in reader:
            if chunk.empty: continue
            
            chunk['load_timestamp'] = datetime.now()
            chunk['file_source'] = file_name
            
            chunk.to_sql('raw_flight_states', staging_engine, if_exists='append', index=False)
            
            clean_chunk = transform_chunk(chunk)
            
            clean_chunk.to_csv(output_path, mode='a', index=False, header=is_first_chunk)
            
            total_rows += len(chunk)
            if is_first_chunk:
                is_first_chunk = False
                
    return total_rows

def main():
    print("--- Bắt đầu Quy trình ---")
    
    control_conn = None
    staging_engine = None
    
    try:
        control_conn = get_control_connection()
        staging_engine = get_staging_engine()
        
        try:
            all_files = os.listdir(RAW_DATA_DIR)
            csv_files = {f for f in all_files if f.startswith('states_') and f.endswith('.csv')}
        except FileNotFoundError:
            print(f"LỖI: Không tìm thấy thư mục {RAW_DATA_DIR}")
            return

        if not csv_files:
            print("Không có file CSV nào.")
            return

        processed = get_processed_files(control_conn)
        new_files = list(csv_files - processed)
        
        register_new_files(control_conn, new_files)
        
        files_to_run = []
        with control_conn.cursor() as cur:
            cur.execute("SELECT file_name FROM file_log WHERE status = 'NEW'")
            files_to_run = [r[0] for r in cur.fetchall()]
            
        print(f"Tìm thấy {len(files_to_run)} file cần xử lý.")
        
        for fname in files_to_run:
            try:
                update_file_log(control_conn, fname, 'PROCESSING')
                
                rows = process_single_file(fname, staging_engine)
                
                update_file_log(control_conn, fname, 'CLEAN_EXPORTED', row_count=rows)
                print(f"✅ Hoàn tất file {fname}. Tổng dòng: {rows}")
                
            except Exception as e:
                print(f"❌ LỖI file {fname}: {e}")
                update_file_log(control_conn, fname, 'FAILED', msg=str(e))

    except Exception as e:
        print(f"Lỗi hệ thống: {e}")
    finally:
        if control_conn: control_conn.close()
        if staging_engine: staging_engine.dispose()

    print("--- Kết thúc quy trình ---")

if __name__ == "__main__":
    main()