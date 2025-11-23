import sys
import requests
import json
import csv
import psycopg2  
import configparser # Thư viện đọc config
import os
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE_PATH = os.path.join(BASE_DIR, 'config.ini')

def load_config():
    """
    Đọc file config.ini 
    """
    if not os.path.exists(CONFIG_FILE_PATH):
        print(f"LỖI: Không tìm thấy file '{CONFIG_FILE_PATH}'", file=sys.stderr)
        print(f"Hãy sao chép 'config.ini.template' thành 'config.ini' và điền thông tin CSDL.", file=sys.stderr)
        sys.exit(1)
        
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE_PATH)
    
    if 'database' not in config:
        print(f"LỖI: File 'config.ini' phải có mục [database]", file=sys.stderr)
        sys.exit(1)
    
    # [FIX QUAN TRỌNG]: Ép kiểu SectionProxy sang Dictionary chuẩn của Python
    # Điều này giúp tránh lỗi "'SectionProxy' object has no attribute 'copy'"
    return dict(config['database'])

# Đọc config MỘT LẦN DUY NHẤT
db_settings = load_config()

# (MỚI) HÀM KẾT NỐI (Tự chứa trong script)
def get_db_config(db_name):
    """
    Lấy thông tin config chung và thêm 'dbname' cụ thể.
    """
    # Bây giờ db_settings là dict thật, nên lệnh .copy() sẽ hoạt động tốt
    settings = db_settings.copy()
    settings['dbname'] = db_name
    return settings

def get_control_connection():
    """
    Kết nối đến db_control (dùng psycopg2 chuẩn)
    """
    try:
        conn_settings = get_db_config('db_control')
        conn = psycopg2.connect(**conn_settings)
        conn.autocommit = False # Tự quản lý transaction
        return conn
    except Exception as e:
        print(f"LỖI KẾT NỐI db_control: {e}", file=sys.stderr)
        raise # Ném lỗi ra ngoài để dừng chương trình

# -------------------------------------------------------------------
# HÀM GỌI API (Nhận URL từ CSDL)
# -------------------------------------------------------------------

def get_system_config(conn, config_key):
    """
    Đọc một cấu hình cụ thể từ bảng 'configuration'
    """
    try:
        with conn.cursor() as cursor:
            sql = "SELECT config_value FROM configuration WHERE config_key = %s"
            cursor.execute(sql, (config_key,))
            row = cursor.fetchone()
            if row:
                return row[0]
            else:
                raise Exception(f"Không tìm thấy config_key '{config_key}' trong db_control.configuration.")
    except Exception as e:
        print(f"LỖI khi đọc bảng configuration: {e}", file=sys.stderr)
        raise e

def get_access_token(token_url, client_id, client_secret):
    """
    Lấy Access Token (OAuth2) từ OpenSky.
    """
    print(f"Đang lấy Access Token từ: {token_url}...")
    try:
        data = {
            'grant_type': 'client_credentials',
            'client_id': client_id,
            'client_secret': client_secret
        }
        # POST request để lấy token
        response = requests.post(token_url, data=data, headers={
            'Content-Type': 'application/x-www-form-urlencoded'
        })
        
        response.raise_for_status() # Tự động báo lỗi nếu là 4xx hoặc 5xx
        
        token_data = response.json()
        print("Lấy Access Token thành công.")
        return token_data['access_token']
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            print("LỖI 401 KHI LẤY TOKEN! Vui lòng kiểm tra lại client_id/client_secret trong db_control.", file=sys.stderr)
        else:
            print(f"LỖI HTTP KHI LẤY TOKEN: {e}", file=sys.stderr)
        raise e # Ném lỗi ra ngoài

def call_opensky_api(config, access_token):
    """
    Gọi API OpenSky (dùng Token)
    """
    # Xây dựng URL động từ config (đọc từ CSDL)
    url = f"{config['base_url']}{config['endpoint']}"
    params = {
        'lamin': config['lamin'], 'lomin': config['lomin'],
        'lamax': config['lamax'], 'lomax': config['lomax']
    }
    
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.json()

# -------------------------------------------------------------------
# CÁC HÀM XỬ LÝ
# -------------------------------------------------------------------

def get_job_config(conn, job_name):
    """
    Đọc cấu hình từ db_control.job_definitions
    """
    print(f"Đang đọc cấu hình job '{job_name}' từ CSDL...")
    config = {}
    try:
        with conn.cursor() as cursor:
            # Lấy TẤT CẢ các cột
            sql = "SELECT * FROM job_definitions WHERE job_name = %s"
            cursor.execute(sql, (job_name,))
            
            # Lấy tên cột để map vào dict
            if cursor.description:
                colnames = [desc[0] for desc in cursor.description]
                row = cursor.fetchone()
                
                if row:
                    config = dict(zip(colnames, row))
                    print(f"Đã đọc cấu hình cho job: {job_name}")
                    return config
                else:
                    raise Exception(f"Không tìm thấy job_name '{job_name}' trong job_definitions.")
            else:
                 raise Exception("Cursor không có description (Lỗi lạ).")

    except Exception as e:
        print(f"LỖI khi đọc job_config: {e}", file=sys.stderr)
        raise e

def log_job_start(conn, job_name):
    """
    Ghi log "STARTED" vào db_control.job_logs
    """
    print(f"Ghi log: Job '{job_name}' -> STARTED")
    log_id = -1
    try:
        with conn.cursor() as cursor:
            sql = "INSERT INTO job_logs (job_name, start_time, status) VALUES (%s, %s, %s) RETURNING log_id"
            cursor.execute(sql, (job_name, datetime.now(), "STARTED"))
            log_id = cursor.fetchone()[0]
        conn.commit()
        return log_id
    except Exception as e:
        print(f"LỖI khi ghi log bắt đầu: {e}", file=sys.stderr)
        conn.rollback()
        raise e

def log_job_end(conn, log_id, status, message=None):
    """
    Cập nhật log "COMPLETED" hoặc "FAILED"
    """
    print(f"Ghi log: Job (ID: {log_id}) -> {status}")
    try:
        with conn.cursor() as cursor:
            sql = "UPDATE job_logs SET end_time = %s, status = %s, message = %s WHERE log_id = %s"
            # Cắt bớt message nếu quá dài
            final_message = (message[:500] if message else None)
            cursor.execute(sql, (datetime.now(), status, final_message, log_id))
        conn.commit()
    except Exception as e:
        print(f"LỖI khi ghi log kết thúc: {e}", file=sys.stderr)
        conn.rollback()

def save_data_to_csv(json_response, output_dir, job_name):
    """
    Lưu phản hồi JSON (từ API) vào file CSV
    """
    if not json_response or 'states' not in json_response:
        print("Cảnh báo: Phản hồi API không chứa 'states' hoặc bị rỗng. Không tạo file CSV.")
        return None # Trả về None nếu không có dữ liệu

    states_array = json_response.get('states', [])
    if not states_array:
        print("Thông tin: API trả về mảng 'states' rỗng. Không tạo file CSV.")
        return None

    # Tạo tên file duy nhất
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"states_{job_name}_{timestamp}.csv"
    
    # Kiểm tra và tạo thư mục nếu chưa tồn tại
    if not os.path.exists(output_dir):
        try:
            os.makedirs(output_dir)
        except OSError as e:
             print(f"LỖI: Không thể tạo thư mục '{output_dir}': {e}", file=sys.stderr)
             raise e

    # Tạo đường dẫn an toàn (cross-platform)
    file_path = os.path.join(output_dir, file_name)
    
    print(f"Đang lưu {len(states_array)} dòng vào: {file_path}...")
    
    # Định nghĩa header cứng cho OpenSky States API
    csv_header = [
        "icao24", "callsign", "origin_country", "time_position", "last_contact",
        "longitude", "latitude", "baro_altitude", "on_ground", "velocity",
        "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk",
        "spi", "position_source"
    ]
    
    try:
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(csv_header)
            writer.writerows(states_array) # Ghi tất cả các dòng (mảng của mảng)
            
        return file_path
    except Exception as e:
        print(f"LỖI khi ghi file CSV '{file_path}': {e}", file=sys.stderr)
        raise e

# -------------------------------------------------------------------
# HÀM MAIN
# -------------------------------------------------------------------
def main():
    if len(sys.argv) < 2:
        print("Lỗi: Vui lòng cung cấp job_name (ví dụ: python script.py crawl_europe_live_data)", file=sys.stderr)
        sys.exit(1)
        
    job_name = sys.argv[1]
    print(f"--- Bắt đầu Bước 1: Extract Job '{job_name}' ---")

    conn = None
    log_id = -1
    
    try:
        # 1. Kết nối CSDL
        conn = get_control_connection()

        # 2. Đọc cấu hình (Gộp cả 2 bảng)
        # 2a. Đọc cấu hình job (client_id, client_secret, output_path...)
        job_config = get_job_config(conn, job_name)
        
        # 2b. Đọc cấu hình hệ thống (token_url)
        token_url = get_system_config(conn, 'opensky_token_url')
        
        # 3. Ghi log "bắt đầu"
        log_id = log_job_start(conn, job_name)

        # 4. Lấy Access Token (OAuth2)
        access_token = get_access_token(token_url, job_config['client_id'], job_config['client_secret'])

        # 5. Gọi API OpenSky
        api_response_json = call_opensky_api(job_config, access_token)

        # 6. Lưu dữ liệu thô vào file CSV
        csv_file_path = save_data_to_csv(api_response_json, job_config['output_path'], job_name)
        
        if csv_file_path:
             # 7a. Cập nhật log "hoàn thành"
            log_job_end(conn, log_id, "COMPLETED", f"Tải thành công: {csv_file_path}")
        else:
             # 7b. Hoàn thành nhưng không có data
             log_job_end(conn, log_id, "COMPLETED", "Hoàn thành (Không có dữ liệu mới).")

    except Exception as e:
        print(f"LỖI NGHIÊM TRỌNG trong job '{job_name}': {e}", file=sys.stderr)
        if conn and log_id != -1:
            # 7c. Cập nhật log "thất bại"
            log_job_end(conn, log_id, "FAILED", str(e))
            
    finally:
        if conn:
            conn.close()
            
    print(f"--- Job '{job_name}' đã chạy xong ---")

if __name__ == "__main__":
    main()