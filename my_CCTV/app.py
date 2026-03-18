import sys
import cv2              # OpenCV 라이브러리 (카메라 영상 처리 및 QR 인식용)
import time             # 시간 지연(sleep) 등을 위해 사용
import threading        # 멀티스레딩 (웹서버, 카메라, ROS를 동시에 돌리기 위함)
import base64           # 이미지를 문자열로 변환하여 웹으로 전송하기 위함
import pymysql          # MySQL 데이터베이스 연동 라이브러리
import numpy as np      # 수치 계산용 라이브러리 (이미지 배열 처리 등)
from datetime import datetime # 날짜 및 요일 계산용

# ROS 2 관련 라이브러리 임포트
import rclpy
from rclpy.node import Node
from std_msgs.msg import String

# Flask(웹 프레임워크) 관련 임포트
from flask import Flask, render_template
from flask_socketio import SocketIO, emit
from pyzbar.pyzbar import decode  # QR 코드 디코딩 라이브러리

# Flask 앱 객체 생성
app = Flask(__name__)
# SocketIO 설정 (비동기 모드를 threading으로 설정하여 ROS/CV와 호환성 유지)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# --- 전역 변수 설정 (여러 스레드에서 공유하는 데이터) ---
ros_node = None         # ROS 2 노드 객체를 담을 변수
last_qr_data = None     # 가장 최근에 인식된 QR 데이터를 저장 (중복 인식 방지)
is_processing = False   # 로봇이 현재 작업 중인지 확인하는 플래그 (True면 QR 인식 무시)
is_edit_mode = False    # 관리자 수정 모드 활성화 여부 (True면 로봇 이동 안 함)

# =========================================================
# 1. Database Logic (데이터베이스 처리 함수들)
# =========================================================

# DB 연결 객체를 생성하여 반환하는 함수
def get_db_connection():
    return pymysql.connect(
        host='127.0.0.1',       # DB 서버 주소 (로컬호스트)
        user='rokey',           # DB 사용자 ID
        password='1234',        # DB 비밀번호
        db='hospital_db',       # 사용할 데이터베이스 이름
        charset='utf8'          # 한글 처리를 위한 인코딩 설정
    )

# [DB 수정] 특정 QR과 요일에 해당하는 약/수액/물품 정보를 업데이트
def update_db_schedule(qr_code, day, pill, liquid, supply):
    try:
        conn = get_db_connection()  # DB 연결
        cursor = conn.cursor()
        # SQL 업데이트 쿼리 실행
        sql = """UPDATE daily_schedule 
                 SET pill_type=%s, liquid_type=%s, supply_type=%s 
                 WHERE qr_code=%s AND day_of_week=%s"""
        cursor.execute(sql, (pill, liquid, supply, qr_code, day))
        conn.commit()   # 변경사항 저장
        conn.close()    # 연결 종료
        return True     # 성공 시 True 반환
    except Exception as e:
        print(f"❌ [DB ERROR] {e}") # 에러 로그 출력
        return False    # 실패 시 False 반환

# [DB 조회] 특정 요일의 처방 데이터를 가져오는 함수 (수정 팝업창 띄울 때 사용)
def get_data_by_day(qr_code, target_day):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        # 해당 QR과 요일에 맞는 처방 정보 조회
        sql = "SELECT pill_type, liquid_type, supply_type FROM daily_schedule WHERE qr_code=%s AND day_of_week=%s"
        cursor.execute(sql, (qr_code, target_day))
        row = cursor.fetchone() # 결과 1줄 가져오기
        conn.close()
        if row:
            # 데이터가 있으면 딕셔너리 형태로 반환
            return {"pill": row[0], "liquid": row[1], "supply": row[2]}
    except: pass
    # 데이터가 없거나 에러 발생 시 기본값 반환
    return {"pill": "-", "liquid": "-", "supply": "-"}

# [DB 조회] 해당 환자의 일주일치 스케줄 리스트 가져오기 (메인 화면 표시용)
def get_weekly_list(qr_code):
    data_list = []
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        # 요일 순서대로 정렬하여 데이터 조회 (ORDER BY FIELD 사용)
        sql = """SELECT day_of_week, pill_type, liquid_type, supply_type FROM daily_schedule 
                 WHERE qr_code = %s 
                 ORDER BY FIELD(day_of_week, 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun')"""
        cursor.execute(sql, (qr_code,))
        
        # 조회된 모든 행(요일별 데이터)을 리스트에 담기
        for row in cursor.fetchall():
            data_list.append({
                "day": row[0], 
                "pill": row[1] if row[1]!='None' else '-', # 데이터가 문자열 'None'이면 '-'로 표시
                "liquid": row[2] if row[2]!='None' else '-',
                "supply": row[3] if row[3]!= 'None' else '-'
            })
        conn.close()
    except: pass
    return data_list

# [DB 조회] 오늘 날짜(요일)에 해당하는 처방 정보 조회 (로봇 작업용)
def get_todays_prescription(qr_code):
    days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    today_str = days[datetime.today().weekday()] # 오늘 요일 구하기 (예: 'Mon')
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        # 환자 정보(patients)와 스케줄(daily_schedule)을 조인하여 이름, 병명, 오늘 처방약 조회
        sql = """SELECT p.name, p.disease, s.pill_type, s.liquid_type, s.supply_type 
                 FROM daily_schedule s 
                 JOIN patients p ON s.qr_code = p.qr_code 
                 WHERE s.qr_code = %s AND s.day_of_week = %s"""
        cursor.execute(sql, (qr_code, today_str))
        row = cursor.fetchone()
        conn.close()
        
        if row: 
            # 조회 성공 시 정보 반환
            return {
                "found": True, "qr_code": qr_code, 
                "name": row[0], "disease": row[1], "day": today_str, 
                "pill": row[2], "liquid": row[3], "supply": row[4]
            }
    except: pass
    return None # 조회 실패 시 None

# =========================================================
# 2. ROS 2 Node & Socket Logic (로봇 통신 및 웹 소켓)
# =========================================================

# ROS 2 노드 클래스 정의
class WebBridgeNode(Node):
    def __init__(self):
        # 노드 이름: web_bridge_node, 네임스페이스: dsr01 설정
        super().__init__('web_bridge_node', namespace='dsr01')
        # 퍼블리셔 생성: 'robot_cmd' 토픽으로 문자열 명령을 보냄
        self.publisher_ = self.create_publisher(String, 'robot_cmd', 10)
        # 서브스크라이버 생성: 'monitor_cmd' 토픽으로 로봇 상태를 받음
        self.sub_monitor = self.create_subscription(String, 'monitor_cmd', self.monitor_callback, 50)

    # 로봇에게서 메시지를 받았을 때 실행되는 콜백 함수
    def monitor_callback(self, msg):
        text = msg.data
        print(f"📥 [MONITOR] {text}") # 터미널에 로그 출력
        # 웹 클라이언트에 로그 전송 (화면에 표시하기 위함)
        socketio.emit('ros_log', {'msg' : f"[MONITOR] {text}"})
        
        # 만약 로봇이 "finish" 메시지를 보내면 도착 완료 팝업 띄움
        if text.strip() == "finish":
            socketio.emit('popup_open', {'msg': '도착 완료'})

    # 로봇에게 명령을 보내는 함수
    def send_command(self, data):
        if rclpy.ok(): # ROS가 정상 작동 중이면
            msg = String()
            msg.data = str(data) # 데이터를 문자열로 변환
            self.publisher_.publish(msg) # 토픽 발행
            print(f"📤 [CMD] {msg.data}")

# --- 웹 소켓 이벤트 핸들러 (웹페이지 JS와 통신) ---

# [1] 수정 모드 시작 (웹에서 버튼 클릭 시)
@socketio.on('start_edit_mode')
def handle_start_edit():
    global is_edit_mode, last_qr_data
    is_edit_mode = True     # 수정 모드 켬
    last_qr_data = None     # 모드 전환 시 QR 재인식을 위해 초기화
    print("✏️ [MODE] 수정 모드 ON (로봇 정지)")

# [1-2] 수정 모드 종료
@socketio.on('stop_edit_mode')
def handle_stop_edit():
    global is_edit_mode, last_qr_data
    is_edit_mode = False    # 일반 모드로 복귀
    last_qr_data = None
    print("✅ [MODE] 수정 모드 OFF (일반 모드)")

# [2] 수정 팝업에서 특정 요일을 선택했을 때 데이터 요청 처리
@socketio.on('req_change_day')
def handle_change_day(data):
    qr = data['qr']
    target_day = data['day']
    # DB에서 해당 요일 데이터 조회
    result = get_data_by_day(qr, target_day)
    # 결과를 웹으로 전송 (팝업 내용 갱신)
    socketio.emit('res_day_data', result)

# [3] 웹에서 수정 사항 저장 요청 시 처리
@socketio.on('req_edit_info')
def handle_edit_request(data):
    print(f"✏️ [EDIT] {data}")
    
    # 1. DB 업데이트 함수 호출
    success = update_db_schedule(
        data['qr'], data['day'], data['pill'], data['liquid'], data['supply']
    )
    
    # 2. 성공 여부에 따라 로그 메시지를 웹으로 전송
    if success:
        socketio.emit('new_log', {'log': f"✅ [DB] {data['day']}요일 데이터 저장 완료"})
    else:
        socketio.emit('new_log', {'log': "❌ DB 수정 실패"})

# [4] 시스템 리셋 (사용자가 약 수령 확인 버튼을 눌렀을 때)
@socketio.on('system_reset')
def handle_reset():
    global is_processing, last_qr_data 
    is_processing = False   # 로봇 잠금 해제 (다음 사람을 위해)
    last_qr_data = None     # QR 기록 초기화
    print("🔓 [SYSTEM] 수령 확인 -> 초기화")
    socketio.emit('new_log', {'log': "[SYSTEM] 수령 완료 -> 초기화"})
    # ★ 웹 화면을 초기 상태로 되돌리는 신호 전송
    socketio.emit('clear_ui')

# [5] 강제 리셋 (오류 발생 등으로 인한 관리자 리셋)
@socketio.on('force_reset')
def handle_force_reset():
    global is_processing, last_qr_data
    is_processing = False
    last_qr_data = None
    print("🔄 [SYSTEM] 강제 초기화")
    socketio.emit('new_log', {'log': "⚠️ [RESET] 시스템 강제 초기화"})
    socketio.emit('clear_ui')

# --- QR 처리 핵심 로직 ---
def process_qr_job(qr_data):
    global is_processing, is_edit_mode
    
    # [A] 수정 모드일 때: 로봇은 움직이지 않고, 화면에 정보만 띄움
    if is_edit_mode:
        print(f"🔍 [EDIT MODE] QR: {qr_data}")
        p_info = get_todays_prescription(qr_data) # 환자 정보 조회
        if p_info:
            socketio.emit('search_result', p_info) # 웹에 검색 결과 전송
            socketio.emit('new_log', {'log': f"[EDIT] {p_info['name']}님 정보 로드"})
        else:
            socketio.emit('new_log', {'log': "⚠️ 미등록 QR (수정 불가)"})
        return  # ★ 중요: 로봇 명령 전송 부분으로 가지 않고 함수 종료

    # [B] 일반 모드일 때: 로봇 조제 시작
    # 이미 로봇이 다른 작업을 처리 중이라면 무시
    if is_processing: return 

    print(f"\n🎯 QR Detected: {qr_data}")
    p_info = get_todays_prescription(qr_data) # 오늘 처방 정보 조회
    
    if p_info:
        is_processing = True # 작업 시작 (다른 QR 인식 차단)
        # 주간 일정 리스트도 함께 가져옴
        p_info['weekly_schedule'] = get_weekly_list(qr_data)
        
        # 웹 화면에 환자 정보 및 스케줄 업데이트
        socketio.emit('update_data', p_info)
        socketio.emit('new_log', {'log': f"[SYSTEM] {p_info['name']}님 조제 시작"})
        
        # ROS 노드를 통해 로봇에게 QR 코드(또는 명령) 전송
        if ros_node: 
            ros_node.send_command(qr_data)
    else:
        # DB에 없는 QR일 경우
        socketio.emit('new_log', {'log': "⚠️ 미등록 QR"})

# =========================================================
# 3. Camera Threads & Main (카메라 스레드 및 메인 실행)
# =========================================================

# 1번 카메라 스레드 (QR 인식 및 메인 화면용)
def qr_camera_thread():
    global last_qr_data, is_processing, is_edit_mode
    cap = cv2.VideoCapture(0) # 0번 카메라 연결
    frame_count = 0
    while True:
        ret, frame = cap.read() # 프레임 읽기
        if not ret: 
            time.sleep(0.1)
            continue
        
        # 영상 크기 줄이기 (전송 속도 최적화)
        frame = cv2.resize(frame, (320, 240))
        frame_count += 1
        
        # 성능을 위해 3프레임마다 1번씩만 QR 검사 수행
        # 조건: (일반모드이면서 처리중이 아닐 때) OR (수정모드일 때)
        if (not is_processing or is_edit_mode) and frame_count % 3 == 0:
            decoded_objects = decode(frame) # QR 코드 찾기
            for obj in decoded_objects:
                qr_data = obj.data.decode('utf-8').strip()
                # 새로운 QR 코드가 인식되면
                if qr_data != last_qr_data:
                    last_qr_data = qr_data
                    # 별도 스레드에서 로직 처리 (화면 멈춤 방지)
                    threading.Thread(target=process_qr_job, args=(qr_data,), daemon=True).start()
                # 인식된 QR에 초록색 사각형 그리기
                cv2.rectangle(frame, obj.rect, (0, 255, 0), 2)

        # 이미지를 JPG로 인코딩 후 Base64 문자열로 변환 (웹 전송용)
        _, buffer = cv2.imencode('.jpg', frame)
        img_str = base64.b64encode(buffer).decode('utf-8')
        # 웹소켓으로 이미지 전송
        socketio.emit('video_feed_qr', {'image': img_str})
        time.sleep(0.03) # CPU 과부하 방지용 딜레이

# 2번 카메라 스레드 (단순 모니터링용)
def cam2_thread():
    cap = cv2.VideoCapture(2) # 2번 카메라 연결
    while True:
        ret, frame = cap.read()
        if not ret: 
            time.sleep(0.1)
            continue
        # 영상 리사이즈 및 인코딩 (QR 인식 로직 없음)
        frame = cv2.resize(frame, (320, 240))
        _, buffer = cv2.imencode('.jpg', frame)
        img_str = base64.b64encode(buffer).decode('utf-8')
        socketio.emit('video_feed_cam2', {'image': img_str})
        time.sleep(0.05)

# ROS 통신을 담당하는 별도 스레드 함수
def ros_thread_func():
    global ros_node
    rclpy.init() # ROS 2 초기화
    ros_node = WebBridgeNode() # 노드 생성
    try:
        rclpy.spin(ros_node) # 노드가 종료될 때까지 메시지 대기 (블로킹)
    except: pass
    finally:
        # 종료 시 리소스 해제
        if rclpy.ok(): rclpy.shutdown()

# 메인 페이지 라우팅
@app.route('/')
def index():
    return render_template('index.html') # index.html 파일 렌더링

# 프로그램 진입점
if __name__ == '__main__':
    # 1. ROS 스레드 시작 (데몬 스레드: 메인 프로그램 종료 시 같이 종료됨)
    threading.Thread(target=ros_thread_func, daemon=True).start()
    # 2. QR 카메라 스레드 시작
    threading.Thread(target=qr_camera_thread, daemon=True).start()
    # 3. 2번 카메라 스레드 시작
    threading.Thread(target=cam2_thread, daemon=True).start()
    
    print("\n🚀 SERVER STARTED\n")
    # Flask 서버 실행 (SocketIO 사용)
    socketio.run(app, host='0.0.0.0', port=5000, allow_unsafe_werkzeug=True)