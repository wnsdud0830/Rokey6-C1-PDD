# Smart Hospital Robot Controller (MED + LIQ + DRAWER)

> **소스코드 구성 안내**
> **프로젝트 관련 패키지 폴더만** zip으로 묶었습니다. *(워크스페이스 전체, build/install/log 제외)*
> - 본 README는 이미지(시스템 설계/플로우차트)와 실행 순서, 의존성 설치 방법을 포함합니다.

---

## 1. 시스템 설계, 플로우 차트 그림

### 1-1. 시스템 설계도 (System Architecture)
<p align="center">
  <img src="./images/system_architecture.png" alt="System Architecture" width="780">
</p>

**구성 요약**
- **ROS2 제어 파트(로봇 PC)**
  - Doosan 협동로봇(M0609) 제어 노드가 동작합니다.
  - 로봇 동작은 **단일 통합 노드**에서 순차 실행됩니다: `MED → LIQ → DRAWER`.
  - 상태/진행 상황은 토픽으로 퍼블리시하여 UI에서 확인합니다.
- **웹/관리 파트(서버 PC)**
  - Flask 기반 웹 서버가 UI를 제공합니다.
  - QR 스캔 결과를 DB 조회에 활용하고, ROS2 토픽을 통해 로봇에 실행 명령을 전달합니다.
- **DB(MySQL)**
  - 환자/스케줄 테이블(`daily_schedule`)을 조회하여
    - 약(PILL_A/PILL_B) 스케줄
    - 원액/물약 필요 여부(`liquid_type`)
    - 서랍 물품 필요 여부(`supply_type`)
    를 결정합니다.

**ROS2 통신(핵심 토픽)**
- `/{ROBOT_ID}/robot_cmd` : 웹 서버 → 로봇 제어 노드 (String: `qr_code`)
- `/{ROBOT_ID}/monitor_cmd` : 로봇 제어 노드 → 웹 서버 (String: 상태/"finish")

> 예시: ROBOT_ID가 `dsr01`이면 `/dsr01/robot_cmd`, `/dsr01/monitor_cmd`.

### 1-2. 플로우 차트 (Flow Chart)
<p align="center">
  <img src="./images/flowchart.png" alt="Flow Chart" width="780">
</p>

- 웹 UI에서 QR 인식 → DB 조회 → 실행 가능 상태이면 `robot_cmd(qr_code)` 발행
- 로봇 노드는 `system_busy` 잠금으로 **동시 실행을 방지**하고, 워커 스레드에서 시퀀스를 실행
- 완료 시 `monitor_cmd`로 `finish`를 보내 UI가 다음 입력을 받을 수 있도록 전환

### 1-3. 노드 아키텍처 (Node Architecture)
<p align="center">
  <img src="./images/node_architecture.png" alt="Node Architecture" width="780">
</p>

- **Server Process (app.py)**
  - QR Camera Thread (Scan & Decode)
  - DB Function (Select / Update)
  - SocketIO Handler (Web UI Update)
  - ROS2 Node (Publish / Subscribe)
- **Robot Control Script (통합 노드)**
  - `/robot_cmd` 구독 → 워커 스레드 스폰
  - DB 조회 후 `MED → LIQ → DRAWER` 수행
  - `/monitor_cmd` 퍼블리시로 UI에 진행상태 전달

---

## 2. 운영체제 환경 (OS Environment)

- **OS:** Ubuntu 22.04 LTS
- **ROS Version:** ROS 2 Humble jazzy
- **Language:** Python 3.11
- **IDE:** VS Code

권장/전제 구성(코드 기준)
- **DB:** MySQL (서버/내부망) — `daily_schedule` 테이블 사용
- **Robot SDK:** Doosan DSR / `DSR_ROBOT2`, `dsr_msgs2` (로봇 제어 PC)

---

## 3. 사용한 장비 목록 (Hardware List)

<p align="center">
  <img src="./images/hardware.png" alt="Hardware" width="780">
</p>

- **PC 2대**
  - 서버 PC(Flask + DB 연동 + UI)
  - 로봇 PC(ROS2 + Doosan 로봇 제어 노드)
- **DOOSAN ROBOTICS 협동 로봇(M0609)**
- **웹캠(USB Camera)**: QR 스캔/영상 입력
- **알약 디스펜서 / 알약 보관용기**
- **의약품 보관 서랍(3칸 등)**
- **물약/원액 용기**

---

## 4. 의존성 (requirements.txt)

프로젝트에는 **서버(app.py)** 와 **로봇 제어 노드(통합 노드)** 가 함께 존재합니다.
- 서버는 일반 Python 패키지(pip) 기반
- 로봇 제어 노드는 ROS2 + Doosan SDK 환경이 필요합니다.

### 4-1. 서버(Flask) requirements.txt 예시

```txt
# --- Server / UI ---
Flask==2.3.3
Flask-SocketIO==5.3.6
python-socketio==5.10.0
opencv-python==4.8.1.78
numpy==1.24.4
pyzbar==0.1.9
pymysql==1.1.0

# --- DB  ---
mysql-connector-python==8.3.0

# --- ROS2  ---
# rclpy
# std_msgs
# dsr_msgs2
```

설치:
```bash
python3 -m pip install -r requirements.txt
```

### 4-2. ROS2/로봇 제어 측 의존
- ROS2 Humble: `rclpy`, `std_msgs` 등
- Doosan 패키지/메시지: `dsr_msgs2`, `DSR_ROBOT2`, `DR_init`
- DB 연결(통합 노드 코드 기준): `mysql-connector-python` (또는 환경에 맞게 PyMySQL로 통일)

---

## 5. 간단한 사용 설명 (launch 순서 및 스크립트)

아래는 **권장 실행 순서**입니다. (서버 → ROS2 → 로봇)

### 5-1. DB 준비(MySQL)
1) MySQL 서버가 실행 중인지 확인
2) 코드에서 사용하는 접속 정보가 올바른지 확인
   - `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASS`, `DB_NAME`
3) `daily_schedule` 테이블에 `qr_code` 기반 데이터가 존재해야 합니다.

### 5-2. 서버 실행 (Flask + SocketIO + QR)
프로젝트의 `my_CCTV` 폴더(또는 app.py 위치)에서:

```bash
cd my_CCTV
python3 app.py
```

- 실행 후 브라우저에서 서버 주소로 접속 (예: `http://<SERVER_IP>:5000`)
- UI에서 QR을 인식하면 서버가 DB를 조회하고, ROS2 토픽으로 실행 명령을 퍼블리시합니다.

### 5-3. ROS2/로봇 제어 노드 실행
로봇 PC에서(ROS2 워크스페이스 환경):

```bash
# 워크스페이스 빌드/소스
colcon build
source install/setup.bash

# 통합 노드 실행
ros2 run hospital pharmacy
```

### 5-4. 동작 확인(토픽)
터미널에서 상태 확인:

```bash
# 로봇 상태 메시지 확인
ros2 topic echo /dsr01/monitor_cmd

# 명령 토픽 확인
ros2 topic echo /dsr01/robot_cmd
```

수동으로 테스트 명령 발행:

```bash
ros2 topic pub /dsr01/robot_cmd std_msgs/msg/String "{data: 'YOUR_QR_CODE'}" -1
```

### 5-5. 전체 동작 요약
1) 웹 UI에서 QR 인식
2) 서버가 DB 조회 후 필요 작업 판단(MED/LIQ/DRAWER)
3) 서버가 `/dsr01/robot_cmd`로 `qr_code` 발행
4) 통합 로봇 노드가 `MED → LIQ → DRAWER` 순서로 실행
5) 완료 시 `/dsr01/monitor_cmd`로 `finish` 발행 → UI에서 완료 표시 및 다음 입력 대기

---

## 참고
- 로봇 시퀀스 중 **SAFE_STOP/SAFE_OFF** 등의 보호정지 상황에 대해, DRAWER 파트는 모니터 스레드 기반 복구 로직이 포함되어 있습니다.
- E-STOP(비상정지)은 자동 복구 대상이 아니며, **수동 해제 후 재명령**이 필요합니다.
