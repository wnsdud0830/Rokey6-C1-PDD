# -*- coding: utf-8 -*-

"""
- 환자 QR 코드를 입력받으면 ROS2 단일 노드(integrated_med_liq_drawer_node)에서 3개의 조제/제공 시퀀스를 한 번의 명령 처리 단위로 순차 실행:
    1) MED  (알약-약통&트레이(디스펜서) 기반 투약 시퀀스)
    2) LIQ  (물약-액상 전달 시퀀스, DB 값이 NONE이 아니면 1회 실행)
    3) DRAWER(의학용품 서랍: DB supply_type 기반, 서랍 작업 중에만 안전복구 스레드 활성화)
- 입력 인터페이스:
    - Subscribe:  /dsr01/robot_cmd   (String: qr_code) : 외부 모니터링 시스템에서 QR 코드/명령을 송신
- 출력/모니터링 인터페이스:
    - Publish:    /dsr01/monitor_cmd (String: 상태/진행 메시지) 
    - Publish:    "finish" (작업 완료 신호로 사용)
-전체 동작 흐름
   - MED (알약 조제/트레이 동작)
     * DB(hospital_db.daily_schedule)에서 오늘 요일 기준 pill_type 정보를 조회하여
       Pill A/B를 요일 칸에 맞춰 필요한 요일만 dispense하고,
       이어서 원본 tail 시퀀스(트레이/디스펜서 마무리 동작)를 수행한다.
   - LIQ (물약/원액 제공)
     * DB에서 liquid_type이 NONE이 아니면 1회만 liquid 시퀀스를 실행한다(need gate).
   - DRAWER (의약품 서랍에서 물품 픽업/전달/서랍 닫기)
     * DB에서 supply_type을 조회하여 필요한 서랍만 처리한다.
     * 서랍 작업 중 SafeStop/SafeOff 발생 시 자동 복구를 위해 서랍 구간에서만
       별도의 모니터 스레드를 활성화한다(Estop은 자동 복구 금지). 
- 핵심 설계 포인트:
    - 로봇 API(DSR_ROBOT2) 버전 차이로 인한 인자 불일치 문제를 med_drl_call/liq__drl_motion_call에서 흡수
      (inspect.signature로 지원하지 않는 kwargs 제거 + unexpected keyword argument 발생 시 재시도)
    - 안전복구는 "서랍 시퀀스에만" 적용:
        - 별도 monitor thread에서 GetRobotState + SetRobotControl 서비스 호출로 SAFE_STOP/SAFE_OFF 자동 해제
        - E-STOP은 자동 해제 금지(수동 해제 필요)
        - drawer 동작들은 drawer__exec_action으로 감싸서 fault 발생 시 "복구 완료 후 재시도"하여 True Resume
    - 시스템 동시 실행 방지:
        - system_busy 플래그 + lock으로 QR 요청이 중첩될 때 무시(또는 상태 publish)
"""

import rclpy
import DR_init
import time
import inspect
import threading

from std_msgs.msg import String
from rclpy.executors import MultiThreadedExecutor
from dsr_msgs2.srv import SetRobotControl, GetRobotState, GetLastAlarm

# =========================
# Robot / ROS2 basic config
# =========================
ROBOT_ID    = "dsr01"
ROBOT_MODEL = "m0609"
ROBOT_TOOL  = "Tool Weight"
ROBOT_TCP   = "GripperDA_v1"

# Drawer motion params
VELOCITY = 60
ACC = 60
TIMESET = 3

# =========================
# DB config
# =========================
DB_HOST = "192.168.10.52"
DB_PORT = 3306
DB_USER = "rokey"
DB_PASS = "1234"
DB_NAME = "hospital_db"

# DR_init 설정 (임포트 전 필수)
DR_init.__dsr__id    = ROBOT_ID
DR_init.__dsr__model = ROBOT_MODEL


def main(args=None):
    rclpy.init(args=args)

    # 단일 노드로 통합
    node = rclpy.create_node("integrated_med_liq_drawer_node", namespace=ROBOT_ID)
    DR_init.__dsr__node = node

    # DSR_ROBOT2 단일 로드 + 안전 req
    # 이후 모든 로봇 API 심볼은 req()로 가져와 존재 여부를 강제 체크
    import DSR_ROBOT2 as dr

    """
    DSR_ROBOT2 심볼을 안전하게 가져오는 헬퍼.
    - 런타임 환경/버전 차이로 심볼이 없으면 즉시 ImportError로 실패(빠른 실패)
    """
    def req(name: str):
        v = getattr(dr, name, None)
        if v is None:
            raise ImportError(f"DSR_ROBOT2에 '{name}' 심볼이 없습니다.")
        return v

    # ================
    # 공용 심볼 로딩
    # ================
    set_digital_output = req("set_digital_output")
    wait               = req("wait")

    set_tool       = req("set_tool")
    set_tcp        = req("set_tcp")
    set_robot_mode = req("set_robot_mode")
    get_robot_mode = req("get_robot_mode")
    get_tool       = req("get_tool")
    get_tcp        = req("get_tcp")

    ROBOT_MODE_MANUAL     = req("ROBOT_MODE_MANUAL")
    ROBOT_MODE_AUTONOMOUS = req("ROBOT_MODE_AUTONOMOUS")

    OFF = req("OFF")
    ON  = req("ON")

    posx = req("posx")
    posj = req("posj")

    # 상태/진행 상황을 UI로 알리기 위한 Publisher
    status_pub = node.create_publisher(String, "monitor_cmd", 10)

    def publish_status(text: str):
        msg = String()
        msg.data = text
        status_pub.publish(msg)

    # =========================================================
    # 1) DRAWER 시퀀스 (서랍만 안전복구(THREAD) 활성화)
    # =========================================================
    drawer_movel = req("movel")
    drawer_movej = req("movej")
    DR_MV_MOD_REL = req("DR_MV_MOD_REL")

    # drawer에서 상태 확인/복구에 필요한 심볼
    # 직접 API 호출과 서비스 호출 두 방식이 혼재되어 있음
    # - 서비스 호출은 monitor thread에서 안전하게 처리(스핀 없이 polling)
    drawer_get_robot_state = req("get_robot_state")
    drawer_get_last_alarm  = req("get_last_alarm")
    drawer_get_current_posx = req("get_current_posx")

    # drawer 핵심 위치/목표 포즈
    drawer_stay = posx(587.56, 138.21, 203.28, 3.48, 153.35, -83.12)
    drawer_goal = posx(312.07, 362.32, 62.72, 59.29, 179.4, 58.58)

    drawer_handle_list = [
        posx(644.7, 217.23, 118.99, 171.72, -158.61, 83.96),
        posx(647.69, 220.35, 42.21, 175.62, -153.85, 88.28),
        posx(640.3, 83.07, 79.76, 179.17, -152.65, 92.51)
    ]

    drawer_closespot_list = [
        posx(499.7, 217.23, 118.99, 171.72, -158.61, 83.96),
        posx(502.69, 220.35, 42.21, 175.62, -153.85, 88.28),
        posx(495.3, 83.07, 79.76, 179.17, -152.65, 92.51)
    ]

    # 서랍 열기/닫기, 점프/접근/상승 등의 패턴 동작을 구성
    drawer_go   = posx(-145.00, 0.00, 0.00, 0.00, 0.00, 0.00)
    drawer_back = posx(145.00, 0.00, 0.00, 0.00, 0.00, 0.00)
    drawer_jump = posx(0.00, 0.00, 100.00, 0.00, 0.00, 0.00)
    drawer_up   = posx(0.00, 0.00, 200.00, 0.00, 0.00, 0.00)
    drawer_down = posx(0.00, 0.00, -110.00, 0.00, 0.00, 0.00)
    drawer_inf  = posx(80.00, 0.00, 0.00, 0.00, 0.00, 0.00)
    drawer_z10  = posx(0.00, 0.00, 10.00, 0.00, 0.00, 0.00)

    def drawer_grip_open():
        set_digital_output(1, OFF)
        set_digital_output(2, OFF)
        set_digital_output(3, ON)
        wait(0.8)

    def drawer_grip_close():
        set_digital_output(1, ON)
        set_digital_output(2, OFF)
        set_digital_output(3, OFF)
        wait(4.0)

# =========================================================
# Drawer safety recovery (Thread-based, enable only during drawer)
# - NO rclpy.spin_* in this thread
# - NO DSR_ROBOT2.get_robot_state/get_last_alarm usage here
# =========================================================

    CONTROL_RESET_SAFE_STOP = 2  # 보호 정지 해제
    CONTROL_RESET_SAFE_OFF  = 3  # 서보 ON (Safe Off -> Standby)

    # 로봇 상태 코드 매핑
    ROBOT_STATE_MAP = {
        0:  "STATE_INITIALIZING",
        1:  "STATE_STANDBY",
        2:  "STATE_MOVING",
        3:  "STATE_SAFE_OFF",
        4:  "STATE_TEACHING",
        5:  "STATE_SAFE_STOP",
        6:  "STATE_EMERGENCY_STOP",
        7:  "STATE_HOMMING",
        8:  "STATE_RECOVERY",
        9:  "STATE_SAFE_STOP2",
        10: "STATE_SAFE_OFF2",
        15: "STATE_NOT_READY",
    }

    # fault state 그룹화
    FAULT_SAFE_STOP = {5, 9}   # 보호정지 계열
    FAULT_SAFE_OFF  = {3, 10}  # 서보 off 계열
    FAULT_ESTOP     = {6}      # 비상정지(자동 복구 금지)
    FAULT_ALL       = FAULT_SAFE_STOP | FAULT_SAFE_OFF | FAULT_ESTOP

    # drawer 안전복구 스레드 제어 플래그
    _drawer_recovery_enabled = threading.Event()  # drawer 작업 중에만 monitor 활성화
    _drawer_fault_event = threading.Event()       # fault 감지 플래그(재시도 트리거)
    _drawer_estop_event = threading.Event()       # E-STOP 감지 플래그(즉시 중단 트리거)
    _drawer_mon_stop = threading.Event()          # 모니터 스레드 종료 신호

    # service clients (same node, single creation) monitor thread에서만 사용
    _drawer_ctrl_cli  = node.create_client(SetRobotControl, f'/{ROBOT_ID}/system/set_robot_control')
    _drawer_state_cli = node.create_client(GetRobotState,   f'/{ROBOT_ID}/system/get_robot_state')
    _drawer_alarm_cli = node.create_client(GetLastAlarm,    f'/{ROBOT_ID}/system/get_last_alarm')
    
    """Thread-safe: call_async + polling only (no spin)."""
    """
    [Thread-safe 서비스 호출 유틸]
    - call_async()로 Future 생성
    - spin 없이 polling으로 done()까지 대기
    - timeout 초과 시 None 반환
    """
    def _call_srv_async(cli, req, timeout_sec=1.0):
        
        if cli is None or (not cli.service_is_ready()):
            return None

        fut = cli.call_async(req)
        t0 = time.time()
        while not fut.done():
            if time.time() - t0 > timeout_sec:
                return None
            time.sleep(0.005)

        try:
            return fut.result()
        except Exception:
            return None

    """
    서비스(GetLastAlarm) 기반 알람 상태(robot_state) 문자열(repr)로 로깅용 반환
        - 응답 구조가 환경마다 다를 수 있어 field name 후보들을 순회
    """
    def drawer_safe_get_robot_state():
        resp = _call_srv_async(_drawer_state_cli, GetRobotState.Request(), timeout_sec=0.5)
        if resp is None:
            return None
        # 대부분 robot_state 필드
        for k in ("robot_state", "state", "current_state"):
            if hasattr(resp, k):
                try:
                    return int(getattr(resp, k))
                except Exception:
                    pass
        return None

    # 서비스(GetLastAlarm) 기반 알람 상태 문자열(repr)로 로깅용 반환
    def drawer_safe_get_last_alarm_repr():
        resp = _call_srv_async(_drawer_alarm_cli, GetLastAlarm.Request(), timeout_sec=0.5)
        if resp is None:
            return None
        return repr(resp)

    """
    서비스(SetRobotControl) 호출하여 안전 상태 해제 시도.
    - success 필드가 있으면 그것을 반환
    """
    def drawer_call_set_robot_control(control_value: int) -> bool:
        req = SetRobotControl.Request()
        req.robot_control = int(control_value)
        resp = _call_srv_async(_drawer_ctrl_cli, req, timeout_sec=2.0)
        if resp is None:
            return False
        # 보통 success bool
        return bool(getattr(resp, "success", False))

    """
    안전복구 후 STANDBY 상태가 "연속" stable_cnt_need회 유지되는지 확인
    - 일시적 튐/전이 상태를 디바운스하기 위한 안정성 체크
    """
    def drawer_wait_standby_stable(max_sec=12.0, stable_cnt_need=10) -> bool:
        t0 = time.time()
        stable = 0
        while time.time() - t0 < max_sec:
            s = drawer_safe_get_robot_state()
            if s == 1:
                stable += 1
                if stable >= stable_cnt_need:
                    return True
            else:
                stable = 0
            time.sleep(0.05)
        return False

    """
    SAFE_STOP/SAFE_OFF가 해제될 때까지 대기(모니터 스레드가 reset 수행)
    - E-STOP이면 자동복구 불가 -> False 반환 + _drawer_estop_event set
    """
    def drawer_wait_fault_clear(max_sec=30.0) -> bool:
        """SAFE_STOP/SAFE_OFF 해제(모니터가 reset 수행)될 때까지 대기."""
        t0 = time.time()
        last_log = 0.0
        while time.time() - t0 < max_sec:
            s = drawer_safe_get_robot_state()
            if s is None:
                time.sleep(0.05)
                continue

            # 0.5초마다 로그
            if time.time() - last_log > 0.5:
                node.get_logger().warn(f"[DRAWER][RESUME] waiting clear... state={s} ({ROBOT_STATE_MAP.get(s,'UNKNOWN')})")
                last_log = time.time()

            if s in FAULT_ESTOP:
                _drawer_estop_event.set()
                return False
            
            # fault 상태군에서 벗어나면 OK
            if (s not in FAULT_SAFE_STOP) and (s not in FAULT_SAFE_OFF):
                return True

            time.sleep(0.05)
        return False

    def drawer_wait_recovery_if_needed() -> bool:
        """
        fault_event set이면: 
        - 모니터 스레드가 reset을 끝낼 때까지 대기
        - 복구 실패 or E-STOP이면 False 반환
        """
        if _drawer_estop_event.is_set():
            return False
        if not _drawer_fault_event.is_set():
            return True

        node.get_logger().warn("[DRAWER][RESUME] SAFE_STOP/SAFE_OFF detected -> wait recovery then retry same action.")
        ok = drawer_wait_fault_clear(max_sec=30.0)
        if not ok:
            alarm = drawer_safe_get_last_alarm_repr()
            node.get_logger().error(f"[DRAWER][RESUME] recovery failed or E-STOP. alarm={alarm}")
            return False

        _drawer_fault_event.clear()
        time.sleep(0.2)
        return True

    def drawer_exec_action(name: str, fn):
        """
        [서랍 True-Resume 액션 래퍼]
        - fn() 실행 중 fault 발생 시:
            - 복구 완료까지 대기 후 같은 동작을 재시도
        - E-STOP 또는 복구 실패 시 RuntimeError로 상위 중단
        """
        while True:
            if not drawer_wait_recovery_if_needed():
                raise RuntimeError(f"[DRAWER][ABORT] {name}: E-STOP or recovery failed")

            try:
                fn()
            except Exception as e:
                s = drawer_safe_get_robot_state()
                node.get_logger().error(f"[DRAWER][ACT] {name} exception={e} state={s} ({ROBOT_STATE_MAP.get(s,'UNKNOWN')})")
                # fault면 복구 후 재시도
                if (s in FAULT_ALL) or _drawer_fault_event.is_set():
                    continue
                raise

            # 동작 후에도 fault가 남아있으면 복구 후 재시도
            s2 = drawer_safe_get_robot_state()
            if (s2 in FAULT_ALL) or _drawer_fault_event.is_set():
                continue

            return

    """
    [서랍 전용 안전복구 모니터 스레드]
    - drawer 수행 중(_drawer_recovery_enabled set)일 때만 동작
    - 상태를 주기적으로 읽어 SAFE_STOP/SAFE_OFF면 자동 reset 요청
    - E-STOP은 자동 해제 금지 -> 이벤트 set + 반복 로깅
    """
    def drawer_recovery_monitor_thread():
        node.get_logger().info("[DRAWER][MON] monitor thread started.")
        publish_status("[DRAWER][MON] monitor thread started.")

        # 서비스 준비 기다림(최초 1회)
        _drawer_ctrl_cli.wait_for_service(timeout_sec=2.0)
        _drawer_state_cli.wait_for_service(timeout_sec=2.0)
        _drawer_alarm_cli.wait_for_service(timeout_sec=2.0)

        next_allowed_reset_time = 0.0
        last_state = None
        last_estop_log = 0.0

        while (not _drawer_mon_stop.is_set()) and rclpy.ok():
            # drawer 작업 중이 아니면 idle
            if not _drawer_recovery_enabled.is_set():
                time.sleep(0.1)
                continue

            s = drawer_safe_get_robot_state()
            if s is None:
                time.sleep(0.1)
                continue
            
            # 상태 변경 시 로깅(디버깅/운영 가시성)
            if s != last_state:
                node.get_logger().info(f"[DRAWER][MON] state: {last_state} -> {s} ({ROBOT_STATE_MAP.get(s,'UNKNOWN')})")
                last_state = s

            # fault flagging. fault 상태면 이벤트 set (액션 래퍼가 재시도 모드로 들어가도록)
            if s in FAULT_ALL:
                _drawer_fault_event.set()

            # E-STOP은 자동 reset 금지(수동 해제 필요)
            if s in FAULT_ESTOP:
                _drawer_estop_event.set()
                now = time.time()
                if now - last_estop_log > 1.0:
                    alarm = drawer_safe_get_last_alarm_repr()
                    node.get_logger().error(f"[DRAWER][MON] E-STOP detected. Manual release required. alarm={alarm}")
                    last_estop_log = now
                time.sleep(0.2)
                continue
            
            now = time.time()
            # reset 연속 호출 방지(쿨다운)
            if (s in (FAULT_SAFE_STOP | FAULT_SAFE_OFF)) and (now < next_allowed_reset_time):
                time.sleep(0.1)
                continue

            # 디바운스: SAFE_* 상태를 한 번 더 확인
            if s in (FAULT_SAFE_STOP | FAULT_SAFE_OFF):
                time.sleep(0.3)
                s2 = drawer_safe_get_robot_state()
                if s2 is None:
                    continue
            else:
                s2 = s

            # SAFE_STOP reset
            if s2 in FAULT_SAFE_STOP:
                node.get_logger().warn(f"[DRAWER][MON] SAFE_STOP detected (state={s2}). reset=2 ...")
                ok = drawer_call_set_robot_control(CONTROL_RESET_SAFE_STOP)
                state_after = drawer_safe_get_robot_state()
                node.get_logger().warn(f"[DRAWER][MON] reset_return={ok}, state_after={state_after} ({ROBOT_STATE_MAP.get(state_after,'UNKNOWN')})")

                if ok and drawer_wait_standby_stable():
                    node.get_logger().info("[DRAWER][MON] SAFE_STOP cleared -> STANDBY stable.")
                    _drawer_fault_event.clear()
                else:
                    alarm = drawer_safe_get_last_alarm_repr()
                    node.get_logger().warn(f"[DRAWER][MON] SAFE_STOP not stabilized yet. alarm={alarm}")

                next_allowed_reset_time = time.time() + 0.8
                time.sleep(0.1)
                continue

            # SAFE_OFF reset
            if s2 in FAULT_SAFE_OFF:
                node.get_logger().warn(f"[DRAWER][MON] SAFE_OFF detected (state={s2}). reset=3 ...")
                ok = drawer_call_set_robot_control(CONTROL_RESET_SAFE_OFF)
                state_after = drawer_safe_get_robot_state()
                node.get_logger().warn(f"[DRAWER][MON] reset_return={ok}, state_after={state_after} ({ROBOT_STATE_MAP.get(state_after,'UNKNOWN')})")

                if ok and drawer_wait_standby_stable():
                    node.get_logger().info("[DRAWER][MON] SAFE_OFF cleared -> STANDBY stable.")
                    publish_status("[DRAWER][MON] SAFE_OFF cleared -> STANDBY stable.")
                    _drawer_fault_event.clear()
                else:
                    alarm = drawer_safe_get_last_alarm_repr()
                    node.get_logger().warn(f"[DRAWER][MON] SAFE_OFF not stabilized yet. alarm={alarm}")

                next_allowed_reset_time = time.time() + 0.8
                time.sleep(0.1)
                continue

            # 정상 상태면 fault_event 정리(옵션)
            if (s not in FAULT_SAFE_STOP) and (s not in FAULT_SAFE_OFF):
                _drawer_fault_event.clear()

            time.sleep(0.1)

    # 모니터 thread는 1회만 생성해서 항상 돌아가되,
    # _drawer_recovery_enabled 플래그로 drawer 동작 중에만 실질 동작
    _drawer_mon_thread = threading.Thread(target=drawer_recovery_monitor_thread, daemon=True)
    _drawer_mon_thread.start()

    """
    posx(ABS) + posx(delta) -> posx(ABS) (단순 가산; 기존 REL delta를 ABS target으로 고정하기 위함)
    - 현재 코드에서는 실사용이 없지만,
        "REL delta를 ABS target으로 고정"하는 접근(누적 방지)을 위해 남겨둔 유틸 형태.
    """
    def _posx_add(a, b):
        return posx(
            float(a[0]) + float(b[0]),
            float(a[1]) + float(b[1]),
            float(a[2]) + float(b[2]),
            float(a[3]) + float(b[3]),
            float(a[4]) + float(b[4]),
            float(a[5]) + float(b[5]),
        )


    # ----- TRUE RESUME helpers (drawer only) -----
    # 아래 함수들은 "현재 위치"를 읽어서 REL 이동을 ABS 목표로 바꾸는 방식으로,
    # fault 후 재시도 시 REL 누적/드리프트가 발생하지 않도록 설계되어 있음.

    """
    현재 TCP posx를 [x,y,z,rx,ry,rz] float 리스트로 반환.
    - get_current_posx()가 (pos, sol) 또는 pos만 반환하는 경우 둘 다 처리.
    """
    def drawer__cur_posx6() -> list:
        cur = drawer_get_current_posx()
        # (pos, sol) 형태가 흔함
        pos = cur[0] if isinstance(cur, (tuple, list)) and len(cur) >= 1 and isinstance(cur[0], (list, tuple)) else cur
        return [float(pos[0]), float(pos[1]), float(pos[2]), float(pos[3]), float(pos[4]), float(pos[5])]

    #posx 형태(인덱싱 가능)에서 6축 delta 리스트로 변환
    def drawer__delta6_from_posx(p) -> list:
        return [float(p[0]), float(p[1]), float(p[2]), float(p[3]), float(p[4]), float(p[5])]

    """
    현재 위치 + delta_posx(REL)를 ABS 목표 포즈로 변환
    - 'REL 명령을 ABS로 고정'해서 재시도 시 누적되지 않게 함
    """
    def drawer__abs_target_from_delta(delta_posx):
        c = drawer__cur_posx6()
        d = drawer__delta6_from_posx(delta_posx)
        t = [c[i] + d[i] for i in range(6)]
        return posx(t[0], t[1], t[2], t[3], t[4], t[5])

    """
    목표 포즈 도달 검증(선택 기능)
    - 선형 오차(x,y,z) tol_xyz(mm)
    - 회전 오차(rx,ry,rz) tol_r(deg)
    - verify_pose로 전달된 경우, 도달하지 못하면 재시도 루프를 유도
    """
    def drawer__pose_close_enough(target_pose, tol_xyz=0.5, tol_r=1.0) -> bool:
        try:
            now6 = drawer__cur_posx6()
            tgt6 = [float(target_pose[0]), float(target_pose[1]), float(target_pose[2]),
                    float(target_pose[3]), float(target_pose[4]), float(target_pose[5])]
            dx = abs(now6[0] - tgt6[0])
            dy = abs(now6[1] - tgt6[1])
            dz = abs(now6[2] - tgt6[2])
            dr = max(abs(now6[3] - tgt6[3]), abs(now6[4] - tgt6[4]), abs(now6[5] - tgt6[5]))
            return (dx <= tol_xyz and dy <= tol_xyz and dz <= tol_xyz and dr <= tol_r)
        except Exception:
            return False

    # 아래 drawer__wait_fault_clear / drawer__wait_recovery_if_needed / drawer__exec_action 은
    # 앞에서 정의한 drawer_wait_* / drawer_exec_action과 역할이 유사하나,
    # 여기서는 "DSR API 직접 호출" 기반으로 True Resume를 한번 더 강화한 형태.
    # (현 코드에서는 실제 drawer_execute()가 drawer__exec_action을 사용하므로 이 블록이 핵심)

    """
    DSR API(get_robot_state)로 fault 해제 대기
    - 서비스 기반이 아닌 직접 호출 기반
    """
    def drawer__wait_fault_clear(max_sec=30.0) -> bool:
        t0 = time.time()
        last_print = 0.0
        while time.time() - t0 < max_sec:
            s = drawer_get_robot_state()

            if time.time() - last_print > 0.5:
                node.get_logger().info(f"[DRAWER][RESUME] waiting fault clear... state={s} ({ROBOT_STATE_MAP.get(s,'UNKNOWN')})")
                last_print = time.time()

            # E-STOP은 자동 재개 불가
            if s in FAULT_ESTOP:
                return False

            # SAFE_STOP/SAFE_OFF만 벗어나면 OK
            if (s not in FAULT_SAFE_STOP) and (s not in FAULT_SAFE_OFF):
                return True

            time.sleep(0.05)

        return False

    """
    _drawer_fault_event가 set이면:
    - fault가 풀릴 때까지 대기 후 재개
    - 실패(E-STOP 포함) 시 False
    """
    def drawer__wait_recovery_if_needed() -> bool:
        if not _drawer_fault_event.is_set():
            return True

        node.get_logger().warn("[DRAWER][RESUME] fault 감지 -> 복구 완료 대기 후 현재 동작을 재개합니다.")
        ok = drawer__wait_fault_clear(max_sec=30.0)
        if not ok:
            alarm = drawer_get_last_alarm()
            node.get_logger().error(f"[DRAWER][RESUME] 복구 실패 또는 E-STOP. alarm={alarm}")
            publish_status("[DRAWER][E-STOP] manual release + re-command required.")
            return False

        _drawer_fault_event.clear()
        time.sleep(0.2)
        return True

    """
    [drawer 동작 실행 래퍼(BOOL 반환)]
    - fn 실행 실패/예외 시:
        - fault 상태이면 복구 후 재시도
        - 일반 예외이면 False 반환
    - verify_pose가 있으면 목표 도달 검증 후 불충분 시 재시도
    """
    def drawer__exec_action(name: str, fn, verify_pose=None) -> bool:
        while True:
            if not drawer__wait_recovery_if_needed():
                return False

            try:
                fn()
            except Exception as e:
                s = drawer_get_robot_state()
                node.get_logger().error(f"[DRAWER][{name}] exception: {e} / state={s}({ROBOT_STATE_MAP.get(s,'UNKNOWN')})")
                # fault면 복구 후 재시도
                if s in FAULT_ALL or _drawer_fault_event.is_set():
                    continue
                return False

            # 동작 후에도 fault 남아있으면 복구/재시도
            if _drawer_fault_event.is_set() or drawer_get_robot_state() in FAULT_ALL:
                continue

            # 목표 도달 검증(선택)
            if verify_pose is not None:
                if not drawer__pose_close_enough(verify_pose):
                    continue

            return True

    # -------------------------
    # DB -> 서랍 인덱스 매핑
    # -------------------------
    drawer_supply_to_index = {
        "Ointment": 0,
        "Peroxide": 1,
        "Bandage": 2,
    }

    """
        DB(daily_schedule)에서 supply_type을 읽어,
        서랍 번호(index)와 supply 이름을 리스트로 반환.
        - 예외/이상치 처리:
            - NULL/빈 문자열 skip
            - 'NONE' skip
            - 'A,B,C' 형태면 split해서 매핑되는 항목만 채택
            - 중복은 seen으로 제거
    """
    def drawer_get_supply_index(qr_code: str):
        import mysql.connector

        conn = mysql.connector.connect(
            host=DB_HOST, port=DB_PORT,
            user=DB_USER, password=DB_PASS,
            database=DB_NAME
        )
        try:
            cur = conn.cursor(dictionary=True)
            cur.execute(
                """
                SELECT supply_type
                FROM daily_schedule
                WHERE qr_code = %s
                """,
                (qr_code,)
            )
            rows = cur.fetchall()

            indices = []
            seen = set()

            for r in rows:
                raw = (r.get("supply_type") or "").strip()
                if not raw:
                    continue
                if raw.upper() == "NONE":
                    continue

                parts = [p.strip() for p in raw.split(",") if p.strip()]
                for p in parts:
                    if p in drawer_supply_to_index and p not in seen:
                        seen.add(p)
                        indices.append((drawer_supply_to_index[p], p))
            return indices
        finally:
            conn.close()

    """
        [서랍 메인 시퀀스]
        - 시작 시: 안전복구 enable + fault/estop 플래그 초기화
        - DB에서 supply 목록 조회 후, supply마다:
            1) 서랍 열기(손잡이 접근 -> 파지 -> 당기기 -> release)
            2) 물품 집기(점프/접근/내려가기 -> 파지 -> 올라오기)
            3) 목적지 전달(drawer_goal -> release)
            4) 서랍 닫기(closespot -> 파지 -> z10/back -> release)
            5) home 복귀
        - 모든 동작은 drawer__exec_action으로 감싸:
            - 안전복구 후 재시도(True Resume)
            - verify_pose로 도달 검증(선택)
    """
    def drawer_execute(qr_code: str) -> bool:
        # drawer 수행 중에만 안전복구 enable
        _drawer_estop_event.clear()
        _drawer_recovery_enabled.set()  # monitor thread가 실제 동작하도록 ON
        _drawer_fault_event.clear()

        try:
            indices = drawer_get_supply_index(qr_code)
            if not indices:
                node.get_logger().info(f"[DRAWER][SKIP] qr_code={qr_code} no valid supply_type")
                return True

            node.get_logger().info(f"[DRAWER][START] supplies={ [s for _, s in indices] }")
            publish_status(f"[DRAWER][START] supplies={ [s for _, s in indices] }")

            drawer_home_pos = posx(367.3, 8.54, 206.29, 116.65, 179.95, 116.31)

            # 로봇 기본 설정(서랍 시퀀스 시작 시 안전하게 재설정)
            set_robot_mode(ROBOT_MODE_MANUAL)
            set_tool(ROBOT_TOOL)
            set_tcp(ROBOT_TCP)
            set_robot_mode(ROBOT_MODE_AUTONOMOUS)
            time.sleep(2)

            # ---- TRUE RESUME: 공통 준비 동작을 액션 단위로 ----
            if not drawer__exec_action("MOVEJ_JREADY", lambda: drawer_movej([0, 0, 90, 0, 90, 0], time=TIMESET)):
                publish_status("[DRAWER][ABORT] movej failed")
                return False

            if not drawer__exec_action("MOVE_HOME_POS", lambda: drawer_movel(drawer_home_pos, time=TIMESET), verify_pose=drawer_home_pos):
                publish_status("[DRAWER][ABORT] move home failed")
                return False

            if not drawer__exec_action("GRIP_OPEN_INIT", lambda: drawer_grip_open()):
                publish_status("[DRAWER][ABORT] grip open failed")
                return False

            if not drawer__exec_action("MOVE_STAY", lambda: drawer_movel(drawer_stay, time=TIMESET), verify_pose=drawer_stay):
                publish_status("[DRAWER][ABORT] move stay failed")
                return False

            # ---- 각 supply(서랍 작업) ----
            for idx, supply in indices:
                node.get_logger().info(f"[DRAWER][DO] supply_type={supply} drawer={idx+1}")
                publish_status(f"[DRAWER][DO] supply_type={supply} drawer={idx+1}")

                # REL 누적 방지
                # - 동일 supply 루프 내에서는 같은 REL 동작을 여러 번 재시도할 수 있으므로,
                #   한 번 계산한 ABS 목표를 캐시하여 재시도 시에도 동일 목표로 이동하게 함.
                rel_targets = {}

                def rel_pose(key: str, delta_posx):
                    if key not in rel_targets:
                        rel_targets[key] = drawer__abs_target_from_delta(delta_posx)
                    return rel_targets[key]

                # 1) open drawer
                handle_pose = drawer_handle_list[idx]
                if not drawer__exec_action("MOVE_HANDLE", lambda: drawer_movel(handle_pose, time=TIMESET), verify_pose=handle_pose):
                    publish_status("[DRAWER][ABORT] move handle failed")
                    return False

                if not drawer__exec_action("GRIP_CLOSE_1", lambda: drawer_grip_close()):
                    publish_status("[DRAWER][ABORT] grip close failed")
                    return False

                tgt_go = rel_pose("GO", drawer_go)
                if not drawer__exec_action("MOVE_GO_ABS", lambda: drawer_movel(tgt_go, time=TIMESET), verify_pose=tgt_go):
                    publish_status("[DRAWER][ABORT] move go failed")
                    return False

                if not drawer__exec_action("GRIP_OPEN_1", lambda: drawer_grip_open()):
                    publish_status("[DRAWER][ABORT] grip open failed")
                    return False

                # 2) pick object
                tgt_jump = rel_pose("JUMP", drawer_jump)
                if not drawer__exec_action("MOVE_JUMP_ABS", lambda: drawer_movel(tgt_jump, time=TIMESET), verify_pose=tgt_jump):
                    publish_status("[DRAWER][ABORT] move jump failed")
                    return False

                tgt_inf = rel_pose("INF", drawer_inf)
                if not drawer__exec_action("MOVE_INF_ABS", lambda: drawer_movel(tgt_inf, time=TIMESET), verify_pose=tgt_inf):
                    publish_status("[DRAWER][ABORT] move inf failed")
                    return False

                tgt_down = rel_pose("DOWN", drawer_down)
                if not drawer__exec_action("MOVE_DOWN_ABS", lambda: drawer_movel(tgt_down, time=TIMESET), verify_pose=tgt_down):
                    publish_status("[DRAWER][ABORT] move down failed")
                    return False

                if not drawer__exec_action("GRIP_CLOSE_2", lambda: drawer_grip_close()):
                    publish_status("[DRAWER][ABORT] grip close(2) failed")
                    return False

                tgt_up = rel_pose("UP", drawer_up)
                if not drawer__exec_action("MOVE_UP_ABS", lambda: drawer_movel(tgt_up, time=TIMESET), verify_pose=tgt_up):
                    publish_status("[DRAWER][ABORT] move up failed")
                    return False

                # 3) deliver
                if not drawer__exec_action("MOVE_GOAL", lambda: drawer_movel(drawer_goal, time=TIMESET), verify_pose=drawer_goal):
                    publish_status("[DRAWER][ABORT] move goal failed")
                    return False

                if not drawer__exec_action("GRIP_OPEN_DELIVER", lambda: drawer_grip_open()):
                    publish_status("[DRAWER][ABORT] grip open(deliver) failed")
                    return False

                # 4) close drawer
                closespot_pose = drawer_closespot_list[idx]
                if not drawer__exec_action("MOVE_CLOSESPOT", lambda: drawer_movel(closespot_pose, time=TIMESET), verify_pose=closespot_pose):
                    publish_status("[DRAWER][ABORT] move closespot failed")
                    return False

                if not drawer__exec_action("GRIP_CLOSE_3", lambda: drawer_grip_close()):
                    publish_status("[DRAWER][ABORT] grip close(3) failed")
                    return False

                tgt_z10 = rel_pose("Z10", drawer_z10)
                if not drawer__exec_action("MOVE_Z10_ABS", lambda: drawer_movel(tgt_z10, time=TIMESET), verify_pose=tgt_z10):
                    publish_status("[DRAWER][ABORT] move z10 failed")
                    return False

                tgt_back = rel_pose("BACK", drawer_back)
                if not drawer__exec_action("MOVE_BACK_ABS", lambda: drawer_movel(tgt_back, time=TIMESET), verify_pose=tgt_back):
                    publish_status("[DRAWER][ABORT] move back failed")
                    return False

                if not drawer__exec_action("GRIP_OPEN_2", lambda: drawer_grip_open()):
                    publish_status("[DRAWER][ABORT] grip open(after close) failed")
                    return False

                if not drawer__exec_action("MOVE_HOME_END", lambda: drawer_movel(drawer_home_pos, time=TIMESET), verify_pose=drawer_home_pos):
                    publish_status("[DRAWER][ABORT] move home(end) failed")
                    return False

            node.get_logger().info(f"[DRAWER][DONE] qr_code={qr_code}")
            publish_status(f"[DRAWER][DONE] qr_code={qr_code}")
            return True

        except Exception as e:
            node.get_logger().error(f"[DRAWER][ERROR] failed: {e}")
            publish_status(f"[DRAWER][ERROR] failed: {e}")
            return False

        finally:
            # drawer 작업 종료 → 모니터 비활성화
            _drawer_recovery_enabled.clear()
            _drawer_fault_event.clear()

    # =========================================================
    # 2) MEDICINE 시퀀스
    # - DB의 pill_type/day_of_week 기반으로 투약 횟수 결정
    # - DRL API 버전 차이를 med_drl_call로 흡수
    # =========================================================
    med_movel  = req("movel")
    med_movej  = req("movej")
    med_movec  = req("movec")
    med_amovel = req("amovel")

    set_singular_handling = req("set_singular_handling")
    set_velj = req("set_velj")
    set_accj = req("set_accj")
    set_velx = req("set_velx")
    set_accx = req("set_accx")

    task_compliance_ctrl    = req("task_compliance_ctrl")
    release_compliance_ctrl = req("release_compliance_ctrl")
    set_stiffnessx          = req("set_stiffnessx")
    set_desired_force       = req("set_desired_force")

    med_get_tool_force       = req("get_tool_force")
    med_get_current_posx_req = req("get_current_posx")

    DR_AVOID = req("DR_AVOID")
    DR_MV_MOD_ABS = req("DR_MV_MOD_ABS")
    DR_MV_MOD_REL = req("DR_MV_MOD_REL")
    DR_MV_RA_DUPLICATE = req("DR_MV_RA_DUPLICATE")
    DR_MV_APP_NONE     = req("DR_MV_APP_NONE")
    DR_MV_ORI_TEACH    = req("DR_MV_ORI_TEACH")

    DR_TOOL       = req("DR_TOOL")
    DR_FC_MOD_ABS = req("DR_FC_MOD_ABS")
    DR_BASE       = req("DR_BASE")

    """
        [MED용 DRL 호출 래퍼]
        - vel/acc를 [v, something] 형태로 넣는 경우가 있어 1st 요소만 사용
        - 현재 설치된 DSR API가 지원하지 않는 keyword 인자는 제거
        - unexpected keyword argument 발생 시 해당 키를 pop하고 재시도
    """
    def med_drl_call(fn, *args, **kwargs):
        if "vel" in kwargs and isinstance(kwargs["vel"], (list, tuple)):
            kwargs["vel"] = kwargs["vel"][0]
        if "acc" in kwargs and isinstance(kwargs["acc"], (list, tuple)):
            kwargs["acc"] = kwargs["acc"][0]

        # signature 기반으로 지원하지 않는 kwargs 제거(선제적)
        try:
            sig = inspect.signature(fn)
            kwargs = {k: v for k, v in kwargs.items() if k in sig.parameters}
        except (ValueError, TypeError):
            pass
        
        # runtime TypeError에도 대응(사후적)
        while True:
            try:
                return fn(*args, **kwargs)
            except TypeError as e:
                msg = str(e)
                if "unexpected keyword argument" in msg:
                    bad = msg.split("'")[1]
                    kwargs.pop(bad, None)
                    if not kwargs:
                        return fn(*args)
                    continue
                raise
    
    # MED 공통 포즈
    med_System_home = posx(367.3, 8.54, 206.29, 116.65, 179.95, 116.31)

    # Pill A/B 핸들 접근 포즈
    PILL_A_HANDLE = posx(338.45, -221.00, 98.08, 92.10, -92.98, 91.44)
    PILL_B_HANDLE = posx(338.45 + 190.0, -221.00, 98.08, 92.10, -92.98, 91.44)

    # 트레이(디스펜서))(요일 칸) 초기 접근 포즈
    TRAY_PRE        = posx(72.06, -174.00, 35.00, 49.49, -178.95, 47.48)
    TRAY_PUSH_START = posx(110.28, -178.77, 21.83, 29.35, -179.46, 29.89)

    # MED 그리퍼 IO(회전/디스펜서 동작에 따라 조합이 다름)
    def med_grip_open_rot():
        set_digital_output(1, OFF)
        set_digital_output(2, ON)
        set_digital_output(3, ON)

    def med_grip_close_rot():
        set_digital_output(1, OFF)
        set_digital_output(2, ON)
        set_digital_output(3, OFF)

    def med_grip_open_disp():
        set_digital_output(1, OFF)
        set_digital_output(2, OFF)
        set_digital_output(3, ON)

    def med_grip_close_disp():
        set_digital_output(1, OFF)
        set_digital_output(2, OFF)
        set_digital_output(3, OFF)

    """
        get_current_posx() 결과를 ABS 포즈로 정규화
        - (pos, sol) 형태면 pos만 반환
    """
    def med__cur_posx_abs():
        cur = med_get_current_posx_req()
        if isinstance(cur, (list, tuple)) and len(cur) >= 1:
            return cur[0]
        return cur

    """
        BASE 좌표계 기준 현재 TCP posx 및 z 값을 반환
        - 환경에 따라 get_current_posx(ref) 시그니처가 다를 수 있어 TypeError fallback 포함
    """
    def med_get_tcp_posx_base():
        try:
            ref_base = DR_BASE
        except NameError:
            ref_base = 0

        try:
            cur = med_get_current_posx_req(ref_base)
        except TypeError:
            cur = med_get_current_posx_req(ref_base, 0)

        if isinstance(cur, (tuple, list)) and len(cur) == 2 and isinstance(cur[0], (list, tuple)):
            pos = cur[0]
        else:
            pos = cur

        z = float(pos[2])
        return pos, z

    """
        디스펜서/트레이 쪽에서 "누르며 닫는" 동작(컴플라이언스 + force control 포함)
        - CON2: z축 힘 범위로 접촉/동작 성공 여부를 판단하는 안전 게이트 역할
    """
    # 아래 movel은 API 버전 차이를 med_drl_call이 흡수(예: app_type 지원 여부)
    def med_close_d():
        med_drl_call(
            med_movel,
            posx(0.00, 0.00, -145.00, 0.00, 0.00, 0.00),
            radius=0.00, ref=0, mod=DR_MV_MOD_REL,
            ra=DR_MV_RA_DUPLICATE, app_type=DR_MV_APP_NONE
        )

        # 순응 제어(compliance) 활성화 + stiffness 설정
        task_compliance_ctrl()
        set_stiffnessx([3000.00, 3000.00, 1.00, 200.00, 200.00, 200.00], time=0.0)

        # 목표 힘 설정(z축 - 방향)
        set_desired_force(
            [0.00, 0.00, -30.00, 0.00, 0.00, 0.00],
            [0, 0, 1, 0, 0, 0],
            time=0.0, mod=DR_FC_MOD_ABS
        )

        wait(1.50)

        # 툴힘의 z축 절댓값이 범위에 들어오면 1
        def force_z_in_range(min_n: float, max_n: float, ref):
            f = med_get_tool_force(ref)
            fz = float(f[2])
            mag = abs(fz)
            return 1 if (min_n <= mag <= max_n) else 0

        f = med_get_tool_force(DR_TOOL)
        print(f"[FORCE] ref=DR_TOOL Fx,Fy,Fz,Mx,My,Mz = {f} |Fz|={abs(float(f[2])):.3f} N")

        pos_now, z_now = med_get_tcp_posx_base()
        print(f"[Z] before CON2: Z(BASE)={z_now:.2f} mm, pos={pos_now}")

        # CON2: 힘이 유효 범위면 compliance release
        CON2 = force_z_in_range(1.0, 55.0, DR_TOOL)

        if CON2 == 1:
            wait(0.50)
            release_compliance_ctrl()

        # 원위치 복귀(상승)
        med_drl_call(
            med_movel,
            posx(0.00, 0.00, 145.00, 0.00, 0.00, 0.00),
            radius=0.00, ref=0, mod=DR_MV_MOD_REL,
            ra=DR_MV_RA_DUPLICATE, app_type=DR_MV_APP_NONE
        )

    # 트레이 접촉 상태에서 안전하게 후퇴(작은 REL 이동)
    def med_retreat_from_tray_contact():
        med_drl_call(
            med_movel,
            posx(-30.00, 0.00, 0.00, 0.00, 0.00, 0.00),
            radius=0.00, ref=0, mod=DR_MV_MOD_REL,
            ra=DR_MV_RA_DUPLICATE, app_type=DR_MV_APP_NONE
        )

    """
        하루 1칸에 대해 "1회 투약" 수행
        - handle_pose(PILL_A_HANDLE 또는 PILL_B_HANDLE)를 잡아 회전(movej)로 투약
        - 투약 후 System_home 복귀
    """
    def med_dose_once(handle_pose):
        med_retreat_from_tray_contact()

        med_drl_call(
            med_movel,
            med_System_home,
            vel=[200.00, 76.50], acc=[1000.00, 306.00],
            radius=0.00, ref=0, mod=DR_MV_MOD_ABS,
            ra=DR_MV_RA_DUPLICATE, app_type=DR_MV_APP_NONE
        )

        med_grip_open_rot()

        med_drl_call(
            med_movel,
            handle_pose,
            vel=[100.00, 68.25], acc=[500.00, 273.00],
            radius=0.00, ref=0, mod=DR_MV_MOD_ABS,
            ra=DR_MV_RA_DUPLICATE, app_type=DR_MV_APP_NONE
        )

        med_grip_close_rot()

        # 회전 투약
        med_drl_call(
            med_movej,
            posj(0.00, 0.00, 0.00, 0.00, 0.00, 190.00),
            radius=0.00, mod=DR_MV_MOD_REL, ra=DR_MV_RA_DUPLICATE
        )

        med_grip_open_rot()

        med_drl_call(
            med_movel,
            posx(0.00, 20.00, 0.00, 0.00, 0.00, 0.00),
            radius=0.00, ref=0, mod=DR_MV_MOD_REL,
            ra=DR_MV_RA_DUPLICATE, app_type=DR_MV_APP_NONE
        )

        med_drl_call(
            med_movej,
            posj(0.00, 0.00, 0.00, 0.00, 0.00, -190.00),
            vel=70.04, acc=100.00, radius=0.00,
            mod=DR_MV_MOD_REL, ra=DR_MV_RA_DUPLICATE
        )

        med_drl_call(
            med_movel,
            med_System_home,
            vel=[200.00, 76.50], acc=[1000.00, 306.00],
            radius=0.00, ref=0, mod=DR_MV_MOD_ABS,
            ra=DR_MV_RA_DUPLICATE, app_type=DR_MV_APP_NONE
        )

    """
        현재 트레이 접촉 포즈(tray_contact_abs)로 다시 이동
        - 투약 1회 후 다시 원래 트레이 위치로 돌아가기 위해 사용
    """
    def med_goto_tray_contact_abs(tray_contact_abs):
        med_drl_call(med_movel, med_System_home, radius=0.00, ref=0, mod=DR_MV_MOD_ABS,
                     ra=DR_MV_RA_DUPLICATE, app_type=DR_MV_APP_NONE)

        med_drl_call(med_movel, TRAY_PRE, vel=[250.00, 80.63], acc=[1000.00, 322.50],
                     radius=0.00, ref=0, mod=DR_MV_MOD_ABS,
                     ra=DR_MV_RA_DUPLICATE, app_type=DR_MV_APP_NONE)

        med_drl_call(med_movel, tray_contact_abs, radius=0.00, ref=0, mod=DR_MV_MOD_ABS,
                     ra=DR_MV_RA_DUPLICATE, app_type=DR_MV_APP_NONE)

    """
        트레이를 다음 요일 칸으로 밀기(REL +33mm) 후,
        현재 위치를 다시 읽어 tray_contact를 갱신
    """
    def med_push_tray_33mm_and_update():
        med_drl_call(
            med_movel,
            posx(33.00, 0.00, 0.00, 0.00, 0.00, 0.00),
            radius=0.00, ref=0, mod=DR_MV_MOD_REL,
            ra=DR_MV_RA_DUPLICATE, app_type=DR_MV_APP_NONE
        )
        return med__cur_posx_abs()

    """
        DB(daily_schedule)에서 day_of_week, pill_type을 조회하고
        pill A/B가 어떤 요일에 투약해야 하는지 set으로 정리해 반환.
        - normalize_day: MON/TUE... -> 월/화... 변환
        - parse_pill_type_field: 'PILL_A, PILL_B' 형태 처리 + NONE/빈값 처리
    """
    def med_load_schedule_from_db(qr_code: str):
        import mysql.connector

        conn = mysql.connector.connect(
            host=DB_HOST, port=DB_PORT,
            user=DB_USER, password=DB_PASS,
            database=DB_NAME
        )

        def normalize_day(d: str) -> str:
            d = str(d).strip()
            eng2kor = {"MON": "월", "TUE": "화", "WED": "수", "THU": "목", "FRI": "금", "SAT": "토", "SUN": "일"}
            if d.upper() in eng2kor:
                return eng2kor[d.upper()]
            return d

        def parse_pill_type_field(pill_type_str: str):
            s = (pill_type_str or "").strip()
            if not s or s.upper() == "NONE":
                return set()

            tokens = [t.strip() for t in s.split(",")]
            out = set()
            for t in tokens:
                u = t.upper().replace(" ", "")
                if u in ("PILL_A", "PILLA"):
                    out.add("PILL_A")
                elif u in ("PILL_B", "PILLB"):
                    out.add("PILL_B")
            return out

        try:
            cur = conn.cursor(dictionary=True)
            cur.execute(
                """
                SELECT day_of_week, pill_type
                FROM daily_schedule
                WHERE qr_code = %s
                """,
                (qr_code,)
            )
            rows = cur.fetchall()

            pill_days = {"PILL_A": set(), "PILL_B": set()}

            for r in rows:
                day = normalize_day(r.get("day_of_week", ""))
                pill_types = parse_pill_type_field(r.get("pill_type", ""))
                for p in pill_types:
                    pill_days[p].add(day)

            return pill_days

        finally:
            try:
                conn.close()
            except Exception:
                pass
    
    """
        [요일별 알약 투약 파트]
        - DB에서 pillA_days / pillB_days를 얻어
          day_order(일->월 순)로 트레이를 밀면서 해당 요일이면 dose_once 실행
        - tray_contact는 매 칸 이동 후 현재 위치로 갱신되어 drift/오차를 줄임
    """
    def med_run_pill_sequence_from_db(qr_code: str):
        pill_days = med_load_schedule_from_db(qr_code)
        node.get_logger().info(
            f"[MED][DB] qr_code={qr_code} "
            f"pillA_days={sorted(pill_days['PILL_A'])} "
            f"pillB_days={sorted(pill_days['PILL_B'])}"
        )

        # 트레이 시작 위치로 이동
        med_drl_call(med_movel, med_System_home, radius=0.00, ref=0, mod=DR_MV_MOD_ABS, ra=DR_MV_RA_DUPLICATE, app_type=DR_MV_APP_NONE)
        med_grip_open_rot()
        med_drl_call(med_movel, TRAY_PRE, vel=[250.00, 80.63], acc=[1000.00, 322.50], radius=0.00, ref=0, mod=DR_MV_MOD_ABS, ra=DR_MV_RA_DUPLICATE, app_type=DR_MV_APP_NONE)
        med_drl_call(med_movel, TRAY_PUSH_START, radius=0.00, ref=0, mod=DR_MV_MOD_ABS, ra=DR_MV_RA_DUPLICATE, app_type=DR_MV_APP_NONE)

        tray_contact = med__cur_posx_abs()

        day_order = ["일", "토", "금", "목", "수", "화", "월"]

        # Pill A
        for i, day in enumerate(day_order):
            if day in pill_days["PILL_A"]:
                node.get_logger().info(f"[MED][PILL_A] day={day} -> dose once")
                publish_status(f"[MED][PILL_A] day={day} -> dose once")
                med_dose_once(PILL_A_HANDLE)
                med_goto_tray_contact_abs(tray_contact)
            if i < 6:
                tray_contact = med_push_tray_33mm_and_update()

        # Pill B
        for i, day in enumerate(day_order):
            if day in pill_days["PILL_B"]:
                node.get_logger().info(f"[MED][PILL_B] day={day} -> dose once")
                publish_status(f"[MED][PILL_B] day={day} -> dose once")
                med_dose_once(PILL_B_HANDLE)
                med_goto_tray_contact_abs(tray_contact)
            if i < 5:
                tray_contact = med_push_tray_33mm_and_update()

        node.get_logger().info("[MED][PILL] A->B sequence finished, now continue full tail sequence.")

    """
        [MED 후반 고정 시퀀스]
        - 디스펜서/트레이/닫기 반복, 곡선 movec, amovel 등 포함
        - API 불일치는 med_drl_call이 흡수
    """
    def med_run_tail_sequence_original():
        med_drl_call(med_movel, posx(0.00, 0.00, 150.00, 0.00, 0.00, 0.00),
                     radius=0.00, ref=0, mod=DR_MV_MOD_REL,
                     ra=DR_MV_RA_DUPLICATE, app_type=DR_MV_APP_NONE)

        med_drl_call(med_movel, posx(602.58, -192.80, 150.90, 47.11, -179.10, 137.40),
                     radius=0.00, ref=0, mod=DR_MV_MOD_ABS,
                     ra=DR_MV_RA_DUPLICATE, app_type=DR_MV_APP_NONE)

        # 디스펜서 그리퍼 오픈 -> 내려가서 잡고 -> 이동 후 풀기
        med_grip_open_disp()

        med_drl_call(med_movel, posx(602.58, -192.80, 8.90, 47.11, -179.10, 137.40),
                     radius=0.00, ref=0, mod=DR_MV_MOD_ABS,
                     ra=DR_MV_RA_DUPLICATE, app_type=DR_MV_APP_NONE)

        med_grip_close_disp()
        wait(1.00)

        med_drl_call(med_movel, posx(456.00, 9.00, 6.99, 8.37, -179.37, 98.58),
                     radius=0.00, ref=0, mod=DR_MV_MOD_ABS,
                     ra=DR_MV_RA_DUPLICATE, app_type=DR_MV_APP_NONE)

        med_grip_open_disp()

        med_drl_call(med_movel, posx(0.00, 0.00, 100.00, 0.00, 0.00, 0.00),
                     radius=0.00, ref=0, mod=DR_MV_MOD_REL,
                     ra=DR_MV_RA_DUPLICATE, app_type=DR_MV_APP_NONE)

        med_drl_call(med_movel, med_System_home, radius=0.00, ref=0, mod=DR_MV_MOD_ABS,
                     ra=DR_MV_RA_DUPLICATE, app_type=DR_MV_APP_NONE)

        med_grip_close_rot()

        # close_d 반복(6회) + 미세 이동
        for _ in range(0, 6, 1):
            med_close_d()
            med_drl_call(med_movel, posx(32.00, 0.50, 0.00, 0.00, 0.00, 0.00),
                         radius=0.00, ref=0, mod=DR_MV_MOD_REL,
                         ra=DR_MV_RA_DUPLICATE, app_type=DR_MV_APP_NONE)
            med_drl_call(med_movel, posx(0.00, 0.00, 5.00, 0.00, 0.00, 0.00),
                         radius=0.00, ref=0, mod=DR_MV_MOD_REL,
                         ra=DR_MV_RA_DUPLICATE, app_type=DR_MV_APP_NONE)

        med_close_d()

        # 이후 amovel / movec 곡선 이동 등 원본 유지
        med_drl_call(med_amovel, posx(539.62, -80.00, 21.50, 169.56, -177.82, 167.01),
                     ref=0, mod=DR_MV_MOD_ABS, ra=DR_MV_RA_DUPLICATE,
                     app_type=DR_MV_APP_NONE)

        med_drl_call(
            med_movec,
            posx(526.00, 85.30, 17.11, 46.75, -178.24, 31.07),
            posx(440.50, 185.65, 17.43, 46.13, -178.04, 30.28),
            vel=[50.00, 20.64], acc=[1000.00, 82.56],
            radius=0.00, ref=0, angle=[0.00, 0.00],
            ra=DR_MV_RA_DUPLICATE, ori=DR_MV_ORI_TEACH, app_type=DR_MV_APP_NONE
        )

        med_drl_call(med_amovel, posx(0.00, 0.00, 165.00, 0.00, 0.00, 0.00),
                     ref=0, mod=DR_MV_MOD_REL, ra=DR_MV_RA_DUPLICATE,
                     app_type=DR_MV_APP_NONE)

        med_drl_call(med_movel, posx(450.51, -40.36, 21.10, 11.30, -179.48, 10.73),
                     radius=0.00, ref=0, mod=DR_MV_MOD_ABS,
                     ra=DR_MV_RA_DUPLICATE, app_type=DR_MV_APP_NONE)

        med_drl_call(med_movel, posx(0.00, 265.00, 0.00, 0.00, 0.00, 0.00),
                     radius=0.00, ref=0, mod=DR_MV_MOD_REL,
                     ra=DR_MV_RA_DUPLICATE, app_type=DR_MV_APP_NONE)

        med_drl_call(med_movel, med_System_home, radius=0.00, ref=0, mod=DR_MV_MOD_ABS,
                     ra=DR_MV_RA_DUPLICATE, app_type=DR_MV_APP_NONE)

    """
        MED 전체 실행 래퍼
        - 시작/종료/에러를 모니터링 토픽으로 publish
    """
    def med_execute(qr_code: str):
        try:
            node.get_logger().info(f"[MED][START] qr_code={qr_code}")
            publish_status(f"[MED][START] qr_code={qr_code}")
            med_run_pill_sequence_from_db(qr_code)
            med_run_tail_sequence_original()
            node.get_logger().info(f"[MED][DONE] qr_code={qr_code}")
            publish_status(f"[MED][DONE] qr_code={qr_code}")
        except Exception as e:
            node.get_logger().error(f"[MED][ERROR] failed: {e}")
            publish_status(f"[MED][ERROR] failed: {e}")

    """
        [노드 시작 시 1회 초기화]
        - tool/tcp/mode 설정
        - singular handling + 속도/가속 설정
        - set_velx 인자 불일치 이슈가 있어 clamp 인자를 제거한 형태로 호출
    """
    def init_robot_once():
        set_robot_mode(ROBOT_MODE_MANUAL)
        set_tool(ROBOT_TOOL)
        set_tcp(ROBOT_TCP)
        set_robot_mode(ROBOT_MODE_AUTONOMOUS)
        time.sleep(2)

        node.get_logger().info("#" * 50)
        node.get_logger().info(f"ROBOT_ID: {ROBOT_ID}")
        node.get_logger().info(f"ROBOT_MODEL: {ROBOT_MODEL}")
        node.get_logger().info(f"TOOL: {get_tool()}")
        node.get_logger().info(f"TCP: {get_tcp()}")
        node.get_logger().info(f"MODE(0 manual/1 auto): {get_robot_mode()}")
        node.get_logger().info("#" * 50)

        set_singular_handling(DR_AVOID)
        set_velj(60.0)
        set_accj(100.0)
        set_velx(250.0, 80.625)   # clamp 인자 없음
        set_accx(1000.0, 322.5)

    # =========================================================
    # 3) LIQUID 시퀀스
    # - DB(daily_schedule.liquid_type)가 NONE이 아니면 실행
    # - 시퀀스 자체는 "원본 라인/순서 유지"
    # - DRL API 인자 불일치 문제는 liq__drl_motion_call로 흡수
    # =========================================================
    liq__movel_raw  = req("movel")
    liq__movej_raw  = req("movej")
    liq__amovel_raw = getattr(dr, "amovel", None) # amovel은 없는 환경도 있어 안전 처리

    liq_get_tool_force   = req("get_tool_force")
    liq_get_current_posx = req("get_current_posx")

    liq_DR_MV_MOD_ABS = req("DR_MV_MOD_ABS")
    liq_DR_MV_MOD_REL = req("DR_MV_MOD_REL")
    liq_DR_MV_RA_DUPLICATE = req("DR_MV_RA_DUPLICATE")
    liq_DR_MV_APP_NONE     = req("DR_MV_APP_NONE")
    liq_DR_TOOL = req("DR_TOOL")
    liq_DR_BASE = req("DR_BASE")
    liq_DR_AVOID = req("DR_AVOID")

    """
        [LIQ용 DRL 호출 래퍼]
        - MED와 동일한 방식으로 API 버전 차이를 흡수
    """
    def liq__drl_motion_call(fn, *args, **kwargs):
        if "vel" in kwargs and isinstance(kwargs["vel"], (list, tuple)):
            kwargs["vel"] = kwargs["vel"][0]
        if "acc" in kwargs and isinstance(kwargs["acc"], (list, tuple)):
            kwargs["acc"] = kwargs["acc"][0]

        try:
            sig = inspect.signature(fn)
            kwargs = {k: v for k, v in kwargs.items() if k in sig.parameters}
        except (ValueError, TypeError):
            pass

        while True:
            try:
                return fn(*args, **kwargs)
            except TypeError as e:
                msg = str(e)
                if "unexpected keyword argument" in msg:
                    bad = msg.split("'")[1]
                    kwargs.pop(bad, None)
                    if not kwargs:
                        return fn(*args)
                    continue
                raise
    # liq 시퀀스에서 movel 호출은 이 래퍼로 통일
    def liq_movel(*args, **kwargs):
        return liq__drl_motion_call(liq__movel_raw, *args, **kwargs)

    # liq 시퀀스에서 movej 호출은 이 래퍼로 통일
    def liq_movej(*args, **kwargs):
        return liq__drl_motion_call(liq__movej_raw, *args, **kwargs)
    # amovel이 없는 환경이면 즉시 ImportError
    def liq_amovel(*args, **kwargs):
        if liq__amovel_raw is None:
            raise ImportError("DSR_ROBOT2에 'amovel'이 없습니다.")
        return liq__drl_motion_call(liq__amovel_raw, *args, **kwargs)

    liq_System_home = posx(367.27, 8.54, 206.25, 122.29, 179.95, 121.96)

    """
        tool force z축 절댓값이 [min_n, max_n] 범위면 1
        - CON1 같은 접촉/파지 성공 여부 판단 게이트로 사용
    """
    def liq_force_z_in_range(min_n: float, max_n: float, ref):
        f = liq_get_tool_force(ref)
        fz = float(f[2])
        mag = abs(fz)
        return 1 if (min_n <= mag <= max_n) else 0

    """
        현재 posx의 z 값이 범위면 1
        - CON2 같은 위치 기반 성공 여부 판단 게이트로 사용
    """
    def liq_pos_z_in_range(min_mm: float, max_mm: float, ref):
        cur = liq_get_current_posx(ref)
        pos = cur[0] if isinstance(cur, (tuple, list)) and len(cur) == 2 and isinstance(cur[0], (list, tuple)) else cur
        z = float(pos[2])
        return 1 if (min_mm <= z <= max_mm) else 0

    # liquid IO subroutines
    def liq_grip_close():
        set_digital_output(1, OFF)
        set_digital_output(2, ON)
        set_digital_output(3, ON)
        wait(1.50)

    def liq_grip_open():
        set_digital_output(1, ON)
        set_digital_output(2, OFF)
        set_digital_output(3, ON)
        wait(1.50)

    def liq_grip_close_liq():
        set_digital_output(1, OFF)
        set_digital_output(2, ON)
        set_digital_output(3, ON)
        wait(1.00)

    def liq_grip_open_liq():
        set_digital_output(1, ON)
        set_digital_output(2, OFF)
        set_digital_output(3, ON)
        wait(1.00)

    def liq_before_grip_liq():
        set_digital_output(1, OFF)
        set_digital_output(2, OFF)
        set_digital_output(3, ON)
        wait(1.00)

    def liq_hold_liq():
        set_digital_output(1, OFF)
        set_digital_output(2, OFF)
        set_digital_output(3, OFF)
        wait(1.00)

    def liq_customer_liq():
        set_digital_output(1, ON)
        set_digital_output(2, OFF)
        set_digital_output(3, ON)
        wait(1.00)

    """
        [LIQ 시퀀스 본체]
        - 로봇 초기 설정 후 CON1/CON2 조건이 만족되면 보정 시퀀스 수행
    """
    def run_liquid_sequence_once():
        set_robot_mode(ROBOT_MODE_MANUAL)
        set_tool(ROBOT_TOOL)
        set_tcp(ROBOT_TCP)

        set_robot_mode(ROBOT_MODE_AUTONOMOUS)
        time.sleep(2)

        node.get_logger().info("#" * 50)
        node.get_logger().info("Initializing robot with the following settings:")
        node.get_logger().info(f"ROBOT_ID: {ROBOT_ID}")
        node.get_logger().info(f"ROBOT_MODEL: {ROBOT_MODEL}")
        node.get_logger().info(f"ROBOT_TCP: {get_tcp()}")
        node.get_logger().info(f"ROBOT_TOOL: {get_tool()}")
        node.get_logger().info(f"ROBOT_MODE 0:수동, 1:자동 : {get_robot_mode()}")
        node.get_logger().info("#" * 50)

        set_singular_handling(liq_DR_AVOID)
        set_velj(60.0)
        set_accj(100.0)
        set_velx(250.0, 80.625)
        set_accx(1000.0, 322.5)

        # [1] 홈/준비 -> 고객 전달 위치로 접근
        liq_movel(posx(367.27, 8.54, 206.25, 122.29, 179.95, 121.96),
                  radius=0.00, ref=0, mod=liq_DR_MV_MOD_ABS, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        liq_before_grip_liq()

        liq_movel(posx(675.24, -73.95, 104.00, 2.89, 175.53, 93.28),
                  vel=[150.00, 72.38], acc=[1000.00, 289.50],
                  radius=0.00, ref=0, mod=liq_DR_MV_MOD_ABS, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        liq_customer_liq()

        liq_movel(posx(665.26, -72.48, 142.42, 2.19, 175.31, 92.43),
                  radius=0.00, ref=0, mod=liq_DR_MV_MOD_ABS, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        liq_movel(posx(571.30, 75.78, 126.94, 179.72, -175.67, -89.43),
                  radius=0.00, ref=0, mod=liq_DR_MV_MOD_ABS, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        liq_movel(posx(570.15, 74.78, 95.00, 178.16, -176.30, -91.05),
                  radius=0.00, ref=0, mod=liq_DR_MV_MOD_ABS, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        wait(0.50)

        liq_before_grip_liq()

        liq_movel(posx(0.00, 0.00, 150.00, 0.00, 0.00, 0.00),
                  radius=0.00, ref=0, mod=liq_DR_MV_MOD_REL, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        liq_movel(liq_System_home,
                  radius=0.00, ref=0, mod=liq_DR_MV_MOD_ABS, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        liq_before_grip_liq()

        liq_amovel(posx(197.79, -271.00, 206.66, 39.74, -179.74, 39.68),
                   ref=0, mod=liq_DR_MV_MOD_ABS, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        liq_movel(posx(0.00, 0.00, -67.44, 0.00, 0.00, 0.00),
                  radius=0.00, ref=0, mod=liq_DR_MV_MOD_REL, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        liq_hold_liq()

        liq_movel(posx(0.00, 0.00, 100.00, 0.00, 0.00, 0.00),
                  radius=0.00, ref=0, mod=liq_DR_MV_MOD_REL, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        liq_movel(posx(367.27, 8.54, 206.25, 122.29, 179.95, 121.96),
                  vel=[93.87, 67.74], acc=[500.00, 270.98],
                  radius=0.00, ref=0, mod=liq_DR_MV_MOD_ABS, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        liq_movel(posx(573.53, -72.06, 206.73, 31.94, -179.90, 31.83),
                  vel=[101.30, 68.36], acc=[1000.00, 273.43],
                  radius=0.00, ref=0, mod=liq_DR_MV_MOD_ABS, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        liq_movel(posx(0.00, 0.00, -67.44, 0.00, 0.00, 0.00),
                  radius=0.00, ref=0, mod=liq_DR_MV_MOD_REL, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        liq_before_grip_liq()

        liq_movel(posx(572.21, -64.64, 206.00, 148.46, 179.25, 147.71),
                  radius=0.00, ref=0, mod=liq_DR_MV_MOD_ABS, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        liq_movel(posx(572.15, -70.13, 168.91, 57.72, -178.64, 57.37),
                  radius=0.00, ref=0, mod=liq_DR_MV_MOD_ABS, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        liq_grip_close_liq()

        # [CON1] 힘 기반 접촉/파지 성공 판정
        # - 범위 만족 시 보정 루틴 실행
        CON1 = liq_force_z_in_range(4.5, 16.0, liq_DR_TOOL)

        if CON1 == 1:
            liq_movej(posj(0.00, 0.00, 0.00, 0.00, 0.00, -90.00),
                      radius=0.00, mod=liq_DR_MV_MOD_REL, ra=liq_DR_MV_RA_DUPLICATE)
            liq_grip_open_liq()
            liq_movej(posj(-8.20, 31.70, 48.97, 0.39, 100.08, -7.88),
                      radius=0.00, ra=liq_DR_MV_RA_DUPLICATE)
            liq_movel(posx(572.15, -75.13, 168.91, 57.72, -178.64, 57.37),
                      radius=0.00, ref=0, mod=liq_DR_MV_MOD_ABS, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)
            liq_grip_close_liq()
            liq_movej(posj(0.00, 0.00, 0.00, 0.00, 0.00, -90.00),
                      radius=0.00, mod=liq_DR_MV_MOD_REL, ra=liq_DR_MV_RA_DUPLICATE)
            liq_movej(posj(-8.20, 31.70, 48.97, 0.39, 100.08, -7.88),
                      radius=0.00, ra=liq_DR_MV_RA_DUPLICATE)

        liq_movel(posx(0.00, 0.00, 50.00, 0.00, 0.00, 0.00),
                  radius=0.00, ref=0, mod=liq_DR_MV_MOD_REL, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        liq_movel(posx(427.05, -73.31, 75.98, 169.10, 178.27, 170.30),
                  radius=0.00, ref=0, mod=liq_DR_MV_MOD_ABS, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        liq_movel(posx(425.05, -75.31, 15.15, 169.10, 178.27, 170.30),
                  radius=0.00, ref=0, mod=liq_DR_MV_MOD_ABS, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        liq_grip_open_liq()
        liq_before_grip_liq()

        liq_movel(posx(0.00, 0.00, 180.00, 0.00, 0.00, 0.00),
                  radius=0.00, ref=0, mod=liq_DR_MV_MOD_REL, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        liq_movej(posj(-10.99, -2.47, 133.06, 22.86, 6.48, 61.25),
                  radius=0.00, ra=liq_DR_MV_RA_DUPLICATE)

        liq_movel(posx(570.32, -80.31, 73.40, 174.72, -135.47, -92.44),
                  radius=0.00, ref=0, mod=liq_DR_MV_MOD_ABS, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        wait(1.00)

        liq_hold_liq()

        liq_movel(posx(0.00, 0.00, 100.00, 0.00, 0.00, 0.00),
                  vel=[50.00, 64.13], acc=[1000.00, 256.50],
                  radius=0.00, ref=0, mod=liq_DR_MV_MOD_REL, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        liq_movel(posx(605.44, 0.00, 166.68, 131.08, -103.02, 104.60),
                  vel=[50.00, 64.13], acc=[300.00, 256.50],
                  radius=0.00, ref=0, mod=liq_DR_MV_MOD_ABS, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        wait(2.00)

        liq_movel(posx(431.40, -47.21, 213.89, 28.68, -176.23, 116.56),
                  vel=[50.00, 64.13], acc=[1000.00, 256.50],
                  radius=0.00, ref=0, mod=liq_DR_MV_MOD_ABS, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        liq_movel(posx(563.33, -72.93, 75.37, 172.92, -129.69, -95.88),
                  vel=[49.26, 64.06], acc=[300.00, 256.26],
                  radius=0.00, ref=0, mod=liq_DR_MV_MOD_ABS, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        liq_before_grip_liq()

        liq_movej(posj(-10.99, -2.47, 133.06, 22.86, 6.48, 61.25),
                  radius=0.00, ra=liq_DR_MV_RA_DUPLICATE)

        liq_movel(posx(367.26, 8.57, 206.18, 132.35, 179.94, 132.02),
                  vel=[150.00, 72.38], acc=[100.00, 289.50],
                  radius=0.00, ref=0, mod=liq_DR_MV_MOD_ABS, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        liq_movel(posx(427.05, -73.31, 75.98, 169.10, 178.27, 170.30),
                  radius=0.00, ref=0, mod=liq_DR_MV_MOD_ABS, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        liq_grip_open()

        liq_movel(posx(427.05, -75.31, 30.15, 169.10, 178.27, 170.30),
                  vel=[250.00, 80.63], acc=[500.00, 322.50],
                  radius=0.00, ref=0, mod=liq_DR_MV_MOD_ABS, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        liq_grip_close()

        liq_movel(posx(0.00, 0.00, 206.00, 0.00, 0.00, 0.00),
                  radius=0.00, ref=0, mod=liq_DR_MV_MOD_REL, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        liq_movej(posj(-7.16, 32.40, 46.24, -0.34, 102.55, 172.56),
                  radius=0.00, ra=liq_DR_MV_RA_DUPLICATE)

        liq_movel(posx(0.00, 0.00, -35.00, 0.00, 0.00, 0.00),
                  radius=0.00, ref=0, mod=liq_DR_MV_MOD_REL, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        liq_grip_open()

        # [CON2] 위치(z) 기반 상태 판정 -> 조건 만족 시 보정 루틴 실행
        CON2 = liq_pos_z_in_range(170.0, 182.0, liq_DR_BASE)

        if CON2 == 1:
            liq_grip_close()
            liq_movej(posj(0.00, 0.00, 0.00, 0.00, 0.00, 90.00),
                      radius=0.00, mod=liq_DR_MV_MOD_REL, ra=liq_DR_MV_RA_DUPLICATE)
            liq_grip_open()
            liq_movej(posj(-7.16, 32.40, 46.24, -0.34, 102.55, 172.56),
                      radius=0.00, ra=liq_DR_MV_RA_DUPLICATE)
            liq_movel(posx(0.00, 0.00, -33.00, 0.00, 0.00, 0.00),
                      radius=0.00, ref=0, mod=liq_DR_MV_MOD_REL, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)
            liq_grip_close()
            liq_movej(posj(0.00, 0.00, 0.00, 0.00, 0.00, 90.00),
                      radius=0.00, mod=liq_DR_MV_MOD_REL, ra=liq_DR_MV_RA_DUPLICATE)
            liq_grip_open()
            liq_movej(posj(-8.20, 31.70, 48.97, 0.39, 100.08, -7.88),
                      radius=0.00, ra=liq_DR_MV_RA_DUPLICATE)

        liq_grip_open()
        liq_before_grip_liq()

        liq_movel(posx(0.00, 0.00, -65.44, 0.00, 0.00, 0.00),
                  radius=0.00, ref=0, mod=liq_DR_MV_MOD_REL, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        liq_hold_liq()

        liq_movel(posx(367.27, 8.54, 266.25, 122.29, 179.95, 121.96),
                  radius=0.00, ref=0, mod=liq_DR_MV_MOD_ABS, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        liq_amovel(posx(197.79, -271.00, 266.66, 39.74, -179.74, 39.68),
                   ref=0, mod=liq_DR_MV_MOD_ABS, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        liq_movel(posx(-10.00, 0.00, -120.44, 0.00, 0.00, 0.00),
                  radius=0.00, ref=0, mod=liq_DR_MV_MOD_REL, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        liq_before_grip_liq()

        liq_movel(posx(367.27, 8.54, 206.25, 122.29, 179.95, 121.96),
                  radius=0.00, ref=0, mod=liq_DR_MV_MOD_ABS, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        liq_before_grip_liq()

        liq_movel(posx(571.30, 75.78, 126.94, 179.72, -175.67, -89.43),
                  radius=0.00, ref=0, mod=liq_DR_MV_MOD_ABS, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        liq_movel(posx(570.15, 74.78, 95.00, 178.16, -176.30, -91.05),
                  radius=0.00, ref=0, mod=liq_DR_MV_MOD_ABS, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        liq_customer_liq()

        liq_movel(posx(665.26, -72.48, 142.42, 2.19, 175.31, 92.43),
                  radius=0.00, ref=0, mod=liq_DR_MV_MOD_ABS, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        liq_movel(posx(675.24, -73.95, 155.56, 2.89, 175.53, 93.28),
                  vel=[150.00, 72.38], acc=[1000.00, 289.50],
                  radius=0.00, ref=0, mod=liq_DR_MV_MOD_ABS, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        liq_movel(posx(675.24, -73.95, 94.00, 2.89, 175.53, 93.28),
                  vel=[150.00, 72.38], acc=[1000.00, 289.50],
                  radius=0.00, ref=0, mod=liq_DR_MV_MOD_ABS, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        wait(0.50)

        liq_before_grip_liq()

        liq_movel(liq_System_home,
                  radius=0.00, ref=0, mod=liq_DR_MV_MOD_ABS, ra=liq_DR_MV_RA_DUPLICATE, app_type=liq_DR_MV_APP_NONE)

        node.get_logger().info("LIQ DRL-style sequence finished.")

    """
        DB(daily_schedule.liquid_type)에서 액상 처방이 필요한지 판단.
        - liquid_type 필드가 존재하고,
          그 중 하나라도 NONE이 아니면 need=True
        - values는 로깅/검증을 위해 원본 문자열 리스트로 반환
    """
    def need_liquid_for_qr(qr_code: str):
        import mysql.connector

        conn = mysql.connector.connect(
            host=DB_HOST, port=DB_PORT,
            user=DB_USER, password=DB_PASS,
            database=DB_NAME
        )
        try:
            cur = conn.cursor(dictionary=True)
            cur.execute(
                """
                SELECT liquid_type
                FROM daily_schedule
                WHERE qr_code = %s
                """,
                (qr_code,)
            )
            rows = cur.fetchall()

            values = []
            for r in rows:
                v = (r.get("liquid_type") or "").strip()
                if v:
                    values.append(v)

            for v in values:
                if v.strip() and v.strip().upper() != "NONE":
                    return True, values

            return False, values
        finally:
            try:
                conn.close()
            except Exception:
                pass

    """
        LIQ 실행 래퍼
        - DB gate(need_liquid_for_qr)로 필요할 때만 1회 실행
    """
    def liq_execute(qr_code: str):
        try:
            need, vals = need_liquid_for_qr(qr_code)
            node.get_logger().info(f"[LIQ][DB] qr_code={qr_code} liquid_type_list={vals} need={need}")

            if not need:
                node.get_logger().info(f"[LIQ][SKIP] liquid sequence not needed for qr_code={qr_code}")
                return

            node.get_logger().info(f"[LIQ][START] liquid sequence for qr_code={qr_code}")
            publish_status(f"[LIQ][START] liquid sequence for qr_code={qr_code}")
            run_liquid_sequence_once()
            node.get_logger().info(f"[LIQ][DONE] liquid sequence for qr_code={qr_code}")
            publish_status(f"[LIQ][DONE] liquid sequence for qr_code={qr_code}")

        except Exception as e:
            node.get_logger().error(f"[LIQ][ERROR] failed: {e}")
            publish_status(f"[LIQ][ERROR] failed: {e}")

    # =========================================================
    # 단일 /robot_cmd 콜백 + 순차 실행
    # - system_busy로 중복 요청을 무시
    # - 실행 순서: MED -> LIQ -> DRAWER
    # =========================================================
    system_lock = threading.Lock()
    system_busy = {"value": False} # mutable container로 참조 공유

    """
        robot_cmd(=qr_code) 1건에 대한 통합 실행 스레드
        - system_busy를 True로 하고, 모든 시퀀스를 순차 실행
        - 종료 시 system_busy 해제 보장(finally)
    """
    def integrated_worker(qr_code: str):
        with system_lock:
            system_busy["value"] = True
        try:
            node.get_logger().info(f"[SYS][START] qr_code={qr_code}")

            # 실행 순서: MED -> LIQ -> DRAWER
            med_execute(qr_code)
            liq_execute(qr_code)
            drawer_execute(qr_code)

            node.get_logger().info(f"[SYS][DONE] qr_code={qr_code}")
            publish_status("finish") # UI/상위시스템에서 완료 트리거로 사용 가능
        except Exception as e:
            node.get_logger().error(f"[SYS][ERROR] integrated worker failed: {e}")
            publish_status(f"[SYS][ERROR] integrated worker failed: {e}")
        finally:
            with system_lock:
                system_busy["value"] = False

    """
        /robot_cmd 콜백
        - 빈 문자열이면 무시
        - busy이면 무시(또는 모니터링 publish)
        - 아니면 integrated_worker를 daemon thread로 실행
    """
    def on_robot_cmd(msg: String):
        qr_code = msg.data.strip()
        if not qr_code:
            node.get_logger().warn("robot_cmd received empty qr_code. ignore.")
            return

        with system_lock:
            if system_busy["value"]:
                node.get_logger().warn(f"system is busy. ignore qr_code={qr_code}")
                publish_status(f"system is busy. ignore qr_code={qr_code}")
                return

        threading.Thread(target=integrated_worker, args=(qr_code,), daemon=True).start()
    
    # robot_cmd 구독 생성
    node.create_subscription(String, "robot_cmd", on_robot_cmd, 10)

    # 초기 설정
    try:
        init_robot_once()
    # 초기화 실패면 노드 운용 불가 -> 즉시 종료
    except Exception as e:
        node.get_logger().error(f"Robot init failed: {e}")
        rclpy.shutdown()
        return

    # -------------------------
    # executor 구동
    # - num_threads=2: 콜백 + 다른 작업(예: 서비스 응답) 병렬 처리 여유

    executor = MultiThreadedExecutor(num_threads=2)
    executor.add_node(node)

    try:
        executor.spin()
    except KeyboardInterrupt:
        node.get_logger().info("Interrupted by user.")
    finally:
        # 모니터 스레드 종료 신호
        try:
            _drawer_mon_stop.set()
        except Exception:
            pass
        
        # executor/node 정리
        try:
            executor.shutdown()
        except Exception:
            pass
        try:
            node.destroy_node()
        except Exception:
            pass
        rclpy.shutdown()


if __name__ == "__main__":
    main()