#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Runner 负责：
1) 监听来自 Dispatcher 的 RUN <commit> 指令（本地 TCP 端口）
2) 收到 RUN 后立刻回复 OK，然后后台执行测试（避免阻塞 Dispatcher）
3) 测试完成后，主动连接 Dispatcher 上报 RESULT <commit> <status> <seconds>
4) 定期向 Dispatcher 发送 HEARTBEAT <runner_host> <runner_port>（容错所需）
5) 启动时向 Dispatcher 发送 REGISTER <runner_host> <runner_port>
"""

import socket
import socketserver
import threading
import time
import argparse
import subprocess
import os
import random                      # ✅ 用于“随机失败”
from utils import log              # 统一日志

# ===== 与 dispatcher 保持一致的参数（可按需微调）=====
HEARTBEAT_SECS = 5.0              # 每隔 5s 上报一次心跳
CONNECT_TIMEOUT = 3.0             # 各类短连的超时
RANDOM_FAIL_PROB = 0.30           # ✅ 全部测试通过时，额外 30% 概率故意返回 FAIL

# ========== 运行时全局状态 ==========
class RunnerState:
    """
    只允许单并发（一个 Runner 一次只跑一个 commit）。
    如需多并发，可将 busy/curr_commit 扩展成队列 + 线程池。
    """
    def __init__(self):
        self.busy = False                     # 是否正在执行任务
        self.curr_commit = None               # 当前 commit
        self.lock = threading.Lock()          # 保护状态
        self.dispatcher = ("127.0.0.1", 8888) # Dispatcher 地址(默认)，由参数覆盖
        self.me = ("127.0.0.1", 9001)         # Runner 监听地址(默认)，由参数覆盖
        self.workdir = os.path.abspath("./runner_work")  # 预留的工作目录（当前示例未强依赖）

STATE = RunnerState()

# ================== 工具函数 ==================
def _send_line_to_dispatcher(line: str):
    """给 Dispatcher 发一条文本指令（短连接，一次性发送）。"""
    host, port = STATE.dispatcher
    with socket.create_connection((host, port), timeout=CONNECT_TIMEOUT) as s:
        s.sendall((line.strip() + "\n").encode("utf-8"))

def _safe_send_dispatcher(line: str):
    """带异常吞吐的版本，避免硬崩；失败时仅打印日志。"""
    try:
        _send_line_to_dispatcher(line)
    except Exception as e:
        log(f"[runner] send to dispatcher failed: {e}; line={line}")

def _ensure_dir(path: str):
    """确保目录存在。"""
    os.makedirs(path, exist_ok=True)

# ============ 业务：执行某个 commit 的测试 ============
def run_tests_for_commit(commit: str) -> tuple[str, float]:
    """
    真实环境：checkout -> 运行 pytest/unittest -> 解析返回值。
    本实现：直接在当前项目目录运行 pytest；若 pytest 全部通过，
            再以 30% 概率“故意返回 FAIL”（演示容错/重试用）。

    返回:
      status: "OK" 或 "FAIL"
      seconds: 执行耗时
    """
    start = time.time()

    # 1) 可选：准备工作目录（此示例未做 git 检出，直接在项目根目录跑 pytest）
    _ensure_dir(STATE.workdir)

    # 2) 真正执行 pytest（-q 安静模式；--maxfail=1 失败就尽早退出）
    #    cwd=当前进程启动目录（你的仓库根目录，含 tests/）。
    rc = subprocess.call(["pytest", "-q", "--maxfail=1"])
    base_status = "OK" if rc == 0 else "FAIL"

    # 3) 如果基础结果 OK，则叠加“随机失败”概率（默认 30%）
    if base_status == "OK":
        r = random.random()
        if r < RANDOM_FAIL_PROB:
            log(f"[runner] random-fail triggered (r={r:.3f} < p={RANDOM_FAIL_PROB:.2f}) for commit={commit}")
            status = "FAIL"
        else:
            log(f"[runner] random-pass    (r={r:.3f} ≥ p={RANDOM_FAIL_PROB:.2f}) for commit={commit}")
            status = "OK"
    else:
        status = "FAIL"

    seconds = time.time() - start
    return status, seconds

# ============ 任务执行线程 ============
def _worker_execute(commit: str):
    """在后台线程里跑测试，完成后向 Dispatcher 汇报 RESULT。"""
    try:
        log(f"[runner] start testing commit={commit}")
        status, seconds = run_tests_for_commit(commit)

        # 将结果汇报给 Dispatcher（文本协议）
        _safe_send_dispatcher(f"RESULT {commit} {status} {seconds:.3f}")

        log(f"[runner] finished commit={commit} status={status} seconds={seconds:.3f}")
    except Exception as e:
        log(f"[runner] error while running commit={commit}: {e}")
        _safe_send_dispatcher(f"RESULT {commit} FAIL 0.0")
    finally:
        # 不论成功失败，释放 busy 状态
        with STATE.lock:
            STATE.busy = False
            STATE.curr_commit = None

# ============ RUN 的 TCP 服务端（接受 Dispatcher 下发的 RUN 指令） ============
class RunHandler(socketserver.StreamRequestHandler):
    """
    协议：Dispatcher 会主动连到 Runner，发送一行：
        RUN <commit>
    期望 Runner 立即回复一行：
        OK   -> 接受任务并开始异步执行
        BUSY -> 当前正忙，无法接单
        ERR  -> 解析错误或其它问题
    """
    def handle(self):
        line = self.rfile.readline().decode("utf-8").strip()
        if not line:
            return

        parts = line.split()
        cmd = parts[0].upper()

        if cmd == "RUN" and len(parts) >= 2:
            commit = parts[1]

            with STATE.lock:
                if STATE.busy:
                    self.wfile.write(b"BUSY\n")
                    return
                STATE.busy = True
                STATE.curr_commit = commit

            self.wfile.write(b"OK\n")  # 立即应答，不阻塞 Dispatcher

            # 真正执行放到后台线程
            threading.Thread(target=_worker_execute, args=(commit,), daemon=True).start()
        else:
            self.wfile.write(b"ERR\n")

class RunTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True
    allow_reuse_address = True

# ============ 心跳线程 ============
def heartbeat_loop():
    """周期性向 Dispatcher 发送心跳。"""
    while True:
        try:
            host, port = STATE.me
            _send_line_to_dispatcher(f"HEARTBEAT {host} {port}")
        except Exception as e:
            log(f"[runner] heartbeat failed: {e}")
        time.sleep(HEARTBEAT_SECS)

# ============ 启动注册 ============
def register_once():
    """Runner 启动后向 Dispatcher 注册。"""
    host, port = STATE.me
    _send_line_to_dispatcher(f"REGISTER {host} {port}")

# ============ 主入口 ============
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dispatcher", default="127.0.0.1:8888", help="Dispatcher host:port")
    ap.add_argument("--host", default="127.0.0.1", help="Runner listen host for RUN")
    ap.add_argument("--port", type=int, default=9001, help="Runner listen port for RUN")
    args = ap.parse_args()

    # 保存配置到全局状态
    dh, dp = args.dispatcher.split(":")
    STATE.dispatcher = (dh, int(dp))
    STATE.me = (args.host, args.port)

    # 先注册
    register_once()

    # 启动心跳线程
    threading.Thread(target=heartbeat_loop, daemon=True).start()

    # 启动 RUN 服务
    with RunTCPServer(STATE.me, RunHandler) as server:
        log(f"[runner] listening for RUN on {STATE.me[0]}:{STATE.me[1]}")
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            log("[runner] shutting down...")

if __name__ == "__main__":
    main()
