#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dispatcher 负责：
1) 维护 Runner 列表与状态（是否 busy、最后心跳时间 last_seen)
2) 用 Round-Robin(轮询)在空闲 Runner 之间做负载均衡派发
3) 接收 Runner 的心跳(HEARTBEAT)，若超时则判定掉线并回收任务
4) 支持任务自动重试（最多 RETRY_MAX 次）
5) 记录每个 commit 的时间线:queued_at / assigned_at / completed_at 及分配到的 runner
"""

import socket
import socketserver
import threading
import queue
import time
import os
import argparse
from datetime import datetime
from collections import deque          # ✅ 轮询队列需要 deque
from utils import log

# ===== 调度/容错相关参数（可按需微调） =====
HEARTBEAT_SECS   = 5.0    # Runner 建议每 5s 上报一次心跳
RUNNER_DEAD_SECS = 15.0   # 超过该时长未见心跳 => 判定 Runner 掉线
RETRY_MAX        = 3      # 单个 commit 最大自动重试次数
ASSIGN_TICK      = 0.2    # 派发线程空转时的睡眠间隔
CLEANUP_TICK     = 1.0    # 清道夫线程巡检间隔
RESULTS_DIR      = "test_results"  # 测试结果输出目录

# ====== 全局状态对象 ======
class DispatcherState:
    """
    统一保存 Dispatcher 的全部共享状态。所有读写需要通过 self.lock 保证线程安全。
    """
    def __init__(self):
        # runners 映射：key = (host, port)，value = {"busy": bool, "last_seen": float}
        self.runners = {}

        # 轮询队列：按注册顺序保存所有 runner_id（(host,port) 元组），用于 Round-Robin
        self.runner_ring = deque()

        # 待分配的 commit 队列（先进先出）
        self.pending = queue.Queue()

        # 已分配但未完成：commit -> runner_id((host,port))
        self.assigned = {}

        # 任务重试与时间线：commit -> {"retry": int, "queued_at": ts, ...}
        self.tasks = {}

        # 记录每个 commit 的时间线与分配信息（用于结果落盘）
        # self.commits[commit] = {
        #   "queued_at": ts, "assigned_at": ts, "completed_at": ts, "runner": (host,port)
        # }
        self.commits = {}

        # 互斥锁，保障多线程访问一致性
        self.lock = threading.Lock()

    # --- 工具方法：确保结果目录存在 ---
    def _ensure_results_dir(self):
        os.makedirs(RESULTS_DIR, exist_ok=True)

    # --- 注册 Runner：加入 runners 字典与轮询队列 ---
    def register_runner(self, host, port):
        rid = (host, port)
        with self.lock:
            if rid not in self.runners:
                self.runners[rid] = {"busy": False, "last_seen": time.time()}
                self.runner_ring.append(rid)  # 放入轮询队列
        log(f"runner registered: {host}:{port}")

    # --- 记录心跳：仅更新 last_seen ---
    def heartbeat(self, host, port):
        rid = (host, port)
        with self.lock:
            r = self.runners.get(rid)
            if r is not None:
                r["last_seen"] = time.time()

    # --- 从系统中移除 Runner，并回收它正在处理的任务 ---
    def evict_runner(self, rid):
        with self.lock:
            # 回收该 runner 正在处理的所有 commit（自动重试）
            for commit, assigned_rid in list(self.assigned.items()):
                if assigned_rid == rid:
                    self._requeue_commit_locked(commit, reason="runner_evicted")

            # 真正移除 runner
            if rid in self.runners:
                del self.runners[rid]
            # 同步从轮询队列删除
            try:
                self.runner_ring.remove(rid)
            except ValueError:
                pass  # 不在 ring 里就忽略
        log(f"runner evicted: {rid[0]}:{rid[1]}")

    # --- 将 commit 放回 pending 队列并增加重试计数（需要在已持锁环境调用） ---
    def _requeue_commit_locked(self, commit, reason=""):
        # 取出/新建任务记录
        task = self.tasks.setdefault(commit, {"retry": 0})
        # 该 commit 不再处于“已分配”状态
        self.assigned.pop(commit, None)

        # 达到最大重试次数 => 丢弃
        if task["retry"] >= RETRY_MAX:
            log(f"drop commit={commit} after {task['retry']} retries (reason={reason})")
            return

        # 增加重试计数并重新入队
        task["retry"] += 1
        self.pending.put(commit)
        log(f"requeue commit={commit} retry={task['retry']} (reason={reason})")

    # --- Round-Robin 选取一个“当前空闲”的 runner；若都忙则返回 None ---
    def pick_idle_runner_rr(self):
        with self.lock:
            if not self.runner_ring:
                return None
            n = len(self.runner_ring)
            # 轮询 n 次，寻找第一个不 busy 的 runner
            for _ in range(n):
                rid = self.runner_ring[0]          # 查看队头
                self.runner_ring.rotate(-1)        # 把队头移到队尾（实现公平轮询）
                meta = self.runners.get(rid)
                if meta and (not meta["busy"]):
                    return rid
            return None

    # --- 标记 runner 的 busy 状态 ---
    def set_busy(self, rid, busy: bool):
        with self.lock:
            r = self.runners.get(rid)
            if r is not None:
                r["busy"] = busy

# 全局唯一状态
STATE = DispatcherState()

# ====== TCPServer 基类：多线程处理连接 ======
class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    daemon_threads = True         # handler 线程设为守护线程，随主进程退出
    allow_reuse_address = True    # 允许端口快速复用

# ====== 文本协议 Handler ======
class Handler(socketserver.StreamRequestHandler):
    """
    协议：一行一条指令（空格分隔），常见类型：
    - STATUS
    - REGISTER <runner_host> <runner_port>
    - HEARTBEAT <runner_host> <runner_port>
    - DISPATCH <commit>
    - RESULT <commit> <status> <seconds>
    """
    def handle(self):
        # 读取一行指令（阻塞直到读到换行或对端关闭）
        line = self.rfile.readline().decode("utf-8").strip()
        if not line:
            return

        parts = line.split()
        cmd = parts[0].upper()

        if cmd == "STATUS":
            # 返回可观测的队列与 runner 数量，便于外部探测
            with STATE.lock:
                runners = len(STATE.runners)
                pending = STATE.pending.qsize()
                assigned = len(STATE.assigned)
            self.wfile.write(f"OK RUNNERS {runners} PENDING {pending} ASSIGNED {assigned}\n".encode())

        elif cmd == "REGISTER" and len(parts) >= 3:
            # Runner 启动后主动注册：登记在册并进入轮询队列
            host, port = parts[1], int(parts[2])
            STATE.register_runner(host, port)
            self.wfile.write(b"REGISTERED\n")

        elif cmd == "HEARTBEAT" and len(parts) >= 3:
            # Runner 定期上报心跳：仅更新 last_seen
            host, port = parts[1], int(parts[2])
            STATE.heartbeat(host, port)
            self.wfile.write(b"ALIVE\n")

        elif cmd == "DISPATCH" and len(parts) >= 2:
            # 外部（如 repo_observer）提交一个需要测试的 commit
            commit = parts[1]
            # init 任务元数据与入队
            with STATE.lock:
                STATE.tasks.setdefault(commit, {"retry": 0})
                STATE.commits.setdefault(commit, {})["queued_at"] = time.time()
            STATE.pending.put(commit)
            self.wfile.write(b"QUEUED\n")
            log(f"queued commit {commit}")

        elif cmd == "RESULT" and len(parts) >= 4:
            # Runner 测试完成后回传结果：RESULT <commit> <status> <seconds>
            commit, status, seconds = parts[1], parts[2], parts[3]
            completed_at = time.time()

            with STATE.lock:
                # 从 assigned 中移除，释放 runner busy
                rid = STATE.assigned.pop(commit, None)
                if rid and rid in STATE.runners:
                    STATE.runners[rid]["busy"] = False

                # 记录完成时间
                meta = STATE.commits.setdefault(commit, {})
                meta["completed_at"] = completed_at

            # 写结果文件（含时间线信息）
            STATE._ensure_results_dir()
            path = os.path.join(RESULTS_DIR, f"{commit}.txt")

            with STATE.lock:
                meta = STATE.commits.get(commit, {})
                queued_at   = meta.get("queued_at")
                assigned_at = meta.get("assigned_at")
                runner_info = meta.get("runner")

            # 小函数：把时间戳格式化成人类可读
            def fmt_ts(ts_float):
                if ts_float is None:
                    return ""
                return datetime.fromtimestamp(ts_float).isoformat()

            # 计算各阶段耗时
            q_to_a = (assigned_at - queued_at) if (queued_at and assigned_at) else None
            a_to_c = (completed_at - assigned_at) if (assigned_at and completed_at) else None
            total  = (completed_at - queued_at) if (queued_at and completed_at) else None

            with open(path, "w", encoding="utf-8") as f:
                f.write(f"commit={commit}\n")
                f.write(f"status={status}\n")
                f.write(f"duration_seconds_runner={seconds}\n")  # Runner 回传的测试耗时
                f.write(f"queued_at_local={fmt_ts(queued_at)}\n")
                f.write(f"assigned_at_local={fmt_ts(assigned_at)}\n")
                f.write(f"completed_at_local={fmt_ts(completed_at)}\n")
                if runner_info:
                    f.write(f"runner_host={runner_info[0]}\nrunner_port={runner_info[1]}\n")
                if q_to_a is not None:
                    f.write(f"latency_queue_to_assign_sec={q_to_a:.3f}\n")
                if a_to_c is not None:
                    f.write(f"latency_assign_to_finish_sec={a_to_c:.3f}\n")
                if total is not None:
                    f.write(f"latency_total_sec={total:.3f}\n")

            self.wfile.write(b"ACK\n")
            log(f"result {commit}: {status} ({seconds}s) -> {path}")

        else:
            # 未知指令
            self.wfile.write(b"ERR\n")

# ====== 派发线程：Round-Robin 选择空闲 Runner 并下发任务 ======
def assigner_loop():
    while True:
        try:
            # 取出待分配 commit（阻塞直到拿到任务）
            commit = STATE.pending.get(timeout=ASSIGN_TICK)
        except queue.Empty:
            # 没有任务，稍作等待
            time.sleep(ASSIGN_TICK)
            continue

        assigned = False

        # 不断尝试找到空闲 runner（RR 轮询）
        rid = STATE.pick_idle_runner_rr()
        while rid is None:
            # 暂无空闲 runner，稍后重试
            time.sleep(ASSIGN_TICK)
            rid = STATE.pick_idle_runner_rr()

        # 找到空闲 runner 后，先把它标记为 busy，避免并发下被其他线程同时选中
        STATE.set_busy(rid, True)

        rhost, rport = rid
        try:
            # 按你的协议：Dispatcher 主动连 Runner 的 (host,port)，发送 "RUN <commit>"
            with socket.create_connection((rhost, rport), timeout=5) as s:
                s.sendall(f"RUN {commit}\n".encode())
                reply = s.recv(1024).decode().strip()

            if reply == "OK":
                # 分配成功：记录时间线与映射
                now = time.time()
                with STATE.lock:
                    STATE.assigned[commit] = rid
                    info = STATE.commits.setdefault(commit, {})
                    info["assigned_at"] = now
                    info["runner"] = rid
                log(f"dispatched {commit} -> {rhost}:{rport}")
                assigned = True
            else:
                # Runner 回应 BUSY/ERR：撤销 busy 状态，稍后再试
                STATE.set_busy(rid, False)

        except Exception as e:
            # 网络/连接失败：认为 runner 不可用，驱逐它并回收任务到重试队列
            log(f"runner {rhost}:{rport} unreachable: {e}")
            STATE.evict_runner(rid)

        if not assigned:
            # 本轮没分配成功：把 commit 放回队列并增加重试计数（加锁在内部处理）
            with STATE.lock:
                STATE._requeue_commit_locked(commit, reason="assign_failed")

# ====== 清道夫线程：清理心跳超时的 Runner 并回收任务 ======
def janitor_loop():
    while True:
        now = time.time()
        to_evict = []
        with STATE.lock:
            # 找出所有心跳超时的 runner
            for rid, meta in list(STATE.runners.items()):
                if now - meta["last_seen"] > RUNNER_DEAD_SECS:
                    to_evict.append(rid)
        # 移除并回收任务
        for rid in to_evict:
            log(f"runner timeout: {rid[0]}:{rid[1]}")
            STATE.evict_runner(rid)
        time.sleep(CLEANUP_TICK)

# ====== 主入口 ======
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")     # Dispatcher 监听地址
    ap.add_argument("--port", type=int, default=8888)  # Dispatcher 监听端口
    args = ap.parse_args()

    # 启动 TCP 指令服务（STATUS/REGISTER/HEARTBEAT/DISPATCH/RESULT）
    server = ThreadedTCPServer((args.host, args.port), Handler)
    log(f"listening on {args.host}:{args.port}")

    # 启动派发线程（负载均衡 + 自动重试）
    threading.Thread(target=assigner_loop, daemon=True).start()

    # 启动清道夫线程（心跳超时 -> 容错回收）
    threading.Thread(target=janitor_loop, daemon=True).start()

    # 主循环：处理指令请求
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log("shutting down...")

if __name__ == "__main__":
    main()
