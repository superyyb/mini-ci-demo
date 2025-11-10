#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Repo Observer 负责：
1) 定期 `git fetch` 远端，获取默认分支的最新 tip（origin/HEAD）
2) 发现新的 commit（与上次不同）即向 Dispatcher 发送一条：
      DISPATCH <commit>
3) 可选：打印 STATUS 观测信息（Runner 数量、排队任务数等）
"""

import subprocess
import time
import socket
import argparse
import os
from utils import log
import random
import textwrap
from datetime import datetime

CONNECT_TIMEOUT = 3.0

def git_rev_parse(repo_path: str, ref: str) -> str:
    """返回 git 引用的 SHA（例如 origin/HEAD）。"""
    return subprocess.check_output(
        ["git", "-C", repo_path, "rev-parse", ref],
        stderr=subprocess.STDOUT
    ).decode().strip()

def git_fetch(repo_path: str):
    """拉取最新远端引用。"""
    subprocess.check_call(["git", "-C", repo_path, "fetch", "--all", "--prune"])

def send_line(dispatcher_host: str, dispatcher_port: int, line: str) -> str:
    """发送一行指令到 Dispatcher，并返回一行响应（若有）。"""
    with socket.create_connection((dispatcher_host, dispatcher_port), timeout=CONNECT_TIMEOUT) as s:
        s.sendall((line.strip() + "\n").encode("utf-8"))
        # 读取一行回复；某些指令（比如 DISPATCH）会回 QUEUED
        resp = s.recv(1024).decode("utf-8").strip()
        return resp

# === [AUTOGEN] helpers ===
def git_config_user(repo_path: str):
    """
    确保本地 clone 有可用的 user.name/user.email（有些裸环境没有配置会导致 commit 失败）
    若已配置则不改。
    """
    def get(cfg_key):
        try:
            return subprocess.check_output(
                ["git", "-C", repo_path, "config", "--get", cfg_key],
                stderr=subprocess.STDOUT
            ).decode().strip()
        except subprocess.CalledProcessError:
            return ""

    if not get("user.name"):
        subprocess.check_call(["git", "-C", repo_path, "config", "user.name", "Autogen Bot"])
    if not get("user.email"):
        subprocess.check_call(["git", "-C", repo_path, "config", "user.email", "autogen@example.com"])


def autogen_tests_once(repo_path: str, rel_dir: str, num_tests: int, pass_prob: float):
    """
    在 repo 内的 rel_dir（例如 'tests/autogen'）生成 num_tests 个 pytest 文件，
    按 pass_prob 概率写入“必过”或“必挂”的断言，然后一次性 add/commit/push。
    """
    os.makedirs(os.path.join(repo_path, rel_dir), exist_ok=True)
    now = datetime.now().strftime("%Y%m%d_%H%M%S")

    created_files = []
    for i in range(num_tests):
        # 文件名唯一：时间戳 + 计数
        fname = f"test_autogen_{now}_{i}.py"
        fpath = os.path.join(repo_path, rel_dir, fname)

        should_pass = (random.random() < pass_prob)

        # 造一个极简可测的 pytest 测例，pass/ fail 可控
        body_ok = textwrap.dedent(f"""
        def test_autogen_pass_{now}_{i}():
            assert (1 + 1) == 2
        """).strip()

        body_fail = textwrap.dedent(f"""
        def test_autogen_fail_{now}_{i}():
            # 故意失败
            assert (2 + 2) == 5
        """).strip()

        content = body_ok if should_pass else body_fail

        with open(fpath, "w", encoding="utf-8") as f:
            f.write(content + "\n")

        created_files.append(os.path.join(rel_dir, fname))

    # git add / commit / push
    if created_files:
        git_config_user(repo_path)
        subprocess.check_call(["git", "-C", repo_path, "add", rel_dir])
        msg = f"autogen tests {now}: files={len(created_files)} pass_prob={pass_prob:.2f}"
        subprocess.check_call(["git", "-C", repo_path, "commit", "-m", msg])
        # 这里推送到 origin 的当前分支（Observer 默认看 origin/HEAD）
        subprocess.check_call(["git", "-C", repo_path, "push", "origin", "HEAD"])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("repo_path", help="Path to the observer clone")
    ap.add_argument("--dispatcher", default="127.0.0.1:8888",
                    help="Dispatcher host:port")
    ap.add_argument("--interval", type=float, default=5.0,
                    help="Polling interval seconds")

    # === [AUTOGEN] new flags ===
    ap.add_argument("--autogen-tests", type=int, default=0,
                    help="If >0, generate this many pytest files every cycle specified by --autogen-every")
    ap.add_argument("--autogen-every", type=float, default=0.0,
                    help="Seconds between autogen rounds. If 0, autogen runs on each poll interval.")
    ap.add_argument("--autogen-pass-prob", type=float, default=0.7,
                    help="Probability that an autogen test will pass (0~1)")
    ap.add_argument("--autogen-dir", default="tests/autogen",
                    help="Relative dir under repo to place generated tests")

    args = ap.parse_args()

    repo = os.path.abspath(args.repo_path)
    host_d, port_d = args.dispatcher.split(":")
    port_d = int(port_d)

    last = None
    log(f"[observer] watching {repo}; dispatcher={host_d}:{port_d}; interval={args.interval}s")

    # === [AUTOGEN] schedule bookkeeping ===
    last_autogen_ts = 0.0
    autogen_gap = args.autogen_every if args.autogen_every > 0 else args.interval

    while True:
        try:
            # === [AUTOGEN] periodic test generation & push ===
            if args.autogen_tests > 0:
                now_ts = time.time()
                if now_ts - last_autogen_ts >= autogen_gap:
                    try:
                        autogen_tests_once(
                            repo_path=repo,
                            rel_dir=args.autogen_dir,
                            num_tests=args.autogen_tests,
                            pass_prob=args.autogen_pass_prob
                        )
                        log(f"[observer] autogen pushed: n={args.autogen_tests}, dir={args.autogen_dir}, pass_prob={args.autogen_pass_prob:.2f}")
                    except subprocess.CalledProcessError as e:
                        log(f"[observer] autogen git error: {e}")
                    except Exception as e:
                        log(f"[observer] autogen error: {e}")
                    finally:
                        last_autogen_ts = now_ts

            # 1) 同步远端
            git_fetch(repo)

            # 2) 读取远端默认分支 tip（注意：这是远端的 HEAD 指向，如 origin/main）
            tip = git_rev_parse(repo, "origin/HEAD")

            # 3) 如果 tip 与上次不同，触发一个新的 DISPATCH
            if tip != last:
                resp = send_line(host_d, port_d, f"DISPATCH {tip}")
                log(f"[observer] new tip={tip[:12]} queued -> {resp}")
                last = tip
            else:
                log("[observer] no changes")

            # 4) 可选：探测当前系统状态（便于调试/监控）
            try:
                status = send_line(host_d, port_d, "STATUS")
                log(f"[observer] STATUS -> {status}")
            except Exception:
                pass

        except subprocess.CalledProcessError as e:
            log(f"[observer] git error: {e}")
        except Exception as e:
            log(f"[observer] network/other error: {e}")

        time.sleep(args.interval)


if __name__ == "__main__":
    main()
