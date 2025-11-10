from datetime import datetime
import os

def log(msg: str):
    """统一日志输出：打印到终端并写入文件"""
    ts = datetime.now().strftime('%H:%M:%S.%f')[:-3]
    line = f"[{ts}] {msg}"

    # 1️⃣ 打印到终端
    print(line)

    # 2️⃣ 写入日志文件
    os.makedirs("test_results", exist_ok=True)  # 确保目录存在
    log_path = os.path.join("test_results", "ci_log.txt")
    with open(log_path, "a", encoding="utf-8") as f:
        print(line, file=f)
