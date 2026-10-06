"""与 Python 程序实时对话的网页后端。

- 粘贴的代码写入临时文件，在独立子进程中执行，启动后立即返回运行标识。
- stdout / stderr 各由一个线程读取，按 UTF-8 增量解码（跨读边界的
  多字节字符会被补齐，不丢字、不乱码），未换行的片段也立即入队。
- stdin 的写入与关闭由独立的锁串行化，阻塞写不占用事件锁：超过管道
  容量的长行会等待子进程开始读取，期间两路输出照常排空、/events 照常
  返回；整行连同末尾换行完整送达，不截断、不限长。终止会立即杀死子
  进程，等待中的写入按约定以 409 结束。
- 自动对话模式：提交代码、步骤 JSON 数组与总时限（秒），后端启动运行
  并立即返回 run_id，随后由脚本线程依次执行步骤——等待指定 stdout /
  stderr 文本、发送单行、关闭 stdin。等待在该路已到达的全部文本上匹配
  （天然跨片段），每路游标从上次匹配末尾继续，只读事件流、不消耗页面
  事件。全部步骤完成且退出码为 0 才成功；超时、写入失败、非零退出或
  排空后仍有未完成步骤均失败，注明失败位置与原因并终止尚存进程。
- 自动运行占用同一单运行槽位：期间手动输入与 EOF 返回 409，手动终止
  取消脚本并停止后续动作；阻塞中的发送同样受总时限与终止控制。
- 全站同时仅一个未结束运行；旧 run_id 对新进程一律无效。结果公布与
  重跑都要等输出排空、进程回收且脚本停止，因此旧脚本无法操作新运行。
- 正常退出与主动终止（SIGKILL，不等程序自行退出）都会先排空两路输出、
  回收进程，之后才公布退出码并允许重跑。
- 状态全部在内存中，进程重启即清空。仅供本机可信代码使用，无安全沙箱。
"""

import codecs
import math
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import uuid

from flask import Flask, jsonify, request, send_from_directory

app = Flask(__name__, static_folder="static", static_url_path="")

STATE_RUNNING = "running"
STATE_FINISHED = "finished"

CHUNK = 4096               # 每次从管道读取的原始字节数
LONG_POLL_SECONDS = 20     # /events 长轮询最长等待

_state_lock = threading.Lock()
_current_run = None        # 当前（可能是唯一未结束的）运行


class Run:
    """一次子进程运行：两路输出增量解码成事件流，退出码最后公布。"""

    def __init__(self, code):
        self.id = uuid.uuid4().hex
        self.events = []            # [{seq, stream, data}]，seq 从 1 递增
        self.seq = 0
        self.cond = threading.Condition()
        # 串行化 stdin 的写入与关闭：保证一行的字节连续完整、关闭不插队。
        # 阻塞写只持有它、不持有 cond，因此不会挡住输出泵和 /events。
        self.stdin_lock = threading.Lock()
        self.state = STATE_RUNNING
        self.exit_code = None
        self.stdin_closed = False
        self.auto = None            # 自动对话脚本（AutoScript），手动运行为 None

        self.tmpdir = tempfile.mkdtemp(prefix="webrun_")
        self.script = os.path.join(self.tmpdir, "main.py")
        with open(self.script, "w", encoding="utf-8") as f:
            f.write(code)

        env = dict(os.environ, PYTHONUNBUFFERED="1", PYTHONIOENCODING="utf-8")
        self.proc = subprocess.Popen(
            [sys.executable, "-u", self.script],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
            cwd=self.tmpdir,
            env=env,
        )
        self.pumps = [
            threading.Thread(target=self._pump,
                             args=("stdout", self.proc.stdout), daemon=True),
            threading.Thread(target=self._pump,
                             args=("stderr", self.proc.stderr), daemon=True),
        ]
        for t in self.pumps:
            t.start()
        threading.Thread(target=self._reaper, daemon=True).start()

    def _emit_locked(self, stream, data):
        self.seq += 1
        self.events.append({"seq": self.seq, "stream": stream, "data": data})
        self.cond.notify_all()

    def _emit(self, stream, data):
        with self.cond:
            self._emit_locked(stream, data)

    def _pump(self, stream, pipe):
        # 增量解码器：多字节 UTF-8 字符被两次 read 切开时会先缓存、
        # 等后半截到达后再输出，因此不丢字也不乱码；非法字节替换为 U+FFFD。
        decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        try:
            while True:
                chunk = os.read(pipe.fileno(), CHUNK)
                if not chunk:          # EOF：子进程关闭了这一路
                    break
                text = decoder.decode(chunk)
                if text:
                    self._emit(stream, text)
            tail = decoder.decode(b"", final=True)
            if tail:
                self._emit(stream, tail)
        except OSError:
            pass
        finally:
            try:
                pipe.close()
            except OSError:
                pass

    def _reaper(self):
        # 先等两路输出都读到 EOF（排空），再 wait() 回收进程，
        # 全部完成后才公布退出码——保证退出码之后不会再有迟到的输出。
        for t in self.pumps:
            t.join()
        code = self.proc.wait()
        with self.cond:
            self.exit_code = code
            self.state = STATE_FINISHED
            self._emit_locked("meta", {"exit_code": code})
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def write_line(self, line):
        """把整行（含末尾换行）写入子进程 stdin。

        成功返回 None，失败返回错误信息。超过管道容量的长行会阻塞到
        子进程开始读取；阻塞期间只持有 stdin_lock，输出泵与 /events
        照常工作。子进程退出或被杀死时，阻塞的写会以 BrokenPipeError
        解除，按约定转为拒绝。
        """
        payload = (line + "\n").encode("utf-8")
        with self.stdin_lock:
            with self.cond:
                if self.state != STATE_RUNNING:
                    return "运行已结束"
                if self.stdin_closed:
                    return "标准输入已关闭，写入被拒绝"
            try:
                # 单次 write 可能只写进一部分（如被信号打断），循环写满
                # 为止：程序开始读输入后必收到整行及末尾换行，绝不截断。
                view = memoryview(payload)
                while view:
                    n = self.proc.stdin.write(view)
                    if n is None:      # 阻塞式文件不会发生，防御性处理
                        n = len(view)
                    view = view[n:]
                self.proc.stdin.flush()
            except (BrokenPipeError, OSError):
                return "子进程已关闭标准输入"
        return None

    def close_stdin(self):
        """关闭子进程 stdin（发送 EOF）。

        与写入同用 stdin_lock：进行中的整行写完后关闭才生效，
        生效之后的写入一律被拒绝。
        """
        with self.stdin_lock:
            with self.cond:
                if not self.stdin_closed:
                    self.stdin_closed = True
                    try:
                        self.proc.stdin.close()   # 向子进程发送 EOF
                    except OSError:
                        pass

    def occupies_slot(self):
        """是否仍占用全站唯一的运行槽位。

        自动运行即使进程已回收，也要等脚本停止（结果公布）后才释放槽位，
        保证旧脚本不会与新运行并存。
        """
        with self.cond:
            if self.state == STATE_RUNNING:
                return True
            return self.auto is not None and not self.auto.done

    def auto_busy(self):
        """自动脚本仍在工作（结果未公布）时，手动输入与 EOF 被拒绝。"""
        with self.cond:
            return self.auto is not None and not self.auto.done

    def auto_snapshot_locked(self):
        """自动运行进度快照（调用方须持有 cond）；手动运行返回 None。"""
        auto = self.auto
        if auto is None:
            return None
        total = len(auto.steps)
        return {
            "active": not auto.done,
            "step": min(auto.step_index + 1, total) if total else 0,
            "total": total,
            "desc": auto.step_desc,
            "result": auto.result,
        }


class _Cancelled(Exception):
    """手动终止取消了自动脚本。"""


class _StepError(Exception):
    """自动脚本某一步失败（信息已注明位置与原因）。"""


def _short(text, limit=40):
    text = text.replace("\n", "\\n")
    return text if len(text) <= limit else text[:limit] + "…"


class AutoScript:
    """自动对话脚本：按步骤等待输出文本、发送单行、关闭 stdin。

    - 等待在「该路已到达的全部文本」上查找，天然支持跨片段匹配；每路
      游标从上次匹配末尾继续，不会重复消费已匹配的文本。
      脚本只读事件流（自记 seq 游标），不删除、不消耗页面事件。
    - 总时限覆盖所有等待与阻塞写：写长行阻塞时由独立线程执行，脚本
      线程按剩余时限轮询，超时即杀死进程解除阻塞并按失败处理。
    - 手动终止置取消标志并杀死进程，脚本停止后续动作，结果记为取消。
    - 结果在输出排空、进程回收之后才公布；公布前槽位仍被占用，
      因此旧脚本无法操作新运行。
    """

    def __init__(self, run, steps, time_limit):
        self.run = run
        self.steps = steps
        self.limit = time_limit
        self.started = time.monotonic()
        self.deadline = self.started + time_limit
        self.text = {"stdout": "", "stderr": ""}   # 每路已到达的全部文本
        self.cursor = {"stdout": 0, "stderr": 0}   # 每路匹配游标
        self.seq_seen = 0                          # 已读到的事件序号
        self.step_index = 0
        self.step_desc = ""
        self.cancelled = False
        self.done = False                          # 脚本已停止、结果已公布
        self.result = None
        self.thread = threading.Thread(target=self._main, daemon=True)

    def start(self):
        self.thread.start()

    def cancel(self):
        """手动终止：置取消标志并唤醒脚本；已公布的结果不可更改。"""
        with self.run.cond:
            if not self.done:
                self.cancelled = True
                self.run.cond.notify_all()

    # ---- 内部工具 ----

    def _kill(self):
        try:
            self.run.proc.kill()   # SIGKILL：立即终止，同时解除阻塞写
        except OSError:
            pass

    def _check_cancelled_locked(self):
        if self.cancelled:
            raise _Cancelled()

    def _ingest_locked(self):
        # 把新事件并入每路文本；只推进自己的游标，事件本身留给页面轮询。
        events = self.run.events
        while self.seq_seen < len(events):
            ev = events[self.seq_seen]
            self.seq_seen += 1
            if ev["stream"] in self.text:
                self.text[ev["stream"]] += ev["data"]

    def _set_step(self, index, desc):
        with self.run.cond:
            self.step_index = index
            self.step_desc = desc
        # 发一个 meta 事件让长轮询立即醒来，页面能及时看到当前步骤
        self.run._emit("meta", {"auto_step": {
            "step": min(index + 1, len(self.steps)) if self.steps else 0,
            "total": len(self.steps),
            "desc": desc,
        }})

    def _describe(self, cfg):
        if "wait" in cfg:
            return '等待 %s 出现 "%s"' % (cfg["wait"], _short(cfg["text"]))
        if "send" in cfg:
            return '发送一行："%s"' % _short(cfg["send"])
        return "关闭标准输入（EOF）"

    # ---- 步骤执行 ----

    def _step_wait(self, index, stream, needle):
        shown = _short(needle)
        with self.run.cond:
            while True:
                self._ingest_locked()
                buf = self.text[stream]
                pos = buf.find(needle, self.cursor[stream])
                if pos >= 0:
                    self.cursor[stream] = pos + len(needle)
                    return
                self._check_cancelled_locked()
                if self.run.state != STATE_RUNNING:
                    raise _StepError(
                        '第 %d 步：直到输出排空、进程回收，%s 中始终没有 "%s"'
                        % (index + 1, stream, shown))
                remaining = self.deadline - time.monotonic()
                if remaining <= 0:
                    raise _StepError(
                        '第 %d 步：等待 %s 出现 "%s" 超过总时限 %.3g 秒'
                        % (index + 1, stream, shown, self.limit))
                self.run.cond.wait(timeout=min(0.2, remaining))

    def _step_send(self, index, line):
        # 阻塞写放到守护线程：脚本线程按总时限与取消标志轮询，
        # 超时或终止时杀死进程，阻塞写随管道断开而结束。
        outcome = {}

        def do_write():
            outcome["error"] = self.run.write_line(line)

        t = threading.Thread(target=do_write, daemon=True)
        t.start()
        while True:
            t.join(timeout=0.1)
            if not t.is_alive():
                break
            with self.run.cond:
                if self.cancelled:
                    t.join(timeout=2)    # 终止已杀死进程，写随即断开
                    raise _Cancelled()
            if time.monotonic() >= self.deadline:
                self._kill()             # 解除阻塞写
                t.join(timeout=2)
                raise _StepError(
                    "第 %d 步：发送阻塞，超过总时限 %.3g 秒"
                    % (index + 1, self.limit))
        with self.run.cond:
            self._check_cancelled_locked()
        error = outcome.get("error")
        if error is not None:
            raise _StepError("第 %d 步：发送失败：%s" % (index + 1, error))

    def _step_close_stdin(self, index):
        with self.run.cond:
            self._check_cancelled_locked()
            if self.run.state != STATE_RUNNING:
                raise _StepError(
                    "第 %d 步：进程已结束、输出已排空，无法关闭标准输入"
                    % (index + 1))
        self.run.close_stdin()

    def _do_step(self, index, cfg):
        if "wait" in cfg:
            self._step_wait(index, cfg["wait"], cfg["text"])
        elif "send" in cfg:
            self._step_send(index, cfg["send"])
        else:
            self._step_close_stdin(index)

    def _wait_finish(self):
        """等待输出排空、进程回收（state 变为 finished），受总时限与取消约束。"""
        with self.run.cond:
            while self.run.state != STATE_FINISHED:
                self._check_cancelled_locked()
                remaining = self.deadline - time.monotonic()
                if remaining <= 0:
                    raise _StepError(
                        "全部步骤已完成，但进程在总时限 %.3g 秒内仍未退出"
                        % self.limit)
                self.run.cond.wait(timeout=min(0.2, remaining))

    def _wait_finish_best_effort(self):
        """失败/取消后进程已被杀死：等排空与回收完成再公布结果。"""
        with self.run.cond:
            while self.run.state != STATE_FINISHED:
                self.run.cond.wait(timeout=0.2)

    # ---- 主流程 ----

    def _main(self):
        kind = "success"
        reason = None
        try:
            for i, cfg in enumerate(self.steps):
                self._set_step(i, self._describe(cfg))
                self._do_step(i, cfg)
            self._set_step(len(self.steps), "全部步骤完成，等待进程退出")
            self._wait_finish()
        except _Cancelled:
            kind = "cancelled"
            reason = "手动终止，后续步骤已取消"
            self._wait_finish_best_effort()
        except _StepError as e:
            kind = "failure"
            reason = str(e)
            self._kill()                       # 终止尚存进程
            self._wait_finish_best_effort()
        else:
            if self.run.exit_code != 0:
                kind = "failure"
                reason = ("全部步骤完成，但退出码为 %d（非 0）"
                          % self.run.exit_code)
        total = len(self.steps)
        result = {
            "kind": kind,                      # success / failure / cancelled
            "reason": reason,
            "step": min(self.step_index + 1, total) if total else 0,
            "total": total,
            "exit_code": self.run.exit_code,
            "elapsed": round(time.monotonic() - self.started, 3),
        }
        # 结果与「脚本已停止」在同一临界区公布：此后槽位才释放、才允许重跑
        with self.run.cond:
            self.result = result
            self.done = True
            self.run._emit_locked("meta", {"auto": result})


def _validate_steps(steps):
    """校验步骤数组，返回错误信息或 None。

    支持的三种步骤（每步恰好包含其一）：
      {"wait": "stdout"|"stderr", "text": "..."}  等待指定文本
      {"send": "..."}                              发送单行（不含换行）
      {"close_stdin": true}                        关闭标准输入（EOF）
    """
    for i, s in enumerate(steps):
        if not isinstance(s, dict):
            return "步骤 %d 不是对象" % (i + 1)
        kinds = [k for k in ("wait", "send", "close_stdin") if k in s]
        if len(kinds) != 1:
            return "步骤 %d 必须且只能包含 wait / send / close_stdin 之一" % (i + 1)
        kind = kinds[0]
        if kind == "wait":
            if s["wait"] not in ("stdout", "stderr"):
                return "步骤 %d：wait 只能是 stdout 或 stderr" % (i + 1)
            if not isinstance(s.get("text"), str) or not s["text"]:
                return "步骤 %d：等待文本 text 必须是非空字符串" % (i + 1)
        elif kind == "send":
            if not isinstance(s["send"], str):
                return "步骤 %d：send 必须是字符串" % (i + 1)
            if "\n" in s["send"] or "\r" in s["send"]:
                return "步骤 %d：send 只能是不含换行的单行文本" % (i + 1)
        elif s["close_stdin"] is not True:
            return "步骤 %d：close_stdin 必须为 true" % (i + 1)
    return None


def _get_run(run_id):
    """只有与当前运行匹配的 run_id 才有效；旧标识控制不了新进程。"""
    with _state_lock:
        run = _current_run
    if run is None or run.id != run_id:
        return None
    return run


@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.post("/run")
def start_run():
    global _current_run
    code = (request.get_json(force=True, silent=True) or {}).get("code", "")
    if not code.strip():
        return jsonify({"error": "代码为空"}), 400
    with _state_lock:
        if _current_run is not None and _current_run.occupies_slot():
            return jsonify({"error": "已有运行未结束，请先终止"}), 409
        run = Run(code)
        _current_run = run
    # 子进程已启动，立即返回运行标识
    return jsonify({"run_id": run.id})


@app.post("/auto")
def start_auto():
    """自动对话模式：提交代码、步骤数组与总时限，立即返回 run_id。"""
    global _current_run
    data = request.get_json(force=True, silent=True) or {}
    code = data.get("code", "")
    if not isinstance(code, str) or not code.strip():
        return jsonify({"error": "代码为空"}), 400
    limit = data.get("time_limit")
    if (not isinstance(limit, (int, float)) or isinstance(limit, bool)
            or not math.isfinite(limit) or limit <= 0):
        return jsonify({"error": "总时限 time_limit 必须是正数（秒）"}), 400
    steps = data.get("steps")
    if not isinstance(steps, list):
        return jsonify({"error": "steps 必须是步骤数组"}), 400
    error = _validate_steps(steps)
    if error is not None:
        return jsonify({"error": error}), 400
    with _state_lock:
        if _current_run is not None and _current_run.occupies_slot():
            return jsonify({"error": "已有运行未结束，请先终止"}), 409
        run = Run(code)
        run.auto = AutoScript(run, steps, float(limit))
        _current_run = run
    # 子进程已启动，立即返回运行标识；脚本线程随后驱动各步骤
    run.auto.start()
    return jsonify({"run_id": run.id})


@app.get("/events")
def events():
    """长轮询：返回 seq > after 的新事件；run_id 不匹配即返回 409。"""
    run_id = request.args.get("run_id", "")
    try:
        after = int(request.args.get("after", "0"))
    except ValueError:
        after = 0
    run = _get_run(run_id)
    if run is None:
        return jsonify({"error": "运行标识已失效", "status": "stale"}), 409
    deadline = time.monotonic() + LONG_POLL_SECONDS
    with run.cond:
        # 自动运行公布结果前（脚本未停止）也继续等待，避免页面空转
        while (run.seq <= after and run.occupies_slot()
               and time.monotonic() < deadline):
            run.cond.wait(timeout=0.5)
        fresh = [e for e in run.events if e["seq"] > after]
        return jsonify({
            "run_id": run.id,
            "status": run.state,
            "exit_code": run.exit_code,
            "stdin_closed": run.stdin_closed,
            "auto": run.auto_snapshot_locked(),
            "events": fresh,
        })


@app.post("/input")
def send_input():
    data = request.get_json(force=True, silent=True) or {}
    run = _get_run(data.get("run_id", ""))
    if run is None:
        return jsonify({"error": "运行标识已失效"}), 409
    if run.auto_busy():
        return jsonify({"error": "自动对话运行中，手动输入被禁用"}), 409
    # 长行可能在此阻塞等待子进程读取，但不影响输出排空与 /events
    error = run.write_line(data.get("line", ""))
    if error is not None:
        return jsonify({"error": error}), 409
    return jsonify({"ok": True})


@app.post("/stdin/close")
def close_stdin():
    data = request.get_json(force=True, silent=True) or {}
    run = _get_run(data.get("run_id", ""))
    if run is None:
        return jsonify({"error": "运行标识已失效"}), 409
    if run.auto_busy():
        return jsonify({"error": "自动对话运行中，手动关闭标准输入被禁用"}), 409
    run.close_stdin()
    return jsonify({"ok": True})


@app.post("/terminate")
def terminate():
    data = request.get_json(force=True, silent=True) or {}
    run = _get_run(data.get("run_id", ""))
    if run is None:
        return jsonify({"error": "运行标识已失效"}), 409
    if run.auto is not None:
        run.auto.cancel()   # 先取消脚本：停止后续动作，结果记为取消
    try:
        run.proc.kill()   # SIGKILL：立即终止，不等程序自行退出
    except OSError:
        pass
    return jsonify({"ok": True})


@app.get("/status")
def status():
    with _state_lock:
        run = _current_run
    if run is None:
        return jsonify({"state": "idle"})
    with run.cond:
        return jsonify({"state": run.state, "run_id": run.id,
                        "exit_code": run.exit_code,
                        "auto": run.auto_snapshot_locked()})


if __name__ == "__main__":
    # threaded=True：等待输入或一路无输出时，其余请求仍可并发处理
    app.run(host="0.0.0.0", port=8000, threaded=True)
