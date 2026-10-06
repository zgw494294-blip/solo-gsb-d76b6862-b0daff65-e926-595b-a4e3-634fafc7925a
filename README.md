# Python 实时对话运行器

用 Flask + 原生 HTML/CSS/JavaScript 实现的网页：粘贴 Python 代码后在独立子进程中执行，并与它实时对话。

## 启动（单条命令）

```bash
docker compose up --build
```

容器启动时会自动安装依赖（Flask），然后启动服务。

- 网页地址： <http://localhost:8000>
- 端口： `8000`

停止：`Ctrl+C` 或 `docker compose down`。

不使用 Docker 时也可以本地运行：

```bash
pip install -r requirements.txt
python app.py
```

## 功能

- **粘贴即运行**：代码写入临时文件，在独立子进程（`python -u`）中执行，启动后立即返回运行标识 `run_id`。
- **实时对话**：可向子进程标准输入发送一行文本，或关闭标准输入（EOF）；关闭后再次写入会被拒绝（HTTP 409）。
- **自动对话模式**：提交代码、步骤 JSON 数组和总时限（秒），后端按步骤自动等待指定 stdout/stderr 文本、发送单行、关闭标准输入，立即返回 `run_id`；详见下文「自动对话模式」。
- **双路分栏输出**：stdout / stderr 分别按 UTF-8 **增量解码**并分栏增量呈现——未换行的片段也立即显示，跨读边界的多字节字符会被补齐，不丢字、不乱码。
- **互不阻塞**：两路输出各由独立线程读取；程序等待输入或一路无输出时，另一路输出、状态查询和终止请求仍可正常处理。
- **全站单运行**：同时仅允许一个未结束运行（含自动对话运行及其收尾阶段）；运行中再次提交会收到 409。
- **退出码最后公布**：正常退出与主动终止都会先排空两路输出、`wait()` 回收进程，之后才公布退出码并允许重新运行。
- **立即终止**：终止使用 SIGKILL，不等程序自行退出。
- **标识隔离**：旧 `run_id` 对新进程一律无效（409）；前端用轮次号 + run_id 双重校验，迟到回包不会混入新一轮页面。
- **状态即内存**：所有状态保存在内存中，服务重启即清空。

## 内置示例

页面提供五个一键载入的示例：

1. **等待输入** —— 回声程序，逐行读取并回复，输入 `quit` 退出。
2. **双路无换行输出** —— stdout / stderr 交替输出无换行片段，验证增量呈现与 UTF-8 拼接。
3. **EOF** —— `sys.stdin.read()` 阻塞到 EOF，点击「关闭标准输入」后统计收到的内容。
4. **终止** —— 无限循环每秒打印，只能通过「终止」结束。
5. **自动对话** —— 同时载入示例代码与步骤 JSON，点击「启动自动对话」即可看到完整自动交互。

## 自动对话模式

在「自动对话模式」区域填写**步骤 JSON 数组**和**总时限（秒）**，点击「启动自动对话」（或 `POST /auto`），后端启动子进程后立即返回 `run_id`，随后由脚本线程自动执行各步骤。

### 步骤格式

`steps` 为 JSON 数组，每个元素是一个对象，`action` 取值三种：

| action | 参数 | 说明 |
| --- | --- | --- |
| `wait` | `stream`: `"stdout"` 或 `"stderr"`；`text`: 非空字符串 | 等待指定一路输出中出现 `text` |
| `send` | `line`: 字符串（不含换行） | 向标准输入发送该行（自动附加换行） |
| `close` | 无 | 关闭标准输入（EOF） |

等待语义：

- **跨片段匹配**：输出事件可能把一句话切成多个片段，先在每路各自的缓冲里拼回再匹配，跨片段也能命中；
- **每路从上次匹配末尾继续**：同一 `stream` 上后一个 `wait` 从上一个 `wait` 匹配结束的位置继续找，不会重复命中同一段文本；
- **兼顾已到达及后续内容**：步骤开始前已经输出的文本（只要尚未被上次匹配越过）与等待期间新到的文本都参与匹配；
- **不消耗页面事件**：脚本只读取事件流，页面轮询 `/events` 不受影响，刷新页面可完整回放。

运行规则：

- 自动运行**占用同一单运行槽位**：期间 `POST /run`、`POST /auto` 返回 409，手动 `POST /input`、`POST /stdin/close` 也返回 409；
- 手动 `POST /terminate` 会**取消脚本**（停止后续动作）并 SIGKILL 终止进程，结果记为「已取消」；
- 发送阻塞（如超过管道容量的长行等待子进程读取）同样受**总时限**与**终止**控制；
- **成功**：全部步骤完成且进程退出码为 0；
- **失败**：总时限超时、写入失败、进程非零退出，或输出排空后仍有未完成步骤——结果注明失败位置（第几步）与原因，并终止尚存进程；
- 结果与重跑都等到**输出排空、进程回收、脚本停止**之后：结果通过 `/events` 中 `stream: "auto"` 的事件公布（`phase: "step"` 为步骤进展，`phase: "result"` 为最终结果），此前槽位不释放；旧脚本只持有旧运行对象，操作不到新运行。

### 可执行样例

代码（粘贴到代码框）：

```python
import sys

print("READY", flush=True)
name = sys.stdin.readline().strip()
print(f"你好，{name}！", flush=True)
print("提示：stderr 上的文本也能等待", file=sys.stderr, flush=True)
for line in sys.stdin:
    line = line.rstrip("\n")
    if line == "quit":
        print("BYE", flush=True)
        break
    print("ECHO " + line, flush=True)
```

步骤 JSON（填入「步骤 JSON 数组」框，总时限 30 秒）：

```json
[
  {"action": "wait", "stream": "stdout", "text": "READY"},
  {"action": "send", "line": "世界"},
  {"action": "wait", "stream": "stdout", "text": "你好，世界"},
  {"action": "wait", "stream": "stderr", "text": "stderr 上的文本也能等待"},
  {"action": "send", "line": "hello"},
  {"action": "wait", "stream": "stdout", "text": "ECHO hello"},
  {"action": "send", "line": "quit"},
  {"action": "wait", "stream": "stdout", "text": "BYE"}
]
```

点击「启动自动对话」，预期页面依次显示各步骤进展，最终显示「自动对话成功：全部 8 步完成，退出码 0」。页面「自动对话」示例按钮可一键载入本样例。

用 `curl` 等价复现：

```bash
RUN=$(curl -s localhost:8000/auto -H 'Content-Type: application/json' -d '{
  "code": "import sys\nprint(\"READY\", flush=True)\nname = sys.stdin.readline().strip()\nprint(f\"你好，{name}！\", flush=True)",
  "steps": [
    {"action": "wait", "stream": "stdout", "text": "READY"},
    {"action": "send", "line": "世界"},
    {"action": "wait", "stream": "stdout", "text": "你好，世界"}
  ],
  "timeout": 30
}' | python3 -c "import sys,json;print(json.load(sys.stdin)['run_id'])")

# 轮询事件，直到看到 stream=auto、phase=result 的成功事件
curl -s "localhost:8000/events?run_id=$RUN&after=0"
```

配合「EOF」示例代码可演示 `close` 动作：

```json
[
  {"action": "wait", "stream": "stdout", "text": "请发送若干行"},
  {"action": "send", "line": "第一行"},
  {"action": "send", "line": "第二行"},
  {"action": "close"},
  {"action": "wait", "stream": "stdout", "text": "共收到"}
]
```

## 长行复现（回归验证）

子进程**先持续写出大量 stdout、再读取 stdin** 时，发送一条超过管道容量（Linux 默认 64 KiB）的合法单行，可以复现输入同步问题：修复前发送请求与输出互相等待，两路输出停止刷新、`GET /events` 不再返回。

复现步骤：

1. 启动服务并打开 <http://localhost:8000>，粘贴运行下面的代码（输出总量远超管道容量，持续约 25 秒，随后进入读输入）：

   ```python
   import sys
   import time

   # 先持续写出大量 stdout：总量远超 64 KiB 管道容量
   for i in range(500):
       print(f"burst {i}: " + "y" * 300, flush=True)
       time.sleep(0.05)

   # 再读取 stdin：在刷输出期间发送一条超过 64 KiB 的长行
   print("输出完毕，等待长行…", flush=True)
   line = sys.stdin.readline()
   print(f"收到 {len(line.rstrip(chr(10)))} 个字符", flush=True)
   ```

2. 在另一个终端生成一条 200 KiB 的长行并复制：

   ```bash
   python3 -c "print('x' * 200000)"
   ```

3. 在程序仍在刷输出时，把长行粘贴到页面输入框并发送。

修复后的预期行为：

- 发送等待期间，stdout / stderr 两路输出持续被读取、页面持续刷新，`GET /events` 正常返回；
- 程序开始读输入后收到**完整的整行及末尾换行**——页面最终打印 `收到 200000 个字符`，不截断、不限长、不丢输出；
- 发送尚在等待时点击「终止」，子进程被立即 SIGKILL，等待中的发送按约定返回 409 结束；
- 发送成功后点击「关闭标准输入 (EOF)」，该行完整送达后 EOF 才生效，此后再写入返回 409；
- 运行结束后两路输出先排空、进程回收，再公布退出码并允许重跑。

## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/run` | 启动手动运行，立即返回 `run_id`；已有未结束运行时返回 409 |
| POST | `/auto` | 启动自动对话运行：`{code, steps, timeout}`，立即返回 `run_id`；槽位占用时返回 409，参数非法返回 400 |
| GET | `/events?run_id=…&after=…` | 长轮询增量事件（stdout / stderr / meta 退出码 / auto 步骤进展与结果） |
| POST | `/input` | 发送一行文本；stdin 已关闭或自动对话进行中时返回 409 |
| POST | `/stdin/close` | 关闭标准输入（EOF）；自动对话进行中时返回 409 |
| POST | `/terminate` | SIGKILL 立即终止；自动对话运行同时取消脚本 |
| GET | `/status` | 查询当前运行状态（含自动对话状态与结果） |

## 安全说明

**仅供本机可信代码使用**：子进程与 Flask 同权限运行，仅适用于不派生进程的标准库代码，**没有任何安全沙箱**。请勿暴露到公网或运行不可信代码。
