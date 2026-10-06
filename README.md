# Python 实时对话运行器

用 Flask + 原生 HTML/CSS/JavaScript 实现的网页：粘贴 Python 代码后在独立子进程中执行，并与它实时对话（手动或自动）。

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
- **自动对话模式**：提交代码、步骤 JSON 数组和总时限（秒），后端按步骤自动等待输出文本、发送单行、关闭 stdin，并公布成功 / 失败 / 取消结果（详见下文）。
- **双路分栏输出**：stdout / stderr 分别按 UTF-8 **增量解码**并分栏增量呈现——未换行的片段也立即显示，跨读边界的多字节字符会被补齐，不丢字、不乱码。
- **互不阻塞**：两路输出各由独立线程读取；程序等待输入或一路无输出时，另一路输出、状态查询和终止请求仍可正常处理。
- **全站单运行**：同时仅允许一个未结束运行（手动与自动共用同一槽位）；运行中再次提交会收到 409。
- **退出码最后公布**：正常退出与主动终止都会先排空两路输出、`wait()` 回收进程，之后才公布退出码并允许重新运行；自动运行还要等脚本停止。
- **立即终止**：终止使用 SIGKILL，不等程序自行退出；自动运行被终止时脚本同时取消，后续步骤不再执行。
- **标识隔离**：旧 `run_id` 对新进程一律无效（409）；前端用轮次号 + run_id 双重校验，迟到回包不会混入新一轮页面；旧自动脚本在新运行开始前必须已停止，无法操作新进程。
- **状态即内存**：所有状态保存在内存中，服务重启即清空。

## 自动对话模式

在「自动对话模式」卡片中填写**步骤 JSON 数组**和**总时限（秒）**，点击「自动运行」即使用上方代码框中的程序启动一次自动对话。后端立即返回 `run_id`，随后由脚本线程依次执行各步骤；页面实时显示当前步骤，结束时显示成功、失败或取消结果。

### 步骤格式

步骤是一个 JSON 数组，每个元素是恰好包含下列一种键的对象：

| 步骤 | 含义 |
| --- | --- |
| `{"wait": "stdout", "text": "..."}` | 等待 stdout 出现指定文本（`"stderr"` 同理） |
| `{"send": "..."}` | 向 stdin 发送一行（不含换行的单行文本，服务端自动补换行） |
| `{"close_stdin": true}` | 关闭标准输入（EOF） |

语义约定：

- **跨片段匹配**：等待在该路**已到达的全部文本**上查找，目标文本被拆成多个无换行片段也能匹配。
- **每路游标**：每一路从上次匹配末尾继续查找，已匹配的文本不会被后续步骤重复消费。
- **不消耗页面事件**：脚本只读事件流，页面轮询到的输出一字不少。
- **总时限**：覆盖所有等待与阻塞中的发送；超时即失败并终止尚存进程。
- **手动干预**：自动运行占用单运行槽位，期间手动 `/input` 与 `/stdin/close` 返回 409；「终止」会取消脚本、停止后续动作。
- **结果判定**：全部步骤完成**且退出码为 0** 才成功；超时、写入失败、非零退出、或输出排空后仍有未完成步骤，均为失败，结果中注明失败位置（第几步）与原因。结果公布与重跑都要等输出排空、进程回收且脚本停止。

### 可执行样例

页面「自动对话」示例按钮会同时载入下面的代码与步骤（总时限填 30 即可）：

```python
import sys

# 提示语故意不换行：等待文本是跨片段匹配的
sys.stdout.write("请输入名字：")
sys.stdout.flush()
name = sys.stdin.readline().strip()
print(f"很高兴认识你，{name}！")

sys.stderr.write("准备接收第二行\n")   # stderr 也能作为等待目标
sys.stderr.flush()
print("输入 quit 退出：", end="", flush=True)
for line in sys.stdin:
    if line.strip() == "quit":
        print("再见！")
        break
```

步骤 JSON：

```json
[
  {"wait": "stdout", "text": "请输入名字："},
  {"send": "小明"},
  {"wait": "stdout", "text": "很高兴认识你，小明！"},
  {"wait": "stderr", "text": "准备接收第二行"},
  {"wait": "stdout", "text": "输入 quit 退出："},
  {"send": "quit"},
  {"wait": "stdout", "text": "再见！"}
]
```

预期结果：`✅ 自动运行成功：7 步全部完成，退出码 0`。

再举一个用到 `close_stdin` 的例子——程序读到 EOF 后统计字符数：

```python
import sys

print("准备就绪", file=sys.stderr, flush=True)
data = sys.stdin.read()          # 阻塞直到 EOF
print(f"共收到 {len(data)} 个字符")
```

```json
[
  {"wait": "stderr", "text": "准备就绪"},
  {"send": "你好"},
  {"send": "世界"},
  {"close_stdin": true},
  {"wait": "stdout", "text": "共收到 6 个字符"}
]
```

（`你好\n世界\n` 共 6 个字符。）

也可以用命令行启动自动运行并轮询结果：

```bash
curl -s -X POST http://localhost:8000/auto \
  -H 'Content-Type: application/json' \
  -d '{"code": "...", "steps": [...], "time_limit": 30}'
# => {"run_id": "..."}

curl -s "http://localhost:8000/events?run_id=...&after=0"
# 响应中的 auto 字段：{"active": false, "step": 7, "total": 7,
#   "desc": "...", "result": {"kind": "success", "reason": null,
#   "step": 7, "total": 7, "exit_code": 0, "elapsed": 0.123}}
```

`result.kind` 取值：`success`（成功）、`failure`（失败，`reason` 注明位置与原因）、`cancelled`（手动终止取消）。

## 内置示例

页面提供五个一键载入的示例：

1. **等待输入** —— 回声程序，逐行读取并回复，输入 `quit` 退出。
2. **双路无换行输出** —— stdout / stderr 交替输出无换行片段，验证增量呈现与 UTF-8 拼接。
3. **EOF** —— `sys.stdin.read()` 阻塞到 EOF，点击「关闭标准输入」后统计收到的内容。
4. **终止** —— 无限循环每秒打印，只能通过「终止」结束。
5. **自动对话** —— 同时载入代码与步骤 JSON，点「自动运行」演示自动模式。

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

自动模式下阻塞的发送同样受控：若步骤中的 `send` 因子进程不读输入而阻塞，超过总时限即判定失败并杀死进程解除阻塞；「终止」也会立即解除阻塞并取消脚本。

## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/run` | 手动模式启动运行，立即返回 `run_id`；已有未结束运行时返回 409 |
| POST | `/auto` | 自动模式启动：`{code, steps, time_limit}`，立即返回 `run_id`；参数非法 400，槽位被占 409 |
| GET | `/events?run_id=…&after=…` | 长轮询增量事件（stdout / stderr / meta 退出码与自动进度）；响应含 `auto` 进度快照 |
| POST | `/input` | 发送一行文本；stdin 已关闭或自动运行中时返回 409 |
| POST | `/stdin/close` | 关闭标准输入（EOF）；自动运行中时返回 409 |
| POST | `/terminate` | SIGKILL 立即终止；自动运行同时取消脚本 |
| GET | `/status` | 查询当前运行状态（含 `auto` 进度快照） |

## 安全说明

**仅供本机可信代码使用**：子进程与 Flask 同权限运行，仅适用于不派生进程的标准库代码，**没有任何安全沙箱**。请勿暴露到公网或运行不可信代码。
