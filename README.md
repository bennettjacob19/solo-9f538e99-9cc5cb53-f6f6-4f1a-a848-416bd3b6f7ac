# 双出口 VXLAN 演练（dual-egress VXLAN drill）

模拟虚拟机迁移到新宿主机后的出口切换：按 **(VNI, 目的 MAC)** 绑定/改绑出口，
积压在旧出口、尚未发出的帧会随改绑整体转到新隧道，与后续帧一起按入队顺序各发一次。

## 架构

```
                ┌──────────── FastAPI (/docs) ────────────┐
 注入帧 ──────► │ 绑定表 (vni,dst_mac) → egress            │
 绑定/改绑 ───► │                                          │
                │  egress A ──VXLAN/UDP──► receiver A :4789 │
                │  egress B ──VXLAN/UDP──► receiver B :4790 │
                └──────────────────────────────────────────┘
```

- 每个出口内部按 `(vni, dst_mac)` 维护**独立队列**（deque）+ 轮询调度；改绑只摘下
  该目的的队列整体挂到新出口——**O(1)，不扫描其他目的的积压**。
- 一把 `asyncio.Lock` 串行化入队/出队/改绑。**帧写入套接字成功后才出队、才计为已发**：
  发送循环在同一次持锁内窥视队首帧、写套接字、成功才摘下，因此改绑不会切开
  "发到一半"的帧。无论入队、发送、改绑如何并发，该目的的每帧**恰好发一次、按入队顺序**。
- 写入返回 **EAGAIN**（真实内核缓冲满，或经 API 模拟）时帧**不出队**：继续占用原出口
  容量、不计入已发，改绑成功就随目的队列整体迁走；按 `WRITE_RETRY_INTERVAL`
  （默认 60 ms）退避重试——连续不可写 1 秒写入最多约 17 次（< 20 次），解除后经事件
  立即唤醒，100 ms 内自动续发。
- 容量检查先于任何移动：新出口装不下该目的全部待发帧 → 拒绝改绑（409），
  原绑定和两边队列保持不变。
- 暂停只闸住本出口的发送循环，另一出口照常发送；暂停期间积压的帧仍可被改绑。
  一路写入受阻（EAGAIN）同样不影响另一路实收。

## 启动

```bash
# Docker
docker compose up --build

# 或本地
pip install -r requirements.txt
EGRESS_A_CAPACITY=100 EGRESS_B_CAPACITY=50 uvicorn app.main:app --port 8000
```

打开 <http://127.0.0.1:8000/docs>。

启动参数（环境变量）：

| 变量 | 默认 | 说明 |
|---|---|---|
| `EGRESS_A_CAPACITY` / `EGRESS_B_CAPACITY` | 100 / 100 | **两个出口各自的待发帧数上限**，满了拒绝入队 |
| `RECEIVER_A_PORT` / `RECEIVER_B_PORT` | 4789 / 4790 | 两个本地 UDP 接收端端口 |
| `RECEIVER_HOST` | 0.0.0.0 | 接收端绑定地址 |
| `SEND_HOST` | 127.0.0.1 | 出口发送目标（本演练为本机） |
| `SEND_INTERVAL` | 0 | 每帧发送间隔（秒），便于观察暂停/积压 |
| `WRITE_RETRY_INTERVAL` | 0.06 | EAGAIN 后的写入重试间隔（秒）：默认下连续不可写 1 秒写入最多约 17 次（< 20），解除后 100 ms 内续发 |
| `RECEIVED_BUFFER` | 5000 | 每个接收端保留的实收帧条数 |

## API 一览

| 方法/路径 | 作用 |
|---|---|
| `POST /api/bindings` | 按 VNI+目的 MAC 绑定或改绑出口；返回 `moved_pending`；容量不足 → 409 |
| `GET /api/bindings` | 查看绑定表 |
| `POST /api/frames` | 注入带编号的以太网测试帧（`count`、`seq_start`、`payload`）；出口满 → 409 |
| `POST /api/egresses/{A\|B}/pause` / `resume` | 暂停/恢复任一出口 |
| `POST /api/egresses/{A\|B}/block_writes` / `unblock_writes` | 模拟该出口套接字写入返回 EAGAIN / 解除；解除后未暂停且有积压的出口 100 ms 内自动续发 |
| `GET /api/egresses` | 两出口状态：上限、积压、暂停标志、已发数、**写入尝试次数**（`write_attempts`/`write_eagain`/`writes_blocked`）、各目的积压 |
| `GET /api/receivers/{A\|B}/frames` | 两个本地 UDP 接收端**实收**的 VXLAN 帧（VNI、双 MAC、序号、载荷） |
| `POST /api/receivers/reset` | 清空接收端计数与缓存 |
| `GET /api/overview` | 以上一切的汇总视图 |

### 演练示例（curl）

```bash
# 绑定 (VNI 100, MAC1) 到 A，暂停 A，注入 8 帧积压
curl -X POST localhost:8000/api/bindings -H 'content-type: application/json' \
     -d '{"vni":100,"dst_mac":"00:00:00:00:00:11","egress":"A"}'
curl -X POST localhost:8000/api/egresses/A/pause
curl -X POST localhost:8000/api/frames -H 'content-type: application/json' \
     -d '{"vni":100,"dst_mac":"00:00:00:00:00:11","count":8}'

# 改绑到 B：8 帧积压整体转走，B 按序发出
curl -X POST localhost:8000/api/bindings -H 'content-type: application/json' \
     -d '{"vni":100,"dst_mac":"00:00:00:00:00:11","egress":"B"}'
# → {"moved_pending": 8, ...}

# 查看 B 接收端实收帧
curl 'localhost:8000/api/receivers/B/frames?limit=20'
```

### EAGAIN 演练（curl）

```bash
# 模拟 A 的套接字写入返回 EAGAIN，注入 6 帧：积压但不算已发
curl -X POST localhost:8000/api/egresses/A/block_writes
curl -X POST localhost:8000/api/frames -H 'content-type: application/json' \
     -d '{"vni":100,"dst_mac":"00:00:00:00:00:11","count":6}'
# 1 秒内 write_attempts ≤ 20，sent=0，pending=6，接收端 A 实收 0
curl localhost:8000/api/egresses

# 改绑到 B：6 帧未发积压整体迁走，B 按序实收；解除 A 后 A 也不会再发这些帧
curl -X POST localhost:8000/api/bindings -H 'content-type: application/json' \
     -d '{"vni":100,"dst_mac":"00:00:00:00:00:11","egress":"B"}'
curl -X POST localhost:8000/api/egresses/A/unblock_writes
```

## 测试与实测

```bash
pip install -r requirements-dev.txt
pytest                 # 17 个用例：绑定/注入/暂停恢复/改绑/容量不足/并发改绑/EAGAIN 交接与重试
scripts/demo.py        # 对运行中的服务做实测演练（改绑 + 容量不足 + 满员拒绝 + EAGAIN 故障注入）
```

## 已知限制（未完成项）

- 纯内存实现，进程重启后绑定与队列丢失；无持久化、无鉴权。
- 接收端与转发面同进程同主机（本机演练）；未模拟真实网络中的丢包/乱序/分片。
- 仅实现 VXLAN 数据面封装（RFC 7348 头部），无控制面（EVPN/组播/ARP 学习）。
- 写入失败只处理 `BlockingIOError`（EAGAIN/EWOULDBLOCK，含 API 模拟）；其他
  `OSError`（如 EMSGSIZE）不自动重试，会让该出口的发送任务报错退出。
- 重试退避是固定间隔（`WRITE_RETRY_INTERVAL`），非指数退避；解除依赖
  `unblock_writes`/`resume` 事件唤醒或下一个重试周期（最坏一个间隔，默认 60 ms）。
- 接收端缓存有上限（`RECEIVED_BUFFER`），溢出后只保留最近 N 条。
