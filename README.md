# asr-confluence

一个可嵌入 Node.js 20+ 应用的 TypeScript 库，负责接收同一会话中多个 ASR（语音识别）来源的 partial/final 片段，合并为可查询快照，并将每次有效变化以严格递增的修订流提供给下游消费者。存储完全基于本地 SQLite，不依赖 Redis、Kafka 或任何云服务。

## 安装

```bash
npm ci
npm run build
```

## 运行命令

| 命令 | 说明 |
|------|------|
| `npm ci` | 依据 `package-lock.json` 安装依赖 |
| `npm run build` | 将 TypeScript 编译到 `dist/` |
| `npm test` | 运行单元测试（不联网） |
| `npm run e2e` | 编译后运行端到端测试（多进程并发、断电重开、慢消费者，不联网） |

## 快速开始

```typescript
import { RecognitionStore } from 'asr-confluence';

const store = RecognitionStore.open({
  dbPath: '/var/lib/asr/session.db',
  busyTimeoutMs: 10000,
});

const session = store.session('call-abc-123');

// 摄入来自不同 source 的事件
session.ingest({
  eventId: 'evt-001',
  sourceId: 'agent-mic',
  sourceSeq: 1,
  type: 'partial',
  content: '你好世界',
});

session.ingest({
  eventId: 'evt-002',
  sourceId: 'agent-mic',
  sourceSeq: 1,
  type: 'final',
  content: '你好世界',
});

// 查询当前快照
const snap = session.getSnapshot();
console.log(snap.text);        // 合并后的全文
console.log(snap.revision);    // 当前修订号
console.log(snap.summary);     // 确定性摘要统计

// 创建消费者，以 cursor 续读
const consumer = store.consumer('call-abc-123', 'qc-engine-1');
const batch = consumer.read(100);
for (const rev of batch) {
  // 处理修订（rev.snapshotText 为该修订后的全文）
}
consumer.ack(batch[batch.length - 1].revision); // 确认后不再重发

// 也可以用 async iterator 轮询新修订
for await (const batch of consumer.stream({ pollIntervalMs: 200 })) {
  // 处理...
  consumer.ack(batch[batch.length - 1].revision);
}

store.close();
```

## 核心 API

### `RecognitionStore.open(options)`

打开（必要时创建）SQLite 数据库。选项：
- `dbPath` — 数据库文件路径
- `busyTimeoutMs` — 写锁等待超时（默认 10000ms）

### `store.session(sessionId): Session`

获取会话句柄。幂等，会话不存在时自动创建。

### `session.ingest(event): IngestResult`

原子地摄入一个事件，返回：
- `status` — `'accepted'`（产生了新修订）、`'duplicate'`（eventId 完全重复）、`'ignored'`（旧 partial 或内容未变）
- `revision` — 新修订号（duplicate 时返回原始修订号，ignored 时为 null）
- `reason` — 被忽略的原因

可能抛出：
- `EventConflictError` — 同一 eventId 但内容不同
- `SlotFinalizedError` — 同一 `(source, seq)` 已被 final 确认，尝试再次 final

### `session.getSnapshot(): Snapshot`

返回当前合并状态，包含按 sourceId 字典序、sourceSeq 升序排列的片段、全文文本、确定性摘要和当前修订号。

### `store.consumer(sessionId, consumerId): Consumer`

创建/恢复一个消费者。消费者以 `consumerId` 隔离各自的确认游标。

- `consumer.read(limit?)` — 读取游标之后的修订（不推进游标）
- `consumer.ack(revision)` — 确认修订（游标只进不退，崩溃后持久化）
- `consumer.getCursor()` — 当前游标位置
- `consumer.stream(options?)` — AsyncIterable，轮询并产出修订批次

## 数据模型与一致性保证

### 幂等与冲突

- 每个事件携带稳定的 `eventId`。完全相同的重复事件返回 `duplicate`，不产生新修订。
- 同一 `eventId` 但内容/source/seq/type 不同，抛出 `EventConflictError`，明确拒绝。

### 乱序与回退保护

- 同一 source 内，较旧（sourceSeq 更小）的 partial 不能覆盖更新的状态。
- 一旦某 `(source, seq)` 被 final 确认，迟到的 partial 被忽略。
- final 不可被另一个 eventId 重复确认（抛出 `SlotFinalizedError`）。
- final 可以乱序到达，快照始终按 sourceId + sourceSeq 排序，因此**同一事件集合无论分批与并发如何交错，最终文本、片段顺序和摘要完全一致**。

### 修订流

- 每次有效状态变化在单个 SQLite 事务中分配会话内严格递增的 revision。
- 修订记录包含触发事件、变化类型、变化后全文快照和摘要。
- 消费者以 `(consumerId, cursor)` 续读，已确认范围不重发，未确认范围不漏发。
- 游标通过 `MAX(old, new)` 保证只进不退。

### 持久性

- 所有写操作（事件记录、片段状态、修订插入、游标确认）均在 `BEGIN IMMEDIATE` 事务中完成。
- SQLite 配置 `WAL` + `synchronous=FULL`，事务提交后落盘。
- 进程被 `SIGKILL` 终止时，已提交事务完整保留，未提交事务完全回滚，不会出现只写一半的状态。
- 多进程并发写同一数据库时，由 SQLite 写锁 + `busy_timeout` 串行化。

## 关键取舍

| 决策 | 理由 |
|------|------|
| 使用 better-sqlite3（同步 API） | 嵌入式库场景下，同步事务天然保证原子性，避免异步交织导致的状态不一致；性能足以支撑质检系统吞吐。 |
| 片段按 `(sourceId, sourceSeq)` 建主键 | 每个 source 的每个 seq 至多一个有效片段，partial 被同 seq 的 final 覆盖后不保留历史版本；历史变化已完整记录在 revisions 表中。 |
| 快照全文存于每条修订 | 消费者无需重放全部历史即可获取任意时刻的完整文本；代价是存储空间随修订数线性增长，适合质检类需要审计追溯的场景。 |
| 消费者采用拉模型 + 轮询 | 不引入 pub/sub 或通知机制，保持嵌入式库的简单可靠；`stream()` 提供 async iterator 简化消费。 |
| `synchronous=FULL` | 以少量写入吞吐换取断电后的持久性保证，符合"状态、修订与游标在进程崩溃后保持一致"的要求。 |
| 无 TTL / 无压缩 | 库不自动删除修订或会话，由嵌入应用按合规需求管理生命周期。 |

## 项目结构

```
src/
  index.ts       公开 API 导出
  store.ts       RecognitionStore 入口
  session.ts     事件摄入、快照、修订分配（核心事务）
  consumer.ts    消费者游标、读取、确认、流式订阅
  db.ts          SQLite 连接、pragma、schema
  snapshot.ts    确定性快照/摘要构建
  hash.ts        事件内容指纹（冲突检测）
  errors.ts      自定义错误类型
test/
  unit/          幂等、乱序、final 保护、修订、消费者
  e2e/           多进程并发、SIGKILL 断电重开、慢消费者
```
