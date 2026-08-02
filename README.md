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

## 人工复核：片段修订租约

转写结果进入人工复核时，复核员必须先领取有时限的租约才能提交校正。竞争领取同一片段只有一个成功；过期或基于旧版本的提交以可区分的冲突错误结束。校正**不改写原始识别事件**，而是保留 actor、原因和 supersedes 谱系，并作为同一条耐久修订流中的新 revision 被现有消费者续读。

```typescript
const session = store.session('call-abc-123');

// 1. 复核员基于看到的快照 revision 领取租约
const lease = session.acquireLease({
  sourceId: 'agent-mic',
  sourceSeq: 1,
  actor: 'reviewer-alice',
  baseRevision: 5,        // 复核员看到的快照 revision
  ttlMs: 60000,           // 租约时限，默认 60s
});

// 2. 提交校正
const correction = session.submitCorrection({
  leaseId: lease.leaseId,
  correctedContent: '你好，世界',
  reason: '同音字错误：世届 -> 世界',
});
// correction.revision 是新的修订号，correction.supersedesCorrectionId 形成谱系

// 3. 现有消费者无需任何改动即可读到 correction-applied 修订
const consumer = store.consumer('call-abc-123', 'qc-engine');
const batch = consumer.read(100);
for (const rev of batch) {
  if (rev.changeType === 'correction-applied') {
    console.log(rev.correction!.actor, rev.correction!.reason);
    console.log(rev.snapshotText);  // 校正后的全文
  }
}
```

### 租约与校正 API

- `session.acquireLease(options): Lease` — 领取租约。若同一 `(sourceId, sourceSeq)` 上存在未过期的活跃租约，抛出 `LeaseBusyError`；过期后可被新领取者替换。
- `session.submitCorrection(options): Correction` — 基于租约提交校正。可能抛出：
  - `LeaseNotFoundError` — 租约不存在或已释放
  - `LeaseConsumedError` — 租约已被使用（一次性）
  - `LeaseExpiredError` — 租约已过期（`code: 'LEASE_EXPIRED'`）
  - `StaleBaseRevisionError` — 领取租约后片段内容发生了变化（`code: 'STALE_BASE_REVISION'`）
- `session.releaseLease(leaseId): boolean` — 主动释放租约
- `session.getLease(sourceId, sourceSeq): Lease | null` — 查询当前租约状态（读取时惰性过期）
- `session.getCorrectionsForFragment(sourceId, sourceSeq): Correction[]` — 查询校正谱系

校正提交后，该片段被标记为 `corrected`：
- 迟到的 partial 被忽略
- 不同 eventId 的 final 被拒绝（`SlotFinalizedError`），与 final 不回退规则一致

### 错误码对照

| 错误类 | `code` | 含义 |
|--------|--------|------|
| `LeaseBusyError` | `LEASE_BUSY` | 另一复核员持有活跃租约 |
| `LeaseExpiredError` | `LEASE_EXPIRED` | 租约超过 TTL |
| `LeaseConsumedError` | `LEASE_CONSUMED` | 租约已用于提交校正 |
| `LeaseNotFoundError` | `LEASE_NOT_FOUND` | 租约不存在或已释放 |
| `StaleBaseRevisionError` | `STALE_BASE_REVISION` | 租约期间片段内容被修改 |

## 会话归档：自包含交接

合规团队可将完整会话导出为自包含归档文件，交接至隔离环境。归档携带版本号、SHA-256 校验和、当前快照、完整修订历史、校正谱系（actor、baseRevision、原因、supersedes）和所有消费者游标位置。

```typescript
// 导出
await store.exportSession('call-abc-123', '/transfer/call-abc-123.asra');

// 在隔离环境导入
const result = isolatedStore.importSession('/transfer/call-abc-123.asra');
console.log(result.idempotent);  // 重复导入返回 true

// 导入后快照、修订历史、消费者游标与导出端等价
const snap = isolatedStore.session('call-abc-123').getSnapshot();
const consumer = isolatedStore.consumer('call-abc-123', 'qc-engine');
// consumer.getCursor() 与导出端一致，可直接续读
```

### 归档格式

二进制长度前缀段格式，便于流式生成和顺序读取：

```
ASRA (4B magic) | version (uint32 LE) | segments... | checksum trailer
```

每个段为 `type(1B) | length(uint32 LE) | JSON payload`。段类型：

| 类型 | 内容 |
|------|------|
| `H` | 头部：sessionId、导出时间、格式版本、各表行数 |
| `S` | 会话元数据行 |
| `E` | incoming_events（原始识别事件，不改写） |
| `F` | fragments（当前片段状态） |
| `R` | revisions（完整修订流） |
| `C` | corrections（校正记录，含 actor/reason/supersedes 谱系） |
| `L` | correction_leases（租约历史） |
| `U` | consumer_cursors（所有消费者续读位置） |
| `X` | 尾部校验段：前述全部字节的 SHA-256 + 行数校验 |

### 安全保证

- **篡改检测**：任何字节修改都会导致 SHA-256 不匹配，抛出 `ArchiveChecksumError`。
- **截断检测**：长度前缀声明与实际可用字节不符，或缺少尾部校验段，抛出 `ArchiveFormatError`。
- **计数校验**：头部和尾部双重记录的行数与实际段数逐一比对。
- **原子导入**：归档先完整解析和校验（在校验通过前不写任何数据），通过后在单个 `BEGIN IMMEDIATE` 事务中写入全部表。校验失败或进程中途被 kill 不会留下半导入会话。
- **幂等导入**：同一归档重复导入时，检测到会话已存在且内容一致则返回 `idempotent: true`；若目标会话存在但内容不同则抛出 `SessionExistsError`。
- **版本兼容**：格式版本高于当前支持版本时抛出 `ArchiveVersionError`；未知段类型被保留不丢弃，JSON 中未知字段自然透传，为后续版本预留空间。

### 错误码

| 错误类 | `code` | 含义 |
|--------|--------|------|
| `ArchiveFormatError` | `ARCHIVE_FORMAT` | 魔数错误、段截断、计数不匹配、缺少头部/尾部 |
| `ArchiveChecksumError` | `ARCHIVE_CHECKSUM` | SHA-256 校验失败（内容被篡改） |
| `ArchiveVersionError` | `ARCHIVE_VERSION` | 归档格式版本高于当前支持版本 |
| `SessionExistsError` | `SESSION_EXISTS` | 目标会话已存在且内容与归档不同 |

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

- 所有写操作（事件记录、片段状态、修订插入、游标确认、租约领取/消耗、校正记录）均在 `BEGIN IMMEDIATE` 事务中完成。
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
  store.ts       RecognitionStore 入口（含 exportSession/importSession）
  session.ts     事件摄入、快照、修订分配、租约与校正事务
  consumer.ts    消费者游标、读取、确认、流式订阅
  archive.ts     自包含归档格式、流式导出、校验与原子导入
  db.ts          SQLite 连接、pragma、schema 与迁移
  snapshot.ts    确定性快照/摘要构建
  hash.ts        事件内容指纹（冲突检测）
  errors.ts      自定义错误类型（含租约、归档冲突）
test/
  unit/          幂等、乱序、final 保护、修订、消费者、校正租约、归档
  e2e/           多进程并发、SIGKILL 断电重开、慢消费者、校正流程、归档故障注入
```
