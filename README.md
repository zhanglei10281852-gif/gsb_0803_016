# asr-transcript-store

把多个 ASR 识别器（source）的 partial/final 输出汇聚成一致转写状态的 Node.js 20 嵌入式 TypeScript 库：

- 接收同一会话内多个 source 的事件流，容忍重复、乱序、并发到达；
- 提供可查询的确定性快照（最终文本、片段顺序、摘要不随到达顺序变化）；
- 每次有效变化产生会话内严格递增的 revision，作为耐久修订流供下游质检消费；
- 消费者凭 `consumerId` + 服务端游标续读：已确认不重发，未确认不漏发；
- 全部状态存本地 SQLite（WAL），单事务写入，进程崩溃后不会出现半写状态。不依赖 Redis/Kafka/云服务。

## 构建与验证

```bash
npm ci          # 安装依赖（使用提交的 package-lock.json）
npm run build   # tsc -> dist/
npm test        # vitest 单元测试（离线）
npm run e2e     # 多实例并发 / 断电重开 / 慢消费者三个场景（离线，跑在 dist 上）
```

## 集成方式

```ts
import { TranscriptStore, ConflictError } from 'asr-transcript-store';

const store = new TranscriptStore('/var/lib/qa/transcripts.db');

// 1) 接入识别器输出
try {
  const r = store.ingest('session-42', 'mic-0', {
    eventId: 'mic-0-e17',
    sourceSeq: 17,
    kind: 'partial',      // 或 'final'
    text: '你好 世界',
    startMs: 12500,       // 可选，用于确定性排序
  });
  // r.status: 'applied' | 'duplicate' | 'stale'；r.revision 为会话当前最大修订号
} catch (e) {
  if (e instanceof ConflictError) { /* 同 eventId 不同内容：数据有 bug，告警 */ }
  else throw e;
}

// 2) 查询快照
const snap = store.snapshot('session-42');
// snap.text      最终文本（final 片段以单个空格连接）
// snap.segments  已确认 final 片段，确定性排序
// snap.partials  每个 source 当前暂态 partial（至多一条）
// snap.summary   { sources, finalSegments, partialSegments, characters }

// 3) 下游质检消费修订流
const { entries } = store.poll('qc-worker-1', 'session-42', 100);
for (const e of entries) {
  await qc.handle(e.change);           // 处理失败则不要 ack，下次 poll 会重投
  store.ack('qc-worker-1', 'session-42', e.revision);
}
```

同一数据库文件可以被多个 `TranscriptStore` 实例（同进程或不同进程）同时读写；写操作串行化在 SQLite 层完成。

## 摄入语义

| 情况 | 行为 |
| --- | --- |
| 同 `eventId` + 完全相同内容 | 幂等成功，返回 `duplicate`，不产生 revision |
| 同 `eventId` + 不同内容 | 抛 `ConflictError`，状态不变 |
| partial，`sourceSeq` ≤ 该 source 已见 partial/final 最大 seq | 丢弃，返回 `stale`（较旧 partial 不会覆盖更新状态，也不会回退已确认 final） |
| 新 partial | 取代该 source 上一条暂态 partial（快照中每 source 至多一条 partial） |
| final | 总是提交（即使乱序晚于更高 seq 的 partial 到达），并清除 ≤ 其 seq 的暂态 partial |

每个被应用（`applied`）的变化在单个事务里：写入事件、把会话 `last_revision` +1、追加 revision 记录。崩溃只可能丢失整个未提交事务，不会留下一半。

## 确定性

快照排序只依赖事件内容（`startMs, sourceId, sourceSeq, eventId`），与到达顺序无关。因此同一事件集合无论分批、乱序、并发如何交错，最终文本、片段顺序和摘要都一致（有属性式测试与 e2e 交叉验证）。注意：revision 的个数/顺序本身随交错不同而不同——被乱序丢弃的 stale partial 数量不同；一致的是最终收敛的内容。

## 关键取舍

- **SQLite 单写者**：写入在数据库层串行。会话级质检吞吐足够，但不适合横向扩展到多机写入。
- **轮询而非推送**：`poll`/`ack` 模型简单且天然支持崩溃恢复（未 ack 会重投，至少一次语义）；需要低延迟推送的消费者可自行高频轮询或监听自己的触发器。
- **每 source 只保留最新 partial**：被取代的 partial 行会被删除（其变化仍留在 revision 流里）。final 永久保留。
- **ack 单调且钳制**：`ack` 只会前进，且不超过会话当前最大 revision；重复 ack 旧位置是 no-op。
- **冲突即错误**：同 `eventId` 不同内容直接抛错而非静默覆盖，把上游数据问题显性化。
- `startMs` 缺失的片段排在最后；排序与到达顺序无关，保证确定性。

## API 摘要

- `new TranscriptStore(file, { busyTimeoutMs? })` — `file` 为 SQLite 路径或 `':memory:'`
- `ingest(sessionId, sourceId, event) → { status, revision }`
- `ingestBatch(sessionId, sourceId, events) → IngestResult[]` — 整批原子，任一冲突则全批回滚
- `snapshot(sessionId) → Snapshot`
- `lastRevision(sessionId) → number`
- `poll(consumerId, sessionId, limit?) → { entries, ackedRevision, lastRevision }`
- `ack(consumerId, sessionId, revision) → number`（生效后的游标）
- `cursor(consumerId, sessionId) → number`
- `close()`

另导出 `ReferenceModel`（纯内存的同等语义实现），供使用方在自己的测试里做交叉校验。
