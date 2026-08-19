# Semantic retrieval extension v1: first-look results

## Result status

This is the first and only look under the [semantic extension v1 preregistration](evaluation-preregistration-semantic-v1.md) before `memory-semantic-extension-v1` became a known regression set. The four retrievers were run without changing the frozen dataset, judgments, runner settings, or retrieval implementations. This document was produced from the two retained local manifests; no retriever was rerun and no fixture was changed while preparing it.

On this fixed synthetic extension, the dense arm returned every positive judgment within the first five results. BM25 and PostgreSQL FTS had lower point estimates. This is a fixed-set observation, not a production promotion decision: the reported intervals are marginal and unpaired, no business threshold was preregistered, and the current server continues to use PostgreSQL FTS.

After this first look, `memory-semantic-extension-v1` is a regression set. Any future retrieval change informed by the cases below is tuning against a known set and cannot be reported as another first-look result.

## Frozen provenance

Both manifests passed the clean source gate before and after evaluation at the same revision.

| Evidence | Semantic extension first look | Original lifecycle regression |
| --- | --- | --- |
| Dataset ID/version | `memory-semantic-extension-v1` / `1.0.0` | `memory-lifecycle-hard-v2` / `2.1.0` |
| Dataset cases/queries | 30 / 30 | 30 / 30 |
| Positive/policy-only queries | 24 / 6 | 24 / 6 |
| Dataset SHA-256 | `36b14f86c62c2636ff551df9090b31cdd63918613f47fa87aa8015088922f82c` | `e198e3881bc71dd2e85b87a0fca107464ca7ea39e5df08ab645e7d18bc243a68` |
| Manifest file | `artifacts/eval/memory-semantic-extension-v1-postgres-vector-latest.json` | `artifacts/eval/memory-lifecycle-v2-postgres-vector-latest.json` |
| Manifest file SHA-256 | `06af80a7ef1a6cf017a2506d13e4c60d67aaeefe618da3a8f2156d29748c5890` | `4db101f48423dff69dbd1a167b5fc9a99c71e78c448e1037e4be534e91590ae9` |
| Generated at | `2026-08-19T12:28:47.440692Z` | `2026-08-19T12:30:22.323735Z` |
| Freeze/build/runtime revision | `0e32fbaa2e783f9d3e71de6940c9e3a2e59eb34a` | `0e32fbaa2e783f9d3e71de6940c9e3a2e59eb34a` |
| Source state | clean, verified, stable before/after | clean, verified, stable before/after |
| Runner/config hash | `evaluation-runner-v2.1` / `b7921c5420219b1205b3091c7dce9f56209286eef122b942f3f137a9effca9f3` | same |

The frozen settings were Recall@5, nDCG@10, retrieval depth 10, one warmup plus three measured searches per query, and a 15-second per-search timeout. Policy pass and a matching clean revision were required.

The component descriptors were identical across the two runs: PostgreSQL server `180006`, schema migration `4`, FTS `simple` configuration with `to_tsvector_lexemes_or_v1` and `ts_rank_cd_v1`, and pgvector `0.8.6` exact cosine search with zero approximate indexes. The dense arm requested and received `text-embedding-bge-m3`, used 1024-dimensional vectors, and recorded embedding behavior SHA-256 `d9d637963703c7473437ce24ed5d99d6f0ec847a537f22f45f8c7c6948f4b702` in embedding space `space_v1_6eb612cfc0c90d8c1cb5180b27971a12f57b6c5a65015753cd505d68fdf60820`. These are component fingerprints, not a model-weight hash.

| Arm | Descriptor config SHA-256 | Extension quality-result SHA-256 | Lifecycle quality-result SHA-256 |
| --- | --- | --- | --- |
| `no-memory-v1` | `d56d57563c7c2de68c3d29fe9f54badea4f15296a108d57bb0df8195b9eec90e` | `a7cd12c81d99e4aa83d5e27e8de6352009a04c2e5e4975d3f799bb9aa7496b0a` | `ca8eb9dac5b1e9f4698d91e59d7613529c8d2983168bd4492c29e3da1ba54afd` |
| `reviewed-cards-bm25-v1` | `d966e8f250cfe5bb1fe08d6f5bb09a2e458878a0b862d51e0f671d906b363b0c` | `bec903d1db7e2f78d2afc85f924428c0e023c3884622bd1e304b40741936a209` | `aaced9907deb9c5d961a3d7b909c2d5c65cad9c0d1106de5bf7c4d16976bd19f` |
| `reviewed-cards-postgres-fts-v1` | `e5ceb0da320069b2fe4d6889da0d672fc2471e01a8ebfe3a41778fff387f28ee` | `ee2c3c45ccbfd10705961d13523d535adf93167f107e4effb05d901664d260df` | `2a4c466427a6954250ff1137c367cf3df8b2be43f985fc34bd1cc01dcd6693f8` |
| `reviewed-cards-postgres-vector-v1` | `ca855f0924ca1152768ebb7a1c5bc40a624e6f35d9dcd9625d3a1ca28a6f4b41` | `cd20cf143142612ebde64fabd5b0ae8dee27e7f763eaf0290dd728e307c3e3a5` | `626445545b5cf1081c2e4d6b5899bb8d4802b2b0e303eb3dc525f2fa4048e493` |

## Semantic extension quality

Each cell is the macro point estimate followed by its 95% marginal query-level percentile-bootstrap interval. The bootstrap used 10,000 SplitMix64 resamples with seed `2026081901`; the 24 positive queries were the sampling units and the six policy-only queries were excluded.

| Arm | Recall@5 | MRR | nDCG@10 | All-relevant pass rate |
| --- | ---: | ---: | ---: | ---: |
| `no-memory-v1` | 0.0000 [0.0000, 0.0000] † | 0.0000 [0.0000, 0.0000] † | 0.0000 [0.0000, 0.0000] † | 0.0000 [0.0000, 0.0000] † |
| `reviewed-cards-bm25-v1` | 0.8333 [0.6875, 0.9583] | 0.7115 [0.5764, 0.8393] | 0.6158 [0.5112, 0.7161] | 0.7917 [0.6250, 0.9583] |
| `reviewed-cards-postgres-fts-v1` | 0.5208 [0.3333, 0.7083] | 0.4428 [0.2671, 0.6234] | 0.3894 [0.2433, 0.5377] | 0.5000 [0.2917, 0.7083] |
| `reviewed-cards-postgres-vector-v1` | 1.0000 [1.0000, 1.0000] † | 0.9479 [0.8646, 1.0000] | 0.8912 [0.8236, 0.9497] | 1.0000 [1.0000, 1.0000] † |

† `boundary_degenerate=true` in the manifest. In particular, `[1, 1]` describes identical outcomes on this finite authored set; it does not mean zero population uncertainty.

These are marginal, unpaired, unstratified intervals. They do not estimate a paired difference between arms and do not support a significance claim. The point estimates also do not establish a production promotion threshold because none was supplied or preregistered.

## Semantic extension bad cases

The table includes every positive extension query with Recall@5 below 1 for a non-ablation retriever. `Top 5` preserves the manifest order; `—` means that the retriever returned no hit. The no-memory ablation returned no hit for all 24 positive queries, as designed. The dense arm had no Recall@5 miss on this run.

| Arm | Case | Query | Positive aliases | Returned top 5 | Recall@5 |
| --- | --- | --- | --- | --- | ---: |
| BM25 | `g02` | 用户采购日常食品后通常怎样完成付款？ | `m_g02_01` | `m_g02_08`, `m_g02_06`, `m_g02_04`, `m_g02_05`, `m_g02_07` | 0.0 |
| BM25 | `h03` | Cedar 的 go-live 由谁负责、在哪个 region，并且要先完成什么业务核对？ | `m_h03_01` | `m_h03_11`, `m_h03_04`, `m_h03_12`, `m_h03_10`, `m_h03_07` | 0.0 |
| BM25 | `i03` | Checkout API 现在应以什么 audience 起步、观察多久后扩大到多少流量；什么错误率会触发 rollback？ | `m_i03_02`, `m_i03_03` | `m_i03_03`, `m_i03_10`, `m_i03_11`, `m_i03_08`, `m_i03_13` | 0.5 |
| BM25 | `i06` | Northwind integration 现在允许的 sustained throughput、burst ceiling 和计数窗口分别是什么？ | `m_i06_02`, `m_i06_04` | `m_i06_11`, `m_i06_02`, `m_i06_07`, `m_i06_13`, `m_i06_10` | 0.5 |
| BM25 | `j04` | Which exact plant-milk product should be used for the user's morning latte, and what two ingredient conditions must it satisfy? | `m_j04_01`, `m_j04_13`, `m_j04_14` | — | 0.0 |
| PostgreSQL FTS | `g02` | 用户采购日常食品后通常怎样完成付款？ | `m_g02_01` | — | 0.0 |
| PostgreSQL FTS | `g05` | 没有远行计划时，用户希望电池补能到什么程度就停止？ | `m_g05_01` | — | 0.0 |
| PostgreSQL FTS | `h02` | 北辰上线由谁拍板，出现故障后应到哪里协同？ | `m_h02_01` | — | 0.0 |
| PostgreSQL FTS | `h03` | Cedar 的 go-live 由谁负责、在哪个 region，并且要先完成什么业务核对？ | `m_h03_01` | `m_h03_11`, `m_h03_04`, `m_h03_12`, `m_h03_10`, `m_h03_08` | 0.0 |
| PostgreSQL FTS | `h05` | 那只需要照顾肾功能的狗，日常应该喂什么以及每顿多少？ | `m_h05_01` | — | 0.0 |
| PostgreSQL FTS | `i02` | 用户目前遇到突发状况时应优先通知谁；若首选联系不上，第二联系人及号码末四位是什么？ | `m_i02_02`, `m_i02_04` | — | 0.0 |
| PostgreSQL FTS | `i03` | Checkout API 现在应以什么 audience 起步、观察多久后扩大到多少流量；什么错误率会触发 rollback？ | `m_i03_02`, `m_i03_03` | `m_i03_03`, `m_i03_11`, `m_i03_10`, `m_i03_08`, `m_i03_13` | 0.5 |
| PostgreSQL FTS | `i05` | 用户现在开车到公司后应该停在哪个位置，这项临时安排持续到什么时候？ | `m_i05_02`, `m_i05_04` | — | 0.0 |
| PostgreSQL FTS | `i06` | Northwind integration 现在允许的 sustained throughput、burst ceiling 和计数窗口分别是什么？ | `m_i06_02`, `m_i06_04` | `m_i06_13`, `m_i06_11`, `m_i06_10`, `m_i06_07`, `m_i06_12` | 0.0 |
| PostgreSQL FTS | `j02` | 用户若要摸黑赶最早一班车，提醒应提前多久、间隔多久，并通过哪些相互独立的设备避免睡过头？ | `m_j02_01`, `m_j02_13`, `m_j02_14` | — | 0.0 |
| PostgreSQL FTS | `j04` | Which exact plant-milk product should be used for the user's morning latte, and what two ingredient conditions must it satisfy? | `m_j04_01`, `m_j04_13`, `m_j04_14` | — | 0.0 |
| PostgreSQL FTS | `k03` | 现在客户可以使用哪些语言和渠道获得实时帮助，语音热线在什么时段开放？ | `m_k03_02`, `m_k03_04` | — | 0.0 |

This list is diagnostic evidence only. Because it is now visible, changing a retriever to address any of these cases is regression-set tuning.

## Original lifecycle regression

The unchanged original 30-case dataset was rerun at the same freeze revision and component configuration. Its quality aggregates remain separate from the extension because many original positive scopes contain five or fewer serviceable cards, making Recall@5 structurally easier. The two datasets therefore are not pooled into a single 60-case quality score.

| Arm | Recall@5 | MRR | nDCG@10 | All-relevant pass rate |
| --- | ---: | ---: | ---: | ---: |
| `no-memory-v1` | 0.0000 [0.0000, 0.0000] † | 0.0000 [0.0000, 0.0000] † | 0.0000 [0.0000, 0.0000] † | 0.0000 [0.0000, 0.0000] † |
| `reviewed-cards-bm25-v1` | 1.0000 [1.0000, 1.0000] † | 0.9792 [0.9375, 1.0000] | 0.9843 [0.9533, 1.0000] | 1.0000 [1.0000, 1.0000] † |
| `reviewed-cards-postgres-fts-v1` | 0.6667 [0.4583, 0.8333] | 0.6250 [0.4375, 0.7917] | 0.6170 [0.4342, 0.7889] | 0.6667 [0.4583, 0.8333] |
| `reviewed-cards-postgres-vector-v1` | 1.0000 [1.0000, 1.0000] † | 0.9792 [0.9375, 1.0000] | 0.9739 [0.9338, 1.0000] | 1.0000 [1.0000, 1.0000] † |

BM25 and dense retrieval had no Recall@5 miss. PostgreSQL FTS missed the following eight positive cases; each returned no hit, so every `Top 5` entry is `—`.

| Case | Query | Positive aliases | FTS top 5 | Recall@5 |
| --- | --- | --- | --- | ---: |
| `a02` | 用户喝咖啡时偏好什么饮品？ | `m_a02_01` | — | 0.0 |
| `a04` | 用户的护照证件什么时候到期？ | `m_a04_01` | — | 0.0 |
| `b03` | 用户母亲的生日是几月几日？ | `m_b03_02` | — | 0.0 |
| `b06` | 用户明确有哪些严重的食物过敏？ | `m_b06_01`, `m_b06_02` | — | 0.0 |
| `c02` | 用户目前居住在哪个城市？ | `m_c02_02` | — | 0.0 |
| `c04` | 当前订单状态应该通过什么渠道通知用户？ | `m_c04_02` | — | 0.0 |
| `d01` | 用户出差订酒店时，对房间环境有什么要求？ | `m_d01_01` | — | 0.0 |
| `d04` | 用户日常写代码首选哪个编辑器？ | `m_d04_01` | — | 0.0 |

## Latency smoke observations

All values are milliseconds over 90 measured samples per arm and dataset. Dense latency includes one live query embedding plus exact PostgreSQL vector search. These are single-machine smoke observations; fixture setup, indexing, concurrency, and capacity behavior are outside their scope.

| Dataset | Arm | p50 ms | p95 ms | max ms |
| --- | --- | ---: | ---: | ---: |
| Semantic extension | `no-memory-v1` | 0.000083 | 0.000084 | 0.000125 |
| Semantic extension | `reviewed-cards-bm25-v1` | 0.090083 | 0.208125 | 0.548375 |
| Semantic extension | `reviewed-cards-postgres-fts-v1` | 0.571083 | 1.144834 | 1.349584 |
| Semantic extension | `reviewed-cards-postgres-vector-v1` | 34.404917 | 47.858916 | 60.395500 |
| Lifecycle regression | `no-memory-v1` | 0.000083 | 0.000125 | 0.000125 |
| Lifecycle regression | `reviewed-cards-bm25-v1` | 0.025375 | 0.052250 | 0.242542 |
| Lifecycle regression | `reviewed-cards-postgres-fts-v1` | 0.523542 | 0.722667 | 0.821458 |
| Lifecycle regression | `reviewed-cards-postgres-vector-v1` | 34.240958 | 42.475917 | 44.463875 |

No latency threshold was configured or evaluated. These values are not an SLA, tail-under-load result, throughput measure, or capacity estimate.

## Policy and cleanup evidence

Both manifests set `policy_pass_required=true` and `policy_pass_verified=true`. For every arm on both datasets, `policy_passed=true`, `policy_pass_rate=1`, and `non_recall_pass_rate=1`. Every aggregate count was zero for forbidden hits, require-empty failures, scope violations, non-active or expired hits, unknown or duplicate hits, over-limit hits, source integrity violations, memory/evidence payload violations, and query execution failures.

The manifests do not embed post-run database row counts. Separate read-only database observations immediately after the semantic first look found zero eval-scoped rows in `user_scope_state`, `evidence_events`, `memory_candidates`, `candidate_source_events`, `memory_identity_chains`, `memory_cards`, and `memory_embeddings`. The same seven counts were again zero after the lifecycle regression rerun, and `ann_indexes=0` was also observed. This is explicitly external local cleanup evidence, not a field covered by either manifest SHA. Independently, the dense descriptor in each manifest records `vector_approximate_index_count=0`.

Zero observed policy violations over these 60 synthetic cases is a regression-gate result only. It is not an estimate of a production cross-tenant leakage rate or any other production failure probability.

## Decision boundary

- The extension was prospective and frozen before its first run, but it is synthetic and not independently collected or production-representative. It is now a known regression set.
- The all-one Recall@5 result and `[1, 1]` boundary-degenerate interval do not establish generalization or eliminate population uncertainty.
- Marginal unpaired intervals cannot establish that one arm is significantly better than another. A paired, preferably stratified, delta analysis would require a separately specified method.
- No production quality threshold, latency SLO, concurrency target, cost budget, or promotion rule was evaluated.
- The vector arm is evaluator/component evidence. The production server still serves PostgreSQL FTS; this run did not switch server retrieval to vector search.
