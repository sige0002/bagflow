# bagflow

rosbag(MCAP)に対する**オフライン検証パイプライン**を、dora-rs 上で宣言的に
組み立てるフレームワーク。ノードは入出力の契約だけを知ればよく、同じ契約を
満たすノードは YAML の1行で付け替えられる。

```
rosbag(mcap+metadata.yaml)
   │  プリフライト: metadata.yamlと購読トピックを照合(実行前にエラー検出)
   ▼
[bagflow_source]──topic A──►[ユーザーノード]──派生データ──►[ユーザーノード]…
   │                └─topic B──►[ユーザーノード](fan-out共有もチェーンも自由)
   ▼ 各ノードの result を集約
[bagflow_report] ──► report.json(検証結果+カバレッジ)
```

- ノード間は Apache Arrow バッチ + dora の共有メモリ転送(受信側ゼロコピー)
- 多言語ノード(Python / Rust / C++ — dora のノードAPIをそのまま利用)
- **全件処理**: EOS+完了ackプロトコルをフレームワークが自動で張るので、
  取りこぼしなくbag全体を処理して正常終了する(カバレッジがreportで常に確認できる)
- **間引き許容**: 入力ごとに `queue_size` を宣言(将来: dora 1.0 の
  `queue_policy: backpressure/drop_oldest` に対応予定)

## フローの書き方

```yaml
bag: /path/to/rosbag_dir          # metadata.yaml + *.mcap のディレクトリ
report: out/report.json

nodes:
  - id: grayscale
    path: grayscale.py
    inputs:
      images: /camera/color/image_raw/compressed   # rostopic を直接購読
    outputs: [gray]

  - id: video
    path: video_sink.py
    inputs:
      frames: grayscale/gray                       # 他ノードの出力を購読
    env:
      OUT_DIR: out
```

実行:

```bash
bagflow check flow.yml        # プリフライトのみ(トピック存在・配線検証)
bagflow run flow.yml          # 実行(dora dataflow を生成して dora start --attach)
bagflow run --no-attach flow.yml   # report.json が書かれた時点で即復帰(最速)
```

## サービス組み込み(最速パターン)

dora の coordinator/daemon は常駐できる。サービス起動時に一度 `dora up`
しておき、bag ごとに `bagflow run --no-attach` を呼ぶと、データフローの
終了処理(ノードのクリーンアップ約2〜3秒)を待たずに report.json 完成時点で
復帰する(reportはアトミックに書かれるので部分読みの心配はない):

```bash
dora up                              # サービス起動時に1回(冪等・約1秒)
bagflow run --no-attach flow.yml     # bagごと: 4ノード構成の実測 約2秒
```

終了処理はdaemon側で非同期に進む。処理の取りこぼしは従来どおり
report.json の `coverage` / `incomplete` で検出できる。

ジョブごとにbagが変わる場合はYAMLを編集せず引数で差し替える:

```bash
bagflow run --no-attach flow.yml \
  --bag /data/incoming/run_xxx \
  --report /data/reports/run_xxx.json
```

想定構成: bag受付(API/録画完了フック)→ ジョブキュー(同時実行数を制御)→
`bagflow run --no-attach --bag ... --report ...` → report.json をDB/APIへ。
常駐daemonのアイドル負荷は実測でCPUほぼ0%・RSS約50MB・共有メモリ0MB。

## ノードの書き方(Python)

```python
from bagflow import BagflowNode

with BagflowNode() as node:
    for name, value, meta in node.messages():   # value: pyarrow配列(トピックはStructArray)
        ...                                     # 自分の処理だけ書く
        node.send("gray", arr, {"rows": 1})     # 下流へのデータ出力(任意)
        node.report({"check": "...", "ok": True})  # report.json に載る結果(任意)
```

EOSの伝播・完了ack・受信件数の記録はヘルパが自動で行う。ノード作者が
気にするのは「自分のinputsに来るデータ」と「出すもの」だけ。

## 推奨パターン: デコードは1回、消費者はゼロコピーで共有

重い変換(JPEGデコードなど)は専用ノードに切り出し、下流はその出力を
購読する。デコードは全体で1回になり、複数の消費者は共有メモリ上の
デコード済みフレームをゼロコピーで参照する:

```
source ─images─> decode ─frames┬─> grayscale ─gray─> video (mp4)
                               └─> brightness (露出チェック)
```

`examples/image_pipeline/` がこの構成。

## キュー制御とメモリ

`queue_size` はエッジごとの共有メモリ滞留の上限(メッセージ数)で、
3段階で制御できる(優先度順):

```yaml
defaults:
  queue_size: 256        # ① フロー全体のデフォルト(組み込み既定値も256)
nodes:
  - id: grayscale
    queue_size: 128      # ② このノードの全入力のデフォルト
    inputs:
      frames:
        node: decode/frames
        queue_size: 64   # ③ 入力ごとの指定(最優先)
source:
  batch_rows: 64         # ソースのバッチ粒度(1メッセージのサイズ)も調整可
  batch_bytes: 8388608
```

- 最悪滞留 ≒ queue_size × 1メッセージのサイズ。生ピクセル(VGAカラーで
  約0.9MB/フレーム)を流すエッジは小さめに設定する
- キューがあふれると古いメッセージからdrop(=間引き)される。dropは
  report.json の coverage に必ず数字で現れるので、黙って欠けることはない
- ノードは**逐次処理**を基本とする(例: `video_sink.py` はfps推定用の
  先頭60フレームだけ保持し、以降はエンコーダへストリーミング書き込み)

## 標準ノード(nodes/)

録画直後のクイック検証(<5秒)向けに `nodes/` に機能ノードを同梱している
(examples/ はデモ、nodes/ が実運用向けのライブラリ):

| ノード | 検出対象 | 閾値(env) |
|---|---|---|
| `decode_image.py` | JPEG→生フレーム(スレッド並列デコード、下流で共有) | — |
| `blur_check.py` | ブレ・ピンボケ(Laplacian分散) | `BLUR_MIN`, `MAX_RATIO` |
| `brightness_check.py` | 露出異常(暗すぎ/白飛び) | `DARK_MEAN`, `BRIGHT_MEAN`, `MAX_RATIO` |
| `freeze_check.py` | カメラ固まり(連続同一フレーム) | `FREEZE_EPS`, `MAX_RUN` |
| `stamp_gap_check.py` | 任意トピックの欠落・停止(タイムスタンプ間隔) | `GAP_MS` / `GAP_FACTOR`, `MAX_GAPS` |
| `topic_rate_check.py` | 全トピックの記録有無・レート(metadataのみ、デコード不要) | `EXPECT_HZ`, `TOLERANCE` |

組み合わせ例は `examples/fast_validation/flow.example.yml`(実測: 6ノードで
約1.5秒)。重いチェック(blur等)がデコードに追いつかない場合はキューあふれで
自動的にサンプリングになり、その割合は coverage の `ratio_vs_upstream` に
正確に現れる — クイックゲートでは「全フレームの20%を検査した」を明示した上で
判定する運用ができる。

## report.json

- `results`: 各ノードが `report()` した内容(各チェックの `ok` / 統計)
- `coverage`: 全エッジの受信数照合 — トピック購読は「bag内件数/ソース送信数/
  受信数」、ノード間エッジは「上流送信数/受信数」(`ratio_vs_upstream`)
- `bag.topics`: 全トピックの件数とHz(metadata由来)
- `incomplete`: EOSが届かなかった(異常終了した)ノードの一覧

## セットアップ

必要なもの: dora CLI(v0.5)、Rust、Python(pyarrow, dora-rs==0.5.0)。
Docker で完結させる場合は `Dockerfile` を参照。

```bash
cargo build --release          # bagflow / bagflow-source
./target/release/bagflow run examples/grayscale_video/flow.yml
```

## 実装メモ

- ソースノードは [mcap2dora](https://github.com/sige0002/mcap2dora) で
  mcap を Arrow にデコードする(埋め込みスキーマからカスタム型も自動対応)
- doraはノード終了後まもなく未消費の共有メモリを回収するため、素朴に
  ソースが送信後すぐ終了するとデータが欠落する。bagflow は
  EOSマーカー+report ノードからの `done` ack(逆向きエッジ)で
  「全ノードが読み切ってから終了」を保証している
