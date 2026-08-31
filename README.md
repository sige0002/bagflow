# bagflow

Copyright 2026 Sadasue Yuki. Licensed under the Apache License, Version 2.0;
see [LICENSE](LICENSE).

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
bagflow run --no-attach --timeout 120 flow.yml   # 待ち時間の上限(既定3600秒)
```

## サービス組み込み(最速パターン)

dora の coordinator/daemon は常駐できる。サービス起動時に一度 `dora up`
しておき、bag ごとに `bagflow run --no-attach` を呼ぶと、データフローの
終了処理(ノードのクリーンアップ約2〜3秒)を待たずに report.json 完成時点で
復帰する(reportはアトミックに書かれるので部分読みの心配はない):

```bash
dora up                              # サービス起動時に1回(冪等・約1秒)
bagflow run --no-attach flow.yml     # bagごと: 6ノードのクイックゲート実測 約0.6秒
```

`bagflow run` はコーディネータに到達できれば `dora up` を呼ばない(1回あたり
約0.15秒の削減)。到達できなければ従来どおり自分で起動するので、常駐して
いない環境でも動作は変わらない。

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

## 標準ノード

録画直後のクイック検証向けに機能ノードを同梱している(examples/ はデモ)。
**各チェックは Rust 版(推奨)と Python 版(`nodes/`、参照実装)の2つがあり、
契約・env・report.json の項目が同一**なので flow.yml の `path` だけで入れ替え
られる:

| 検出対象 | Rust(推奨) | Python(参照) | 閾値(env) |
|---|---|---|---|
| JPEG→生フレーム | **`bagflow-decode`** | `decode_image.py` | `RESIZE`, `PIXEL_FORMAT`, `WORKERS` |
| 同上(nvJPEG版) | `bagflow-decode-cuda` | — | `RESIZE`, `BAGFLOW_NVJPEG` 等 |
| ブレ・ピンボケ(Laplacian分散) | **`bagflow-blur`** | `blur_check.py` | `BLUR_MIN`, `MAX_RATIO`, `RESIZE`, `STRIDE` |
| 露出異常(暗すぎ/白飛び) | **`bagflow-brightness`** | `brightness_check.py` | `DARK_MEAN`, `BRIGHT_MEAN`, `MAX_RATIO` |
| カメラ固まり(連続同一フレーム) | **`bagflow-freeze`** | `freeze_check.py` | `FREEZE_EPS`, `MAX_RUN` |
| 任意トピックの欠落・停止 | **`bagflow-stamp-gap`** | `stamp_gap_check.py` | `GAP_MS` / `GAP_FACTOR`, `MAX_GAPS` |
| 全トピックの記録有無・レート | **`bagflow-topic-rate`** | `topic_rate_check.py` | `EXPECT_HZ`, `TOLERANCE` |

Rust版は OpenCV の演算(`cvtColor`, `INTER_AREA` リサイズ, `Laplacian` の
BORDER_REFLECT_101)をそのまま再現しており、同じフレームを両方に流した実測で
report の値は一致する(唯一の差は `laplacian_var_min` の丸め 0.01 と、Rust版が
インライン処理なので `workers` が常に 1 になる点)。閾値の再調整は不要。

組み合わせ例は `examples/fast_validation/flow.example.yml`。101秒・780MB・
29トピックの bag(VGA 3037フレーム)に対する実測(warm cache、`dora up` 済み):

| 構成 | wall | CPU時間 |
|---|---:|---:|
| Python チェック + BGRデコード | 1.03s | 10.5s |
| **Rust チェック + grayデコード** | **0.56s** | **3.7s** |
| 同上、12コアに制限(Jetson Orin AGX相当) | 0.68s | 3.9s |

### デコードの解像度と画素形式

`RESIZE` は libjpeg の DCT スケールで目標サイズ以上の最小 n/8 までデコードし、
そこから SIMD で正確なサイズに落とす。`PIXEL_FORMAT: gray` を指定すると輝度
プレーンだけをデコードするため、**クロマ伸張と色変換が丸ごと消え、共有メモリを
流れるフレームも1/3**になる。輝度しか見ないチェック(blur/brightness/freeze)
だけを並べる構成で有効:

| 実装 / 設定 | デコードwall(3037フレーム→224×224) |
|---|---:|
| `bagflow-decode` BGR | 0.64s |
| **`bagflow-decode` gray** | **0.35s** |
| `decode_image.py`(Python/cv2) | ~1.3s |
| `bagflow-decode-cuda`(nvJPEG) | 4.1s(逐次デコードPoC。小画像はカーネル起動+転送が支配的で、バッチAPI+pinnedメモリ+HWエンジンbackend化までは高解像度・多カメラ向けの布石) |

`PIXEL_FORMAT: gray` は色を落とすので、**mp4出力など色が要る消費者がいる
フローでは使わない**。また brightness の `mean` は BGR 3ch の単純平均から
輝度加重に変わるため(参照bagで 110.5 → 119.8)、`DARK_MEAN` /
`BRIGHT_MEAN` の妥当性だけは確認すること。

Rustノード(`bagflow-node`クレート)はPythonヘルパと同じプロトコルを実装して
おり、report.json・coverage・EOS/ackの挙動は言語によらず同一。重いチェック
がデコードに追いつかない場合はキューあふれで自動的にサンプリングになり、その
割合は coverage の `ratio_vs_upstream` に正確に現れる — クイックゲートでは
「全フレームの20%を検査した」を明示した上で判定する運用ができる。

## report.json

- `results`: 各ノードが `report()` した内容(各チェックの `ok` / 統計)。
  `bagflow_source` の `source_read` は読み出し自体の健全性 — mcap のデコードに
  失敗したトピック(`failed_topics`)、生バイト列にフォールバックしたトピック
  (`fallback_topics`)、捨てられたメッセージ数(`messages_skipped`)。
  リーダはトピック単位の失敗では止まらずに読み進めるため、これを見ないと
  欠損が coverage の数字のズレとしてしか現れない
- `coverage`: 全エッジの受信数照合 — トピック購読は「bag内件数/ソース送信数/
  受信数」、ノード間エッジは「上流送信数/受信数」(`ratio_vs_upstream`)
- `bag.topics`: 全トピックの件数とHz(metadata由来)
- `incomplete`: EOSが届かなかった(異常終了した)ノードの一覧

## セットアップ

必要なもの: dora CLI(v0.5)、Rust、libturbojpeg(`libturbojpeg0-dev`)、
Pythonノードを使う場合は Python(pyarrow, dora-rs==0.5.0, opencv)。
Docker で完結させる場合は `Dockerfile` を参照。

```bash
cargo build --release          # bagflow / bagflow-source / bagflow-report / 各ノード
./target/release/bagflow run examples/grayscale_video/flow.yml
```

### 共有メモリ(コンテナで動かす場合は必須)

dora はノード間のメッセージを **すべて `/dev/shm` に置く**。滞留の最悪値は
エッジごとに `queue_size × 1メッセージのサイズ` で、デコード済みフレームを
流すフローでは容易に数百MBに達する。**Docker の既定値64MBでは足りず、
枯渇したノードはログを1行も残さずに死ぬ**(残った下流ノードは EOS を待って
止まったままになる)。

```bash
docker run --shm-size=2g ...
```

`bagflow check` / `bagflow run` は起動時に `/dev/shm` の容量と宣言された
キュー総量を表示し、256MiB未満なら警告する。

### 詰まったときの調べ方

`bagflow run --no-attach` は `--timeout`(既定3600秒)で待ち時間を区切れる。
タイムアウト時にはノードログの場所に加え、**どのノードのプロセスが既に
消えているか**を出す:

```
Error: timed out after 8s waiting for out/report.json
  node logs: .bagflow/out/019f9795-.../
  no live process: freeze
  still waiting:   bagflow_source, decode, blur, brightness, stamp_gap, topic_rate, bagflow_report
```

なお dora 0.5 はノードの異常終了を下流に伝播しないため(`InputClosed` が
飛ばない)、生き残ったノードは自力では抜けられない。ハングしたデータフローを
放置するとノードプロセスが残って `/dev/shm` を掴み続けるので、`dora stop --all`
で片付けてから再実行すること。

### 途中で切れた bag

電源断や kill -9 で録画が死んだ bag は最終チャンクが半端に終わる。ソースノードは
これを**エラーではなくデータの終端として扱う** — そこで読むのをやめ、EOS と件数は
通常どおり送るのでフローは完走し、report.json に何が起きたかが残る:

```json
"bagflow_source": [{ "check": "source_read", "ok": false,
    "read_error": "…/run_xxx_0.mcap: Chunk ended in the middle of a record" }]
"coverage": { "decode.images": { "rows_in_bag": 3037, "rows_received": 1792,
                                 "ratio_vs_bag": 0.5901 } }
```

`rows_in_bag` は metadata.yaml 由来(録画開始時の見込み)なので、`ratio_vs_bag`
がそのまま「どこまで録れていたか」になる。ここでソースが異常終了してしまうと
下流が EOS を待って止まり、レポートが1つも出ないため、この扱いは崩さないこと。

## 実装メモ

- ソースノードは [mcap2dora](https://github.com/sige0002/mcap2dora) で
  mcap を Arrow にデコードする(埋め込みスキーマからカスタム型も自動対応)。
  購読トピックを `ReaderOptions::topics` で渡し、**購読していないトピックは
  デコード前に捨てる**。29トピックのbagから2トピックだけ読む実測でスキャンは
  0.42s → 0.21s になる
- doraはノード終了後まもなく未消費の共有メモリを回収するため、素朴に
  ソースが送信後すぐ終了するとデータが欠落する。bagflow は
  EOSマーカー+report ノードからの `done` ack(逆向きエッジ)で
  「全ノードが読み切ってから終了」を保証している
