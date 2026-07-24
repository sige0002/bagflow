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
bagflow check flow.yml   # プリフライトのみ(トピック存在・配線検証)
bagflow run flow.yml     # 実行(dora dataflow を生成して dora start --attach)
```

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

## report.json

- `results`: 各ノードが `report()` した内容
- `coverage`: トピック購読ごとの「bag内件数 / ソース送信数 / 受信数」照合
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
