# ruleset(ブランチ保護)

GitHub の Settings → Rules → Rulesets → New ruleset → Import a ruleset で、JSON を取り込む。
CODEOWNERS が `dev/p2p_video_data` に入ってから取り込む(先に取り込むと、CODEOWNERS の approve 必須が効かない)。

| ファイル | 対象 | 内容 |
|---|---|---|
| `p2p_video_data-protection.json` | `dev/p2p_video_data` | PR 必須・CODEOWNERS 1 人以上の approve 必須・approve 後に push があったら approve を失効させ再レビューを要求・最後に push した人以外の approve 必須・レビュースレッドの解決必須 |

- 対象は既定ブランチ(`master`)ではないので、`~DEFAULT_BRANCH` ではなくブランチ名(`refs/heads/dev/p2p_video_data`)で指定している。
  `master` などほかのブランチは、この ruleset の対象外
- CODEOWNERS は `.github/CODEOWNERS`。PR の向き先ブランチにあるものが使われるので、`dev/p2p_video_data` 宛ての PR にだけ効く
- `bypass_actors` は空(管理者もバイパスしない)。管理者の緊急バイパスが要るなら、取り込み後に UI で足す
- `require_last_push_approval`(最後に push した人以外の approve を必須にする)は true。
  PR 画面の「Update branch」も push 扱いになるため、レビュアーが押すとそのレビュアーは approve できなくなる
  (既存の approve も `dismiss_stale_reviews_on_push` で失効する)。「Update branch」は作者が押す
- `required_review_thread_resolution`(レビュースレッドをすべて Resolve しないとマージできない)は true
- 適用後は `gh api repos/SafieDev/janus-gateway/rulesets` で `enforcement: active` を確認する
- CODEOWNERS の誤り(構文・存在しないユーザー・書き込み権限の無いユーザー)は
  `gh api 'repos/SafieDev/janus-gateway/codeowners/errors?ref=dev/p2p_video_data'` で確認できる(`"errors":[]` なら問題なし)
