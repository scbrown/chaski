# Changelog

## [0.2.0](https://github.com/scbrown/chaski/compare/v0.1.0...v0.2.0) (2026-10-15)


### Features

* bridge admitted scheduled slots to signed-run ports ([#24](https://github.com/scbrown/chaski/issues/24)) ([ed83911](https://github.com/scbrown/chaski/commit/ed83911f3e24479e671768d27c74d32a0503fe18))
* **receiver:** bridge signed phases through the fixed Shuttle CLI ([#26](https://github.com/scbrown/chaski/issues/26)) ([87157ea](https://github.com/scbrown/chaski/commit/87157eaf14e593dcd971733a0a62d679085ece8c))
* **scheduled:** signed scheduled work chain — receiver, executor, sink factory, catalogue adapter (folds [#28](https://github.com/scbrown/chaski/issues/28)-[#31](https://github.com/scbrown/chaski/issues/31)) ([#31](https://github.com/scbrown/chaski/issues/31)) ([226aba0](https://github.com/scbrown/chaski/commit/226aba02df6272df3e78b4b5f8d3f85494f8f561))

## 0.1.0 (2026-10-15)


### Features

* a separable client label for sink writes; cap deliveries at 5 per tick (&lt;= 1 write/s) ([5e3758c](https://github.com/scbrown/chaski/commit/5e3758c552abebfe8923358b48c7b1e6b350008a))
* add scheduled failure-family alerts and retained leases ([#17](https://github.com/scbrown/chaski/issues/17)) ([60964ee](https://github.com/scbrown/chaski/commit/60964ee22a0c3bfacc4266b2979a4e1fb4000444))
* **consumer:** consume committed transaction streams with durable bounded delivery ([#15](https://github.com/scbrown/chaski/issues/15)) ([89e1f89](https://github.com/scbrown/chaski/commit/89e1f89b8c7019e5041d500929091b11159304a0))
* generalize emitters to any verdict pair; catch a same-verdict re-transition ([b10b4a0](https://github.com/scbrown/chaski/commit/b10b4a09fd305770d52276f9e08ee73c2375d922))
* graph sink — deliver emitter events as aegis:ReactionFiring in quipu ([a8e3f8b](https://github.com/scbrown/chaski/commit/a8e3f8b4b34bfd30d92164bb1d508dc7f14d5e75))
* graph sink — deliver emitter events as aegis:ReactionFiring in quipu ([ee12ba0](https://github.com/scbrown/chaski/commit/ee12ba08d445f0e94deab3f358b98751cf25b2a9))
* **graph_sink:** the read-back accepts eventId under the legacy or the quechua term (aegis-9dpcta.1) ([#4](https://github.com/scbrown/chaski/issues/4)) ([3f1d6a2](https://github.com/scbrown/chaski/commit/3f1d6a23960e15ab14fcfb2d9cf45534b1e89da6))
* keeper label on chaski_rule_firing (the rule owner) ([71df07d](https://github.com/scbrown/chaski/commit/71df07dd8f5823f7ee52de1f43ad8ad2bdd1de6a))
* stage 1 read-only reactor (event tail, schedule rules, metrics, canary) ([4b309df](https://github.com/scbrown/chaski/commit/4b309dfecd1592fcc5fec1f7124a2b970ffe2fd8))
* stage 2 emitters — verdict transitions to an outbox, delivered at least once ([68d2f85](https://github.com/scbrown/chaski/commit/68d2f850f397eec422bfd8758eab0304212a43d4))
* stage 2 emitters — verdict transitions to an outbox, delivered at least once ([543bc75](https://github.com/scbrown/chaski/commit/543bc75faf4640669a688dbefa0a72cb17e8a9bf))


### Bug Fixes

* bind emitter for the load_emitters annotation (ruff F821) ([3ee30d5](https://github.com/scbrown/chaski/commit/3ee30d59c97736c4c146cb7a8b59e37d3dd5daff))
* owner review — scope delivery per emitter, snapshot metrics, scrape ([88842e5](https://github.com/scbrown/chaski/commit/88842e53c40adc77a48d2343c86a16263d3a3884))
* refuse concurrent cursor and inbox consumers (aegis-byan1v) ([#18](https://github.com/scbrown/chaski/issues/18)) ([d68da48](https://github.com/scbrown/chaski/commit/d68da488b16d81647af0c12c02e58f9230cdc606))


### Documentation

* the absent-item known limit (sattler's review of 88842e53) ([43c7dc4](https://github.com/scbrown/chaski/commit/43c7dc41835786d65811365574df14cb00961d5d))
