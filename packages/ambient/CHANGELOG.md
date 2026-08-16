# Changelog

## [0.5.0](https://github.com/ewhauser/eve-ambient/compare/v0.4.0...v0.5.0) (2026-08-16)


### Features

* add idempotency lineage primitives ([#7](https://github.com/ewhauser/eve-ambient/issues/7)) ([822b897](https://github.com/ewhauser/eve-ambient/commit/822b8971e3fe878b4c03c2722bdc2e102cfdc68a))
* **ambient:** add attention engine protocol ([#20](https://github.com/ewhauser/eve-ambient/issues/20)) ([c96bdcf](https://github.com/ewhauser/eve-ambient/commit/c96bdcf11d3e359dc14b65f2c986938d7a5603c2))
* **ambient:** simplify consumer application setup ([#22](https://github.com/ewhauser/eve-ambient/issues/22)) ([99f6aaf](https://github.com/ewhauser/eve-ambient/commit/99f6aaf5ee257b777ea40d581251704f6ea35b30))
* **celld:** partition durable custody by channel entity ([#25](https://github.com/ewhauser/eve-ambient/issues/25)) ([72a4890](https://github.com/ewhauser/eve-ambient/commit/72a4890caf6dec47298324ad352dfab598a88f3b))
* **eve:** add durable GitHub ambient ingress ([#24](https://github.com/ewhauser/eve-ambient/issues/24)) ([996b943](https://github.com/ewhauser/eve-ambient/commit/996b943622730c439f4b2044e69b71f340f0dec7))
* make ingress receipts payload-free ([#12](https://github.com/ewhauser/eve-ambient/issues/12)) ([a467d0c](https://github.com/ewhauser/eve-ambient/commit/a467d0cd2b0aa5c6c24d1b069fa9890f924c701b))
* migrate to multi-package Eve workspace ([#14](https://github.com/ewhauser/eve-ambient/issues/14)) ([dfd8c5b](https://github.com/ewhauser/eve-ambient/commit/dfd8c5b3d0173dad38d3cfa6e768f0d4746d9a3e))
* store celld mailbox events by value ([#11](https://github.com/ewhauser/eve-ambient/issues/11)) ([7bde64e](https://github.com/ewhauser/eve-ambient/commit/7bde64e786067fc06c307d5059c657f0356b6218))


### Bug Fixes

* harden idempotency primitives ([#9](https://github.com/ewhauser/eve-ambient/issues/9)) ([88d7c08](https://github.com/ewhauser/eve-ambient/commit/88d7c084f305c4b6ca30ced402b14e129bd79314))

## [0.4.0](https://github.com/ewhauser/eve-extensions/compare/eve-ambient-v0.3.0...eve-ambient-v0.4.0) (2026-08-13)


### Features

* **eve-ambient:** optional celld-backed correlation mailbox ([#45](https://github.com/ewhauser/eve-extensions/issues/45)) ([6b937ec](https://github.com/ewhauser/eve-extensions/commit/6b937ec27e9e8bb5b6ba4f262865346a0499f1db))

## [0.3.0](https://github.com/ewhauser/eve-extensions/compare/eve-ambient-v0.2.0...eve-ambient-v0.3.0) (2026-08-13)


### Features

* **eve-ambient:** warn when debounce maxWait can never fire ([#44](https://github.com/ewhauser/eve-extensions/issues/44)) ([78ae39c](https://github.com/ewhauser/eve-extensions/commit/78ae39c3fe143a92e9e495bd30e933bfb673c5d1))

## [0.2.0](https://github.com/ewhauser/eve-extensions/compare/eve-ambient-v0.1.0...eve-ambient-v0.2.0) (2026-08-12)


### Features

* **eve-ambient:** migrate instance lifecycle to an XState statechart ([#30](https://github.com/ewhauser/eve-extensions/issues/30)) ([734ff71](https://github.com/ewhauser/eve-extensions/commit/734ff7139e25f4abb084c32d39ab9e9f2cb936fc))
