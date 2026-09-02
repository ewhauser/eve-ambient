# Changelog

## [0.7.0](https://github.com/ewhauser/eve-ambient/compare/v0.6.3...v0.7.0) (2026-09-02)


### Features

* **eve-adapter:** upgrade to Eve 0.49 ([#51](https://github.com/ewhauser/eve-ambient/issues/51)) ([d64fa16](https://github.com/ewhauser/eve-ambient/commit/d64fa16277088bde5c804b26a9a66aaf0e76ec4e))

## [0.6.3](https://github.com/ewhauser/eve-ambient/compare/v0.6.2...v0.6.3) (2026-08-20)


### Bug Fixes

* **ambient:** allow transport-authenticated callbacks ([#47](https://github.com/ewhauser/eve-ambient/issues/47)) ([4ddcea4](https://github.com/ewhauser/eve-ambient/commit/4ddcea41733331a3213cb8fea04e158fc2648777))

## [0.6.2](https://github.com/ewhauser/eve-ambient/compare/v0.6.1...v0.6.2) (2026-08-20)


### Bug Fixes

* **ambient:** publish stable durable module IDs ([#45](https://github.com/ewhauser/eve-ambient/issues/45)) ([94a34f9](https://github.com/ewhauser/eve-ambient/commit/94a34f90d9d7fdc6ac2fcc18b33df8de13e9fad3))

## [0.6.1](https://github.com/ewhauser/eve-ambient/compare/v0.6.0...v0.6.1) (2026-08-17)


### Performance Improvements

* **ambient:** batch same-correlation admissions ([#41](https://github.com/ewhauser/eve-ambient/issues/41)) ([f62489b](https://github.com/ewhauser/eve-ambient/commit/f62489baecabcc0e64825a10c1838d3f7c0d8f3a))
* **ambient:** cache resolved workflow hook owners ([#39](https://github.com/ewhauser/eve-ambient/issues/39)) ([453c776](https://github.com/ewhauser/eve-ambient/commit/453c77607fce567ce58e6707a18f4e40af89502d))
* **ambient:** coalesce cold correlation initialization ([#37](https://github.com/ewhauser/eve-ambient/issues/37)) ([f332c97](https://github.com/ewhauser/eve-ambient/commit/f332c97a929ecd79e4ea841fd964597466211977))
* **ambient:** coalesce initial correlation probes ([#40](https://github.com/ewhauser/eve-ambient/issues/40)) ([7aec0e8](https://github.com/ewhauser/eve-ambient/commit/7aec0e80bdf836173d1d35e56ab470a471febc98))
* **ambient:** memoize correlation config hashing ([#42](https://github.com/ewhauser/eve-ambient/issues/42)) ([1c8e33e](https://github.com/ewhauser/eve-ambient/commit/1c8e33e28c918ba17d3a78e8c0db4e017a0a3e10))

## [0.6.0](https://github.com/ewhauser/eve-ambient/compare/v0.5.0...v0.6.0) (2026-08-16)


### ⚠ BREAKING CHANGES

* **ambient:** prepare verified Worlds release ([#32](https://github.com/ewhauser/eve-ambient/issues/32))

### Features

* **ambient:** prepare verified Worlds release ([#32](https://github.com/ewhauser/eve-ambient/issues/32)) ([1a90c78](https://github.com/ewhauser/eve-ambient/commit/1a90c789a83709bde7ea0c6d80b94a1ab3b98cd4))

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
