# TODO

> Synced from GitHub Issues / PRs (2026-02-23)

## 🐛 Bugs

### Priority: High (Open)

- [ ] **[#136](https://github.com/soimy/openclaw-channel-dingtalk/issues/136)** - 同样的内容，会出现两遍
  - 现象：重复输出/重复回包
  - 方向：优先排查入站去重与发送链路重复触发

- [ ] **[#130](https://github.com/soimy/openclaw-channel-dingtalk/issues/130)** - FailoverError: 403 status code (no body)
  - 标签：upstream
  - 方向：区分平台侧鉴权/限流与插件侧重试策略

### Recently Closed

- [x] **[#112](https://github.com/soimy/openclaw-channel-dingtalk/issues/112)** - dingtalk plugins 不能正常链接，request failed with status code 400
  - 状态：已关闭
  - 关联：与 #63 历史问题有重叠

- [x] **[#95](https://github.com/soimy/openclaw-channel-dingtalk/issues/95)** - Accounts Unsupported schema node. Use Raw mode
  - 状态：已关闭
  - 关联修复：PR [#133](https://github.com/soimy/openclaw-channel-dingtalk/pull/133)

- [x] **[#94](https://github.com/soimy/openclaw-channel-dingtalk/issues/94)** - channel is not running
  - 状态：已关闭
  - 关联修复：PR [#93](https://github.com/soimy/openclaw-channel-dingtalk/pull/93)

- [x] **[#63](https://github.com/soimy/openclaw-channel-dingtalk/issues/63)** - 主动发送消息支持 / 主动发送 400 问题
  - 状态：已关闭
  - 关联修复：PR [#109](https://github.com/soimy/openclaw-channel-dingtalk/pull/109)

- [x] **[#106](https://github.com/soimy/openclaw-channel-dingtalk/issues/106)** - 几个小时不用就报错连不上钉钉
  - 状态：已关闭
  - 关联修复：PR [#96](https://github.com/soimy/openclaw-channel-dingtalk/pull/96)

---

## ✨ Feature Requests

### Priority: High (Open)

- [ ] **[#86](https://github.com/soimy/openclaw-channel-dingtalk/issues/86)** - 支持将图片/媒体消息整合进 AI 流式卡片中
  - 现状：媒体仍多以独立消息发送
  - 下一步：卡片模板与 `card-service` / `send-service` 联动增强

- [ ] **[#67](https://github.com/soimy/openclaw-channel-dingtalk/issues/67)** - 机器人群聊中支持 @ 某人
  - 现状：能力未形成完整自动化链路
  - 下一步：补齐文本识别到钉钉 at 结构化转换

### Priority: Medium (Open)

- [ ] **[#111](https://github.com/soimy/openclaw-channel-dingtalk/issues/111)** - AI Card 模式支持 usage footer
  - 现状：usage 信息未在卡片底部渲染
  - 依赖：卡片模板字段扩展

- [ ] **[#76](https://github.com/soimy/openclaw-channel-dingtalk/issues/76)** - 对话打断功能
  - 状态：仍 open（duplicate，指向 #67）
  - 目标：用户可中断运行中的任务

- [ ] **[#101](https://github.com/soimy/openclaw-channel-dingtalk/issues/101)** - 钉盘文件访问支持
  - 现状：钉盘/钉钉文档链路未支持
  - 相关：#107（open，重复问题）

- [ ] **[#125](https://github.com/soimy/openclaw-channel-dingtalk/issues/125)** - 支持大文件分片上传
  - 现状：当前上传链路对大文件能力有限
  - 目标：支持分片上传与重试

### Recently Closed / Delivered

- [x] **[#110](https://github.com/soimy/openclaw-channel-dingtalk/issues/110)** - 部分格式不能被识别推送到钉钉
  - 状态：已关闭

- [x] **[#63](https://github.com/soimy/openclaw-channel-dingtalk/issues/63)** - 主动发送消息支持
  - 状态：已关闭并已落地

---

## 🚀 Recent Dynamics (Code / PR)

- [x] **[#137](https://github.com/soimy/openclaw-channel-dingtalk/pull/137)** - 渠道模块化重构（已合并）
- [x] **[#134](https://github.com/soimy/openclaw-channel-dingtalk/pull/134)** - startAccount 启停时序修复（已合并）
- [x] **[#133](https://github.com/soimy/openclaw-channel-dingtalk/pull/133)** - 多账号 schema ControlUI 兼容（已合并）
- [x] **[#119](https://github.com/soimy/openclaw-channel-dingtalk/pull/119)** - AI Card thinking/tool use streaming（已合并）
- [x] **[#128](https://github.com/soimy/openclaw-channel-dingtalk/pull/128)** - 引用消息支持（已合并）

---

## 📋 Statistics

| Category                    | Count |
| --------------------------- | ----- |
| Issue (Open)                | 8     |
| Issue (Closed)              | 6     |
| Open Bugs                   | 2     |
| Open Feature Requests       | 6     |
| Closed in Tracked Set       | 6     |
| Recent PR Dynamics (Merged) | 5     |
| **Tracked Issues Total**    | **14** |

---

## 🔗 Quick Links

- [All Issues](https://github.com/soimy/openclaw-channel-dingtalk/issues)
- [Open Issues](https://github.com/soimy/openclaw-channel-dingtalk/issues?q=is%3Aissue%20is%3Aopen)
- [Pull Requests](https://github.com/soimy/openclaw-channel-dingtalk/pulls)
- [CONNECTION_ROBUSTNESS.md](./CONNECTION_ROBUSTNESS.md) - 连接稳定性说明
