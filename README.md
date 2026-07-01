# MoviePilot-Plugins

MoviePilot v2 插件集合。

## 插件列表

### OpenListStrm

OpenList(alist) STRM 一条龙助手。[[详情](plugins.v2/openliststrm)]

### AutoSubtitle v2.0

自动字幕下载插件，文件整理完成后自动搜索并下载缺失的中文字幕。

**功能：**
- 手动扫描 + TransferComplete 自动触发，双模式
- 三层字幕检测：本地 sidecar → ffprobe 内封 → 云端拉取
- MP SearchChain + 外部 API (assrt.net / opensubtitles) 双路搜索
- jieba 中英文混合关键词
- SOCKS5/HTTP 代理支持
- 简中优先，无简中 fallback 繁体

**配置：** 代理建议填 `socks5://192.168.5.14:7890`

**安装：** MP 插件市场搜索 "自动字幕下载" 安装。
