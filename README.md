# MoviePilot-Plugins

自用 MoviePilot v2 插件仓库。

## OpenList Strm生成 (OpenListStrm)

遍历 OpenList(alist) 存储，为云盘媒体在媒体库生成带签名直链的 `.strm`，供 Emby/Jellyfin 直连 CDN 播放。

- 定时 / 立即全量生成（可迁移旧 strm）
- 整理完成事件（TransferComplete）增量生成
- 本地目录监听（watchdog）增量生成
- 适配 OpenList scoped 账号（绝对路径前缀 + 相对遍历根）
