import asyncio
import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urljoin
from uuid import uuid4

import yake
import jieba
import jieba.analyse
from anyio import Path as AsyncPath

from app.chain.search import SearchChain
from app.core.cache import TTLCache, cached
from app.core.config import settings
from app.core.context import MediaInfo, SubtitleInfo, TorrentInfo
from app.core.event import eventmanager
from app.core.meta.metabase import MetaBase
from app.core.metainfo import MetaInfo
from app.helper.torrent import TorrentHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import NotificationType
from app.schemas.file import FileItem
from app.schemas.transfer import TransferInfo
from app.schemas.types import EventType, MediaType
from app.utils.http import AsyncRequestUtils
from app.utils.system import SystemUtils

# ── 常量 ──────────────────────────────────────────────────────

_SIMPLIFIED_TAGS = frozenset({
    "zh-cn", "zh_cn", "zh-hans", "zh_hans", "chs",
    "chi_sim", "chi-hans", "chi_hans",
})
_TRADITIONAL_TAGS = frozenset({
    "zh-tw", "zh_tw", "zh-hant", "zh_hant", "zh-hk", "zh_hk",
    "zh-mo", "zh_mo", "cht", "chi_tra", "chi-hant", "chi_hant",
})
_TRAD_CHARS = set(
    "說見時會過個們來學開關頭門體國對發現後經麼樣點從當還沒給讓問聽"
    "實際報長東動裡種類華機產萬話風電業處際識達爾羅馬魚鳥龍書馬長"
    "兒義區彆彆後麵愛專門難擊讓體處變聲讀寫買賣進遠運"
)
# 简体特征：词频最高的字符
_SIMP_CHARS = set("的是不了一我有人在这他个们中学大")

_SUBTITLE_EXTS = frozenset({".srt", ".ass", ".ssa", ".sup", ".vtt", ".sub", ".smi"})
_ARCHIVE_EXTS = frozenset({".zip", ".rar", ".7z", ".tar", ".gz", ".bz2"})
_VIDEO_EXTS = frozenset({".strm", ".mkv", ".mp4", ".ts", ".avi", ".mov", ".m2ts", ".iso", ".wmv"})
_HISTORY_MAX = 50


class AutoSubtitle(_PluginBase):
    """自动字幕下载 — 整理完成后自动搜索并下载缺失的中文字幕。"""

    plugin_name = "自动字幕下载"
    plugin_desc = "整理完成后自动搜索并下载缺失的字幕；支持手动扫描、云端字幕拉取、外部字幕源。"
    plugin_version = "2.0"
    plugin_author = "lyndon"
    author_url = "https://github.com/LuSlate/MoviePilot-Plugins"
    plugin_config_prefix = "autosubtitle_"
    plugin_order = 2
    auth_level = 1

    # ── 生命周期 ───────────────────────────────────────────────

    def __init__(self):
        super().__init__()
        self._enabled = False
        self._media_types: list[str] = ["电影"]
        self._scan_paths: list[str] = []
        self._external_sources: list[str] = ["assrt"]
        self._proxy: str = ""
        self._request_delay: float = 2.0
        self._notify: bool = False
        self._kw_extractor = yake.KeywordExtractor(n=1, top=3, lan="en")
        self._search_lock = asyncio.Lock()
        self._dl_cache = TTLCache(region="autosubtitle_dl", maxsize=256, ttl=1800)
        self._scan_running = False
        self._scan_task_id: Optional[str] = None
        self._scan_progress: dict[str, Any] = {}

    def init_plugin(self, config: dict | None = None):
        if config:
            self._enabled = config.get("enabled", False) or False
            self._media_types = config.get("media_types") or ["电影"]
            self._scan_paths = config.get("scan_paths") or []
            self._external_sources = config.get("external_sources") or ["assrt"]
            self._proxy = (config.get("proxy") or "").strip()
            self._request_delay = float(config.get("request_delay") or 2.0)
            self._notify = config.get("notify", False)
            self.__update_config()

    def get_state(self) -> bool:
        return self._enabled

    def get_service(self) -> list[dict]:
        return []

    def get_api(self) -> list[dict[str, Any]]:
        return []

    def get_page(self) -> list[dict]:
        return self.__build_page()

    def get_form(self) -> tuple[list[dict], dict[str, Any]]:
        return self.__build_form()

    def stop_service(self):
        pass

    # ── API 端点 ───────────────────────────────────────────────

    @property
    def api(self):
        """注册 API 路由。"""
        return {
            "scan": self._api_scan,
            "scan_status": self._api_scan_status,
        }

    async def _api_scan(self, request):
        """POST /plugin/AutoSubtitle/scan — 手动触发扫描。"""
        if self._scan_running:
            return {"code": 409, "message": "扫描已在运行中", "task_id": self._scan_task_id}
        try:
            body = await request.json()
        except Exception:
            body = {}
        paths = body.get("paths") or self._scan_paths
        media_types = body.get("media_types") or self._media_types
        if not paths:
            return {"code": 400, "message": "未配置扫描路径"}
        force = body.get("force", False)
        self._scan_running = True
        self._scan_task_id = uuid4().hex[:8]
        self._scan_progress = {"total": 0, "done": 0, "success": 0, "fail": 0, "current": ""}
        asyncio.ensure_future(self.__run_scan(paths, media_types, force))
        return {"code": 200, "task_id": self._scan_task_id}

    async def _api_scan_status(self, request):
        return {
            "running": self._scan_running,
            "task_id": self._scan_task_id,
            "progress": self._scan_progress,
        }

    async def __run_scan(self, paths: list[str], media_types: list[str], force: bool):
        try:
            video_files: list[Path] = []
            for p in paths:
                root = Path(p)
                if not root.exists():
                    continue
                for f in root.rglob("*"):
                    if f.is_file() and f.suffix.lower() in _VIDEO_EXTS:
                        video_files.append(f)
            video_files = list(dict.fromkeys(video_files))
            self._scan_progress["total"] = len(video_files)
            logger.info(f"[AutoSubtitle] 手动扫描开始，共 {len(video_files)} 个文件")
            for f in video_files:
                if not self._scan_running:
                    break
                self._scan_progress["done"] += 1
                self._scan_progress["current"] = str(f)
                try:
                    ok = await self._process_one(f, None, None, None, force)
                    self._scan_progress["success" if ok else "fail"] += 1
                except Exception as e:
                    logger.error(f"[AutoSubtitle] 扫描出错 {f}: {e}")
                    self._scan_progress["fail"] += 1
                await asyncio.sleep(0.5)
        finally:
            self._scan_running = False
            logger.info(
                f"[AutoSubtitle] 手动扫描完成: 共{self._scan_progress['total']}, "
                f"成功{self._scan_progress['success']}, 失败{self._scan_progress['fail']}"
            )

    # ── 事件 ───────────────────────────────────────────────────

    @eventmanager.register(EventType.TransferComplete)
    async def on_transfer_complete(self, event):
        if not self._enabled:
            return
        data = event.event_data
        meta = data.get("meta")
        mediainfo = data.get("mediainfo")
        transferinfo = data.get("transferinfo")
        if not meta or not mediainfo or not transferinfo:
            return
        if mediainfo.type.value not in self._media_types:
            return
        video = transferinfo.target_item
        if not video or not video.path:
            return
        logger.info(f"[AutoSubtitle] 整理完成触发: {video.name}")
        fileitem = transferinfo.fileitem
        source_dir = Path(fileitem.path).parent if fileitem and fileitem.path else None
        await self._process_one(Path(video.path), meta, mediainfo, source_dir)

    # ── 核心流程 ───────────────────────────────────────────────

    async def _process_one(  # noqa: C901
        self,
        video_path: Path,
        meta: MetaBase | None,
        mediainfo: MediaInfo | None,
        source_dir: Path | None = None,
        force: bool = False,
    ) -> bool:
        """处理单个视频：检测缺口 → L3云端 → 搜索 → 下载。"""
        is_strm = video_path.suffix.lower() == ".strm"
        strm_url = ""
        if is_strm:
            try:
                strm_url = video_path.read_text(encoding="utf-8").strip()
            except Exception:
                pass

        # 1. 检测已有字幕
        if not force and await self._check_subtitles(video_path, strm_url):
            logger.info(f"[AutoSubtitle] 已有中文，跳过: {video_path.name}")
            return True

        # 2. L3 云端拉取
        if is_strm and strm_url:
            if await self._pull_cloud_subs(video_path, strm_url):
                logger.info(f"[AutoSubtitle] 云端拉取成功: {video_path.name}")
                return True

        # 3. 关键词
        keywords = self._extract_keywords(meta, mediainfo, video_path)
        if not keywords:
            logger.warning(f"[AutoSubtitle] 无关键词: {video_path.name}")
            return False

        # 4. MP SearchChain
        subs: Optional[list[SubtitleInfo]] = None
        async with self._search_lock:
            for kw in keywords:
                subs = await self._search_mp(kw)
                if subs:
                    break
                await asyncio.sleep(0.5)

        # 5. 外部 API
        if not subs:
            for src in self._external_sources:
                for kw in keywords:
                    subs = await self._search_external(src, kw)
                    if subs:
                        break
                    await asyncio.sleep(self._request_delay)
                if subs:
                    break

        if not subs:
            self._add_history(video_path.name, "", False)
            return False

        # 6. 下载
        ok = await self._download_best(subs, video_path, mediainfo, meta)
        self._add_history(video_path.name, "", ok)
        return ok

    # ── 检测 ───────────────────────────────────────────────────

    async def _check_subtitles(self, video_path: Path, strm_url: str = "") -> bool:
        vdir = video_path.parent
        vstem = video_path.stem.lower()
        # sidecar
        for f in vdir.iterdir():
            if not f.is_file():
                continue
            if f.suffix.lower() not in _SUBTITLE_EXTS:
                continue
            name = f.name.lower()
            if any(m in name for m in ("chi", "zh-cn", "zh_cn", "chs", "简")):
                return True
            if f.stem.lower() == vstem and self._is_chi_sub(f):
                return True
        # 内封
        uri = str(video_path) if not strm_url else strm_url
        return await self._probe_embedded_chi(uri)

    def _is_chi_sub(self, path: Path) -> bool:
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            return False
        return any(c in text for c in _SIMP_CHARS) or sum(1 for c in text if c in _TRAD_CHARS) >= 3

    async def _probe_embedded_chi(self, uri: str) -> bool:
        try:
            proc = await asyncio.create_subprocess_exec(
                "ffprobe", "-v", "quiet", "-print_format", "json",
                "-show_streams", "-select_streams", "s", uri,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
            )
            stdout, _ = await proc.communicate()
            if proc.returncode != 0 or not stdout:
                return False
            for s in json.loads(stdout).get("streams", []):
                if s.get("codec_type") != "subtitle":
                    continue
                lang = (s.get("tags") or {}).get("language", "").lower()
                if lang in _SIMPLIFIED_TAGS | _TRADITIONAL_TAGS | {"chi", "zho", "zh"}:
                    return True
        except FileNotFoundError:
            pass
        except Exception as e:
            logger.debug(f"[AutoSubtitle] ffprobe 失败: {e}")
        return False

    # ── L3 云端 ────────────────────────────────────────────────

    async def _pull_cloud_subs(self, video_path: Path, strm_url: str) -> bool:
        """ponytail: 从 strm URL 反推目录，HTTP 探测同名字幕。兼容 OpenList/AList/WebDAV。"""
        from urllib.parse import urlparse, unquote

        try:
            parsed = urlparse(strm_url)
            base = f"{parsed.scheme}://{parsed.netloc}"
            clean_path = unquote(parsed.path)
            cloud_dir = os.path.dirname(clean_path)
            stem = video_path.stem

            client = self._make_client()
            markers = [stem, f"{stem}.chi.zh-cn", f"{stem}.chi.default", f"{stem}.zh-cn", f"{stem}.chs"]
            exts = [".srt", ".ass", ".ssa"]

            for ext in exts:
                for marker in markers:
                    sub_path = f"{cloud_dir}/{marker}{ext}"
                    sub_url = urljoin(base, sub_path)
                    try:
                        resp = await client.get_res(sub_url)
                        if resp and resp.status_code == 200 and len(resp.content) > 80:
                            dest = video_path.parent / f"{stem}.chi.default{ext}"
                            dest.write_bytes(resp.content)
                            logger.info(f"[AutoSubtitle] 云端拉取: {dest.name}")
                            return True
                    except Exception:
                        continue
        except Exception as e:
            logger.debug(f"[AutoSubtitle] 云端拉取异常: {e}")
        return False

    # ── 关键词 ─────────────────────────────────────────────────

    def _extract_keywords(
        self, meta: MetaBase | None, mediainfo: MediaInfo | None, video_path: Path
    ) -> list[str]:
        result: list[str] = []
        # 中文
        cn = ""
        if meta and meta.name:
            cn = meta.name.strip()
        if not cn and mediainfo:
            cn = (mediainfo.title or "").strip()
        if not cn:
            cn = video_path.stem
        if cn:
            cleaned = re.sub(r"[^一-鿿\w\s\-]", " ", cn)
            try:
                for tag in jieba.analyse.extract_tags(cleaned, topK=3):
                    if tag not in result:
                        result.append(tag)
            except Exception:
                pass
            if len(result) < 2:
                # fallback: 取前N字符
                short = cleaned.strip()[:15]
                if short and short not in result:
                    result.append(short)
        # 英文
        en = ""
        if meta and meta.en_name:
            en = meta.en_name.strip()
        if not en and mediainfo:
            en = (mediainfo.en_title or "").strip()
        if en:
            try:
                for kw, _ in self._kw_extractor.extract_keywords(en.replace(".", " ")):
                    if kw and kw not in result:
                        result.append(kw)
            except Exception:
                pass
            if en not in result:
                result.append(en)
        return result[:5]

    # ── 搜索 ───────────────────────────────────────────────────

    @cached(region="autosubtitle_search", ttl=600, skip_none=False)
    async def _search_mp(self, keyword: str) -> Optional[list[SubtitleInfo]]:
        try:
            results = await SearchChain().async_search_subtitles_by_title(title=keyword, page=0)
            return results or None
        except Exception as e:
            logger.error(f"[AutoSubtitle] MP 搜索异常: {e}")
            return None

    async def _search_external(self, source: str, keyword: str) -> Optional[list[SubtitleInfo]]:
        if source == "assrt":
            return await self._search_assrt(keyword)
        if source == "opensubtitles":
            return await self._search_opensubtitles(keyword)
        return None

    async def _search_assrt(self, keyword: str) -> Optional[list[SubtitleInfo]]:
        """搜索射手网。"""
        from urllib.parse import quote

        client = self._make_client()
        search_url = f"https://assrt.net/sub/?searchword={quote(keyword)}"
        try:
            resp = await client.get_res(search_url)
            if not resp or resp.status_code != 200:
                return None
        except Exception:
            return None

        ids = list(dict.fromkeys(re.findall(r"/xml/sub/(\d+)/\d+\.xml", resp.text)))
        if not ids:
            return None

        results: list[SubtitleInfo] = []
        for sid in ids[:5]:
            try:
                dr = await client.get_res(f"https://assrt.net/xml/sub/{sid}/{sid}.xml")
                if not dr or dr.status_code != 200:
                    continue
                dh = dr.text
                dl_m = re.search(r'href="(/download/\d+/[^"]+)"', dh)
                if not dl_m:
                    continue
                dl_url = f"https://assrt.net{dl_m.group(1)}"
                title_m = re.search(r"<title>(.*?)</title>", dh)
                title = title_m.group(1).split(" 字幕")[0] if title_m else keyword
                lang = "简体中文" if "简" in dh else "繁體中文" if "繁" in dh else "简体中文"
                results.append(SubtitleInfo(
                    title=title, enclosure=dl_url, language=lang, site_name="assrt",
                ))
            except Exception:
                continue
        return results or None

    async def _search_opensubtitles(self, keyword: str) -> Optional[list[SubtitleInfo]]:
        try:
            client = self._make_client()
            resp = await client.get_res(
                f"https://rest.opensubtitles.org/search/query-{keyword}/sublanguageid-chi,zho"
            )
            if not resp or resp.status_code != 200:
                return None
            data = resp.json()
            if not isinstance(data, list):
                return None
            results = []
            for item in data[:5]:
                results.append(SubtitleInfo(
                    title=item.get("MovieName", keyword),
                    enclosure=item.get("SubDownloadLink", ""),
                    language=item.get("LanguageName", "简体中文"),
                    site_name="opensubtitles",
                ))
            return results or None
        except Exception:
            return None

    # ── 下载 ───────────────────────────────────────────────────

    def _make_client(self):
        """ponytail: 一处创建 client，代理/UA 集中。"""
        proxies = {"http": self._proxy, "https": self._proxy} if self._proxy else {}
        return AsyncRequestUtils(proxies=proxies, ua=settings.USER_AGENT or "MoviePilot")

    async def _download_best(
        self,
        subs: list[SubtitleInfo],
        video_path: Path,
        mediainfo: MediaInfo | None,
        meta: MetaBase | None,
    ) -> bool:
        # 过滤
        candidates = []
        for sub in subs:
            lang = (sub.language or "").strip()
            # 只要简中/繁中/未知，跳过明确的其他语言
            if lang and lang not in ("简体中文", "繁體中文", "", "other") and \
               "简" not in lang and "繁" not in lang and "中" not in lang and \
               "chi" not in lang.lower():
                continue
            if mediainfo and not self._match_media(sub, mediainfo):
                continue
            candidates.append(sub)

        if not candidates:
            return False

        # 简中优先
        def sort_key(s: SubtitleInfo):
            lang = (s.language or "").lower()
            return (0 if "简" in lang or "cn" in lang or "zh" in lang else 1, -(s.grabs or 0))
        candidates.sort(key=sort_key)

        stem = video_path.stem
        tdir = video_path.parent

        for sub in candidates[:3]:  # 最多试 3 个
            dl_url = (sub.enclosure or "").strip()
            if not dl_url or dl_url in self._dl_cache:
                continue
            try:
                client = self._make_client()
                resp = await client.get_res(dl_url)
                if not resp or resp.status_code != 200:
                    continue
                self._dl_cache[dl_url] = True
                work_dir = Path(self.get_data_path()) / "downloads" / uuid4().hex
                os.makedirs(work_dir, exist_ok=True)
                fn = TorrentHelper.get_url_filename(resp, dl_url) or f"sub_{uuid4().hex[:6]}"
                tmp = work_dir / fn
                tmp.write_bytes(resp.content)
                sub_files = self._extract_subs(tmp, work_dir)
                if not sub_files:
                    continue
                if mediainfo and mediainfo.type == MediaType.TV and meta:
                    sub_files = self._filter_episode(sub_files, meta)
                if not sub_files:
                    continue
                for sf in sub_files:
                    lang = self._detect_lang(sf)
                    suffix = ".chi.default" if lang == "简体中文" else ".zh-TW"
                    dest = tdir / f"{stem}{suffix}{sf.suffix}"
                    if dest.exists():
                        continue
                    sf.rename(dest)
                    logger.info(f"[AutoSubtitle] 下载成功: {dest.name}")
                    if self._notify and mediainfo:
                        self.post_message(
                            title=f"字幕下载 - {mediainfo.title}",
                            text=f"{lang}: {dest.name}",
                            mtype=NotificationType.Plugin,
                        )
                    return True
            except Exception as e:
                logger.warning(f"[AutoSubtitle] 下载失败: {sub.title} — {e}")
                continue
        return False

    def _match_media(self, sub: SubtitleInfo, mediainfo: MediaInfo) -> bool:
        names = list(dict.fromkeys(
            n.strip() for n in (sub.title, sub.file_name, sub.description) if n and n.strip()
        ))
        for name in names:
            sm = MetaInfo(title=name, subtitle=sub.description)
            st = TorrentInfo(site=sub.site, site_name=sub.site_name, title=name, description=sub.description)
            if TorrentHelper.match_torrent(mediainfo=mediainfo, torrent_meta=sm, torrent=st):
                return True
        return False

    def _filter_episode(self, files: list[Path], meta: MetaBase) -> list[Path]:
        se = meta.begin_season
        ep = meta.begin_episode
        ep_list = meta.episode_list or []
        matching = []
        for f in files:
            fm = MetaInfo(title=f.stem)
            if fm.begin_season == se and fm.begin_episode == ep:
                matching.append(f)
            elif fm.episode_list and set(fm.episode_list) & set(ep_list):
                matching.append(f)
        return matching

    def _extract_subs(self, file: Path, work_dir: Path) -> list[Path]:
        ext = file.suffix.lower()
        if ext in _SUBTITLE_EXTS:
            return [file]
        if ext in _ARCHIVE_EXTS:
            edir = work_dir / file.stem
            try:
                SystemUtils.unpack_archive(file, edir)
                result = []
                for f in edir.rglob("*"):
                    if f.is_file() and f.suffix.lower() in _SUBTITLE_EXTS:
                        d = work_dir / f.name
                        f.rename(d)
                        result.append(d)
                return result
            except Exception as e:
                logger.error(f"[AutoSubtitle] 解压失败: {e}")
            finally:
                try:
                    file.unlink(missing_ok=True)
                except Exception:
                    pass
        return []

    def _detect_lang(self, file: Path) -> str:
        try:
            text = file.read_text(encoding="utf-8", errors="ignore")[:2000]
        except Exception:
            return "简体中文"
        trad = sum(1 for c in text if c in _TRAD_CHARS)
        simp = sum(1 for c in text if "一" <= c <= "鿿" and c not in _TRAD_CHARS)
        return "繁體中文" if trad > max(simp, 2) else "简体中文"

    # ── 历史/页面 ──────────────────────────────────────────────

    def _history(self) -> list[dict]:
        d = self.get_data("history")
        if isinstance(d, str):
            try:
                return json.loads(d)
            except (json.JSONDecodeError, TypeError):
                return []
        return d if isinstance(d, list) else []

    def _add_history(self, title: str, lang: str, ok: bool):
        h = self._history()
        h.insert(0, {"time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "title": title, "lang": lang, "ok": ok})
        if len(h) > _HISTORY_MAX:
            h = h[:_HISTORY_MAX]
        self.save_data("history", json.dumps(h, ensure_ascii=False))

    def __build_page(self) -> list[dict]:
        h = self._history()
        total, ok = len(h), sum(1 for x in h if x.get("ok"))
        status = "运行中" if self._scan_running else "空闲"
        prog = ""
        if self._scan_running and self._scan_progress:
            p = self._scan_progress
            prog = f" | 扫描: {p.get('done',0)}/{p.get('total',0)} 成功{p.get('success',0)}"
        page: list[dict] = [
            {"component": "VAlert", "props": {"type": "info", "variant": "tonal",
             "text": f"状态: {status}  |  记录: {total}次, 成功{ok}  {prog}"}},
        ]
        if not h:
            page.append({"component": "VAlert", "props": {"type": "info", "variant": "tonal",
                         "text": "暂无字幕下载记录。启用后自动在整理完成时搜索。"}})
        else:
            for x in h[:20]:
                s = "✓" if x.get("ok") else "✗"
                page.append({"component": "VCard", "props": {"class": "mb-1"},
                             "content": [{"component": "VCardText",
                                          "text": f"{s} [{x.get('time','')}] {x.get('title','')}"}]})
        # 扫描按钮
        page.append({
            "component": "VRow", "props": {"class": "mt-4"},
            "content": [{"component": "VCol", "props": {"cols": 12},
                         "content": [{"component": "VBtn",
                                      "props": {"color": "primary", "block": True,
                                                "onclick": "fetch('/api/v1/plugin/AutoSubtitle/scan',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({})}).then(r=>r.json()).then(d=>alert(JSON.stringify(d)))"},
                                      "text": "手动扫描"}]}],
        })
        return page

    def __build_form(self) -> tuple[list[dict], dict[str, Any]]:
        return [{
            "component": "VForm",
            "content": [
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 3},
                     "content": [{"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 3},
                     "content": [{"component": "VSwitch", "props": {"model": "notify", "label": "发送通知"}}]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 6},
                     "content": [{"component": "VSelect", "props": {
                         "model": "media_types", "label": "媒体类型", "multiple": True, "chips": True,
                         "items": [{"title": "电影", "value": "电影"}, {"title": "电视剧", "value": "电视剧"}]}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 6},
                     "content": [{"component": "VSelect", "props": {
                         "model": "external_sources", "label": "外部字幕源", "multiple": True, "chips": True,
                         "items": [{"title": "射手网(伪)", "value": "assrt"},
                                   {"title": "OpenSubtitles", "value": "opensubtitles"}]}}]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12, "md": 6},
                     "content": [{"component": "VTextField", "props": {
                         "model": "proxy", "label": "代理地址",
                         "placeholder": "socks5://192.168.5.14:7890"}}]},
                    {"component": "VCol", "props": {"cols": 12, "md": 3},
                     "content": [{"component": "VTextField", "props": {
                         "model": "request_delay", "label": "请求间隔(秒)", "type": "number"}}]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12},
                     "content": [{"component": "VTextarea", "props": {
                         "model": "scan_paths", "label": "扫描路径（每行一个）", "rows": 2,
                         "placeholder": "/media/libraries"}}]},
                ]},
                {"component": "VRow", "content": [
                    {"component": "VCol", "props": {"cols": 12},
                     "content": [{"component": "VAlert", "props": {
                         "type": "info", "variant": "tonal",
                         "text": "自动模式：整理完成时触发。手动扫描：在数据页面点按钮。"}}]},
                ]},
            ],
        }], {
            "enabled": False, "media_types": ["电影"], "scan_paths": [],
            "external_sources": ["assrt"], "proxy": "", "request_delay": 2.0, "notify": False,
        }

    def __update_config(self):
        self.update_config({
            "enabled": self._enabled, "media_types": self._media_types,
            "scan_paths": self._scan_paths, "external_sources": self._external_sources,
            "proxy": self._proxy, "request_delay": self._request_delay, "notify": self._notify,
        })
