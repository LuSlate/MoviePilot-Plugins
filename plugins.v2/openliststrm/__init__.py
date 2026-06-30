import time
import threading
import urllib.parse
from datetime import datetime, timedelta
from typing import Any, List, Dict, Tuple, Optional
from pathlib import Path

import pytz
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.core.config import settings
from app.core.event import eventmanager, Event
from app.core.meta import MetaBase
from app.core.metainfo import MetaInfoPath
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import TransferInfo, FileItem, RefreshMediaItem, ServiceInfo
from app.core.context import MediaInfo
from app.helper.mediaserver import MediaServerHelper
from app.chain.media import MediaChain
from app.schemas.types import EventType, MediaType
from app.utils.system import SystemUtils


# 浏览器 UA，OpenList 下载直链需要常规 UA 才会 302 到 CDN
_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)


class OpenListClient:
    """
    OpenList(alist) WebDAV 后端 API 客户端。

    替代原 123 私有 API：登录拿 token（缓存复用，失败自动重登），
    fs/list 列目录、fs/get 取文件详情（raw_url 临时直链 + sign 永久签名）。
    账号 base_path 为 /123pan，因此这里所有 path 均为账号相对路径。
    """

    def __init__(self, url: str, user: str, password: str):
        self._url = (url or "").rstrip("/")
        self._user = user or ""
        self._password = password or ""
        self._token: Optional[str] = None
        self._lock = threading.Lock()

    def _login(self) -> str:
        resp = requests.post(
            self._url + "/api/auth/login",
            json={"username": self._user, "password": self._password},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 200:
            raise RuntimeError(f"OpenList 登录失败: {data.get('message')}")
        token = (data.get("data") or {}).get("token")
        if not token:
            raise RuntimeError("OpenList 登录失败: 未返回 token")
        self._token = token
        return token

    def token(self) -> str:
        if not self._token:
            with self._lock:
                if not self._token:
                    self._login()
        return self._token

    def _post(self, api: str, payload: dict, _retry: bool = True) -> dict:
        resp = requests.post(
            self._url + api,
            json=payload,
            headers={"Authorization": self.token()},
            timeout=120,
        )
        resp.raise_for_status()
        data = resp.json()
        # token 失效（401/未授权）时重登重试一次
        if data.get("code") in (401, 403) and _retry:
            with self._lock:
                self._token = None
            return self._post(api, payload, _retry=False)
        return data

    def list_dir(self, path: str) -> List[dict]:
        data = self._post(
            "/api/fs/list",
            {"path": path, "page": 1, "per_page": 0, "refresh": False},
        )
        if data.get("code") != 200:
            logger.warn(f"【OpenList】列目录失败 {path}: {data.get('message')}")
            return []
        return (data.get("data") or {}).get("content") or []

    def get_file(self, path: str) -> dict:
        data = self._post("/api/fs/get", {"path": path, "password": ""})
        if data.get("code") != 200:
            logger.warn(f"【OpenList】获取文件详情失败 {path}: {data.get('message')}")
            return {}
        return data.get("data") or {}

    def raw_url(self, path: str) -> Optional[str]:
        return self.get_file(path).get("raw_url")

    def sign(self, path: str) -> str:
        return self.get_file(path).get("sign") or ""


def ol_iterdir(client: OpenListClient, base_path: str):
    """
    递归遍历 OpenList 目录（账号相对路径），逐项 yield。
    每项: {"path": 账号相对全路径, "name", "is_dir", "sign", "size"}
    """
    stack = [base_path or "/"]
    while stack:
        cur = stack.pop()
        for it in client.list_dir(cur):
            name = it.get("name")
            if not name:
                continue
            full = (cur.rstrip("/") + "/" + name) if cur != "/" else "/" + name
            is_dir = bool(it.get("is_dir"))
            if is_dir:
                stack.append(full)
            yield {
                "path": full,
                "name": name,
                "is_dir": is_dir,
                "sign": it.get("sign") or "",
                "size": it.get("size"),
            }


def build_strm_url(ol_url: str, abs_prefix: str, account_path: str, sign: str) -> str:
    """
    STRM 内容 = 绝对永久签名直链。
    sign 是针对绝对路径(abs_prefix + account_path)计算的永久签名（以 :0 结尾）。
    """
    url = ol_url.rstrip("/") + "/d" + urllib.parse.quote(abs_prefix + account_path)
    if sign:
        url += "?sign=" + sign
    return url


class MediaInfoDownloader:
    """
    媒体信息文件下载器（nfo/图片/字幕/音频）。
    通过 OpenList fs/get 拿 raw_url，再 HTTP GET 流式落盘。
    """

    def __init__(self, client: OpenListClient):
        self.client = client
        self.headers = {"User-Agent": _BROWSER_UA}

    @staticmethod
    def is_file_leq_1k(file_path) -> bool:
        file = Path(file_path)
        if not file.exists():
            return True
        return file.stat().st_size <= 1024

    def save_mediainfo_file(self, file_path: Path, file_name: str, download_url: str):
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with requests.get(
            download_url, stream=True, timeout=30, headers=self.headers
        ) as response:
            response.raise_for_status()
            with open(file_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
        logger.info(f"【媒体信息文件下载】保存 {file_name} 文件成功: {file_path}")

    def downloader(self, account_path: str, path: Path):
        download_url = self.client.raw_url(account_path)
        if not download_url:
            logger.error(
                f"【媒体信息文件下载】{path.name} 下载链接获取失败，无法下载该文件"
            )
            return
        self.save_mediainfo_file(
            file_path=path, file_name=path.name, download_url=download_url
        )

    def auto_downloader(self, downloads_list: List):
        """
        根据列表自动下载，重试至多 3 次，<=1KB 视为失败。
        列表项: [账号相对路径, 本地落盘路径str]
        """
        mediainfo_count = 0
        mediainfo_fail_count = 0
        mediainfo_fail_dict: List = []
        try:
            for item in downloads_list:
                if not item:
                    continue
                download_success = False
                try:
                    for _ in range(3):
                        self.downloader(account_path=item[0], path=Path(item[1]))
                        if not self.is_file_leq_1k(item[1]):
                            mediainfo_count += 1
                            download_success = True
                            break
                        logger.warn(
                            f"【媒体信息文件下载】{item[1]} 下载该文件失败，自动重试"
                        )
                        time.sleep(1)
                except Exception as e:
                    logger.error(f"【媒体信息文件下载】 {item[1]} 出现未知错误: {e}")
                if not download_success:
                    mediainfo_fail_count += 1
                    mediainfo_fail_dict.append(item[1])
                    continue
                if mediainfo_count % 50 == 0:
                    logger.info("【媒体信息文件下载】休眠 2s 后继续下载")
                    time.sleep(2)
        except Exception as e:
            logger.error(f"【媒体信息文件下载】出现未知错误: {e}")
        return mediainfo_count, mediainfo_fail_count, mediainfo_fail_dict


class FullSyncStrmHelper:
    """
    全量生成 STRM 文件（遍历 OpenList 账号目录）。
    """

    def __init__(
        self,
        client: OpenListClient,
        ol_url: str,
        abs_prefix: str,
        user_rmt_mediaext: str,
        user_download_mediaext: str,
        auto_download_mediainfo: bool = False,
    ):
        self.rmt_mediaext = [
            f".{ext.strip()}" for ext in user_rmt_mediaext.replace("，", ",").split(",")
        ]
        self.download_mediaext = [
            f".{ext.strip()}"
            for ext in user_download_mediaext.replace("，", ",").split(",")
        ]
        self.auto_download_mediainfo = auto_download_mediainfo
        self.client = client
        self.ol_url = ol_url
        self.abs_prefix = abs_prefix
        self.strm_count = 0
        self.mediainfo_count = 0
        self.strm_fail_count = 0
        self.mediainfo_fail_count = 0
        self.strm_fail_dict: Dict[str, str] = {}
        self.mediainfo_fail_dict: List = []
        self._mediainfodownloader = MediaInfoDownloader(client=self.client)
        self.download_mediainfo_list = []

    def generate_strm_files(
        self, full_sync_strm_paths: str, full_sync_overwrite_mode: str = "never"
    ):
        media_paths = full_sync_strm_paths.split("\n")
        for path in media_paths:
            if not path:
                continue
            parts = path.split("#", 1)
            if len(parts) < 2:
                continue
            target_dir = parts[0]
            # pan_media_dir 现在是 OpenList 账号相对路径
            pan_media_dir = parts[1].rstrip("/") or "/"

            try:
                for item in ol_iterdir(client=self.client, base_path=pan_media_dir):
                    if item["is_dir"]:
                        continue
                    account_path = item["path"]
                    if pan_media_dir == "/":
                        relpath = account_path.lstrip("/")
                    else:
                        relpath = account_path[len(pan_media_dir):].lstrip("/")
                    file_path = Path(target_dir) / relpath
                    new_file_path = file_path.parent / (file_path.stem + ".strm")

                    try:
                        if self.auto_download_mediainfo:
                            if file_path.suffix in self.download_mediaext:
                                if file_path.exists():
                                    if full_sync_overwrite_mode == "never":
                                        logger.warn(
                                            f"【全量STRM生成】{file_path} 已存在，覆盖模式 never，跳过"
                                        )
                                        continue
                                    logger.warn(
                                        f"【全量STRM生成】{file_path} 已存在，覆盖模式 always"
                                    )
                                self.download_mediainfo_list.append(
                                    [account_path, str(file_path)]
                                )
                                continue

                        if file_path.suffix not in self.rmt_mediaext:
                            logger.warn(
                                "【全量STRM生成】跳过网盘路径: %s",
                                str(file_path).replace(str(target_dir), "", 1),
                            )
                            continue

                        if new_file_path.exists():
                            if full_sync_overwrite_mode == "never":
                                logger.warn(
                                    f"【全量STRM生成】{new_file_path} 已存在，覆盖模式 never，跳过"
                                )
                                continue
                            logger.warn(
                                f"【全量STRM生成】{new_file_path} 已存在，覆盖模式 always"
                            )

                        new_file_path.parent.mkdir(parents=True, exist_ok=True)
                        strm_url = build_strm_url(
                            self.ol_url, self.abs_prefix, account_path, item["sign"]
                        )
                        with open(new_file_path, "w", encoding="utf-8") as file:
                            file.write(strm_url)
                        self.strm_count += 1
                        logger.info(
                            "【全量STRM生成】生成 STRM 文件成功: %s", str(new_file_path)
                        )
                    except Exception as e:
                        logger.error(
                            "【全量STRM生成】生成 STRM 文件失败: %s  %s",
                            str(new_file_path),
                            e,
                        )
                        self.strm_fail_count += 1
                        self.strm_fail_dict[str(new_file_path)] = str(e)
                        continue
            except Exception as e:
                logger.error(f"【全量STRM生成】全量生成 STRM 文件失败: {e}")
                return False

        self.mediainfo_count, self.mediainfo_fail_count, self.mediainfo_fail_dict = (
            self._mediainfodownloader.auto_downloader(
                downloads_list=self.download_mediainfo_list
            )
        )
        if self.strm_fail_dict:
            for path, error in self.strm_fail_dict.items():
                logger.warn(f"【全量STRM生成】{path} 生成错误原因: {error}")
        if self.mediainfo_fail_dict:
            for path in self.mediainfo_fail_dict:
                logger.warn(f"【全量STRM生成】{path} 下载错误")
        logger.info(
            f"【全量STRM生成】全量生成 STRM 文件完成，总共生成 {self.strm_count} 个 STRM 文件，"
            f"下载 {self.mediainfo_count} 个媒体数据文件"
        )
        if self.strm_fail_count != 0 or self.mediainfo_fail_count != 0:
            logger.warn(
                f"【全量STRM生成】{self.strm_fail_count} 个 STRM 文件生成失败，"
                f"{self.mediainfo_fail_count} 个媒体数据文件下载失败"
            )
        return True


class OpenListStrm(_PluginBase):
    """
    OpenList STRM 助手：生成 STRM、监控整理入库、刮削、媒体服务器刷新一条龙服务
    """

    # 插件名称
    plugin_name = "OpenList Strm助手"
    # 插件描述
    plugin_desc = "OpenList STRM 一条龙服务：全量同步 / 整理监控 / 媒体信息下载 / 刮削 / 媒体服务器刷新"
    # 插件图标
    plugin_icon = "https://cdn.oplist.org/gh/OpenListTeam/Logo@main/logo/logo.png"
    # 插件版本
    plugin_version = "2.0"
    # 插件作者
    plugin_author = "lyndon"
    # 作者主页
    author_url = "https://github.com/LuSlate"
    # 插件配置项ID前缀
    plugin_config_prefix = "openliststrm_"
    # 加载顺序
    plugin_order = 27
    # 可使用的用户级别
    auth_level = 1

    # 私有属性
    _client: Optional[OpenListClient] = None
    _scheduler = None
    _lock = threading.Lock()
    _enabled = False
    _once_full_sync_strm = False
    _ol_url = None
    _ol_user = None
    _ol_pass = None
    _abs_prefix = None
    _user_rmt_mediaext = None
    _user_download_mediaext = None
    _transfer_monitor_enabled = False
    _transfer_monitor_paths = None
    _transfer_monitor_scrape_metadata_enabled = False
    _transfer_mp_mediaserver_paths = None
    _transfer_monitor_mediaservers = None
    _transfer_monitor_media_server_refresh_enabled = False
    _timing_full_sync_strm = False
    _full_sync_auto_download_mediainfo_enabled = False
    _cron_full_sync_strm = None
    _full_sync_strm_paths = None
    _full_sync_overwrite_mode = None
    _share_strm_enabled = False
    _share_strm_auto_download_mediainfo_enabled = False
    _user_share_code = None
    _user_share_pwd = None
    _user_share_pan_path = None
    _user_share_local_path = None
    _clear_recyclebin_enabled = False
    _clear_receive_path_enabled = False
    _cron_clear = None

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = config.get("enabled")
            self._once_full_sync_strm = config.get("once_full_sync_strm")
            self._ol_url = (config.get("ol_url") or "").rstrip("/")
            self._ol_user = config.get("ol_user")
            self._ol_pass = config.get("ol_pass")
            self._abs_prefix = (config.get("abs_prefix") or "/123pan").rstrip("/")
            self._user_rmt_mediaext = config.get("user_rmt_mediaext")
            self._user_download_mediaext = config.get("user_download_mediaext")
            self._transfer_monitor_enabled = config.get("transfer_monitor_enabled")
            self._transfer_monitor_paths = config.get("transfer_monitor_paths")
            # 只处理落到该存储类型的整理事件，防止本地(如Strm硬链接)整理触发反馈环
            self._monitor_storage = config.get("monitor_storage") or "alist"
            self._transfer_monitor_scrape_metadata_enabled = config.get(
                "transfer_monitor_scrape_metadata_enabled"
            )
            self._transfer_mp_mediaserver_paths = config.get(
                "transfer_mp_mediaserver_paths"
            )
            self._transfer_monitor_media_server_refresh_enabled = config.get(
                "transfer_monitor_media_server_refresh_enabled"
            )
            self._transfer_monitor_mediaservers = (
                config.get("transfer_monitor_mediaservers") or []
            )
            self._timing_full_sync_strm = config.get("timing_full_sync_strm")
            self._full_sync_auto_download_mediainfo_enabled = config.get(
                "full_sync_auto_download_mediainfo_enabled"
            )
            self._cron_full_sync_strm = config.get("cron_full_sync_strm")
            self._full_sync_strm_paths = config.get("full_sync_strm_paths")
            self._full_sync_overwrite_mode = config.get(
                "full_sync_overwrite_mode", "never"
            )
            self._share_strm_enabled = config.get("share_strm_enabled")
            self._share_strm_auto_download_mediainfo_enabled = config.get(
                "share_strm_auto_download_mediainfo_enabled"
            )
            self._user_share_code = config.get("user_share_code")
            self._user_share_pwd = config.get("user_share_pwd")
            self._user_share_pan_path = config.get("user_share_pan_path")
            self._user_share_local_path = config.get("user_share_local_path")
            self._clear_recyclebin_enabled = config.get("clear_recyclebin_enabled")
            self._clear_receive_path_enabled = config.get("clear_receive_path_enabled")
            self._cron_clear = config.get("cron_clear")
            if not self._abs_prefix:
                self._abs_prefix = "/123pan"
            if not self._user_rmt_mediaext:
                self._user_rmt_mediaext = "mp4,mkv,ts,iso,rmvb,avi,mov,mpeg,mpg,wmv,3gp,asf,m4v,flv,m2ts,tp,f4v"
            if not self._user_download_mediaext:
                self._user_download_mediaext = "srt,ssa,ass,nfo,jpg,jpeg,png"
            if not self._cron_full_sync_strm:
                self._cron_full_sync_strm = "0 */7 * * *"
            if not self._cron_clear:
                self._cron_clear = "0 */7 * * *"
            if not self._user_share_pan_path:
                self._user_share_pan_path = "/"
            self.__update_config()

        try:
            if self._ol_url and self._ol_user:
                self._client = OpenListClient(
                    self._ol_url, self._ol_user, self._ol_pass
                )
        except Exception as e:
            logger.error(f"OpenList 客户端创建失败: {e}")

        # 停止现有任务
        self.stop_service()

        if self._enabled and self._once_full_sync_strm:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._scheduler.add_job(
                func=self.full_sync_strm_files,
                trigger="date",
                run_date=datetime.now(tz=pytz.timezone(settings.TZ))
                + timedelta(seconds=3),
                name="OpenList助手立刻全量同步",
            )
            self._once_full_sync_strm = False
            self.__update_config()
            if self._scheduler.get_jobs():
                self._scheduler.print_jobs()
                self._scheduler.start()

        if self._enabled and self._share_strm_enabled:
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._scheduler.add_job(
                func=self.share_strm_files,
                trigger="date",
                run_date=datetime.now(tz=pytz.timezone(settings.TZ))
                + timedelta(seconds=3),
                name="OpenList助手分享生成STRM",
            )
            self._share_strm_enabled = False
            self.__update_config()
            if self._scheduler.get_jobs():
                self._scheduler.print_jobs()
                self._scheduler.start()

    def get_state(self) -> bool:
        return self._enabled

    @property
    def service_infos(self) -> Optional[Dict[str, ServiceInfo]]:
        """
        服务信息
        """
        _mediaserver_helper = MediaServerHelper()

        if not self._transfer_monitor_mediaservers:
            logger.warning("尚未配置媒体服务器，请检查配置")
            return None

        services = _mediaserver_helper.get_services(
            name_filters=self._transfer_monitor_mediaservers
        )
        if not services:
            logger.warning("获取媒体服务器实例失败，请检查配置")
            return None

        active_services = {}
        for service_name, service_info in services.items():
            if service_info.instance.is_inactive():
                logger.warning(f"媒体服务器 {service_name} 未连接，请检查配置")
            else:
                active_services[service_name] = service_info

        if not active_services:
            logger.warning("没有已连接的媒体服务器，请检查配置")
            return None

        return active_services

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        # STRM 内含永久签名直链，无需 302 跳转服务
        pass

    def get_service(self) -> List[Dict[str, Any]]:
        cron_service = []
        if (
            self._cron_full_sync_strm
            and self._timing_full_sync_strm
            and self._full_sync_strm_paths
        ):
            cron_service.append(
                {
                    "id": "OpenListStrm_full_sync_strm_files",
                    "name": "定期全量同步OpenList媒体库",
                    "trigger": CronTrigger.from_crontab(self._cron_full_sync_strm),
                    "func": self.full_sync_strm_files,
                    "kwargs": {},
                }
            )
        if self._cron_clear and (
            self._clear_recyclebin_enabled or self._clear_receive_path_enabled
        ):
            cron_service.append(
                {
                    "id": "OpenListStrm_main_cleaner",
                    "name": "定期清理OpenList空间",
                    "trigger": CronTrigger.from_crontab(self._cron_clear),
                    "func": self.main_cleaner,
                    "kwargs": {},
                }
            )
        if cron_service:
            return cron_service
        return []

    # ---------------- 路径辅助 ----------------
    @staticmethod
    def has_prefix(full_path, prefix_path) -> bool:
        full = Path(full_path).parts
        prefix = Path(prefix_path).parts
        if len(prefix) > len(full):
            return False
        return full[: len(prefix)] == prefix

    def __get_media_path(self, paths, media_path):
        """
        获取媒体目录路径，返回 (是否匹配, 本地STRM目录, 网盘目录)
        """
        media_paths = paths.split("\n")
        for path in media_paths:
            if not path:
                continue
            parts = path.split("#", 1)
            if len(parts) < 2:
                continue
            if self.has_prefix(media_path, parts[1]):
                return True, parts[0], parts[1]
        return False, None, None

    def _account_rel(self, path) -> str:
        """
        把 MP 存储记录的路径转换为 OpenList 账号相对路径（去掉 abs_prefix）
        """
        p = str(path)
        if self._abs_prefix and p.startswith(self._abs_prefix):
            p = p[len(self._abs_prefix):]
        if not p.startswith("/"):
            p = "/" + p
        return p

    @staticmethod
    def media_scrape_metadata(
        path,
        item_name: str = "",
        mediainfo: MediaInfo = None,
        meta: MetaBase = None,
    ):
        """
        媒体刮削服务
        """
        item_name = item_name if item_name else Path(path).name
        mediachain = MediaChain()
        logger.info(f"【媒体刮削】{item_name} 开始刮削元数据")
        if mediainfo:
            if mediainfo.type == MediaType.MOVIE:
                dir_path = Path(path).parent
                fileitem = FileItem(
                    storage="local",
                    type="dir",
                    path=str(dir_path),
                    name=dir_path.name,
                    basename=dir_path.stem,
                    modify_time=dir_path.stat().st_mtime,
                )
            else:
                rename_format_level = len(settings.TV_RENAME_FORMAT.split("/")) - 1
                if rename_format_level < 1:
                    file_path = Path(path)
                    fileitem = FileItem(
                        storage="local",
                        type="file",
                        path=str(file_path).replace("\\", "/"),
                        name=file_path.name,
                        basename=file_path.stem,
                        extension=file_path.suffix[1:],
                        size=file_path.stat().st_size,
                        modify_time=file_path.stat().st_mtime,
                    )
                else:
                    dir_path = Path(Path(path).parents[rename_format_level - 1])
                    fileitem = FileItem(
                        storage="local",
                        type="dir",
                        path=str(dir_path),
                        name=dir_path.name,
                        basename=dir_path.stem,
                        modify_time=dir_path.stat().st_mtime,
                    )
            mediachain.scrape_metadata(
                fileitem=fileitem, meta=meta, mediainfo=mediainfo
            )
        else:
            meta = MetaInfoPath(Path(path))
            mediainfo = mediachain.recognize_by_meta(meta)
            file_type = "dir"
            dir_path = Path(path).parent
            tem_mediainfo = mediachain.recognize_by_meta(MetaInfoPath(dir_path))
            if tem_mediainfo and tem_mediainfo.imdb_id == mediainfo.imdb_id:
                if mediainfo.type == MediaType.TV:
                    dir_path = dir_path.parent
                    tem_mediainfo = mediachain.recognize_by_meta(MetaInfoPath(dir_path))
                    if tem_mediainfo and tem_mediainfo.imdb_id == mediainfo.imdb_id:
                        finish_path = dir_path
                    else:
                        logger.warn(f"【媒体刮削】{dir_path} 无法识别文件媒体信息！")
                        finish_path = Path(path).parent
                else:
                    finish_path = dir_path
            else:
                logger.warn(f"【媒体刮削】{dir_path} 无法识别文件媒体信息！")
                finish_path = Path(path)
                file_type = "file"
            fileitem = FileItem(
                storage="local",
                type=file_type,
                path=str(finish_path),
                name=finish_path.name,
                basename=finish_path.stem,
                modify_time=finish_path.stat().st_mtime,
            )
            mediachain.scrape_metadata(
                fileitem=fileitem, meta=meta, mediainfo=mediainfo
            )

        logger.info(f"【媒体刮削】{item_name} 刮削元数据完成")

    @eventmanager.register(EventType.TransferComplete)
    def generate_strm(self, event: Event):
        """
        监控目录整理生成 STRM 文件
        """

        def generate_strm_files(
            target_dir: Path,
            pan_media_dir: Path,
            item_dest_path: Path,
            basename: str,
            url: str,
        ):
            try:
                pan_media_dir = str(Path(pan_media_dir))
                pan_path = str(Path(Path(item_dest_path).parent))
                if self.has_prefix(pan_path, pan_media_dir):
                    pan_path = pan_path[len(pan_media_dir):].lstrip("/").lstrip("\\")
                file_path = Path(target_dir) / pan_path
                file_name = basename + ".strm"
                new_file_path = file_path / file_name
                new_file_path.parent.mkdir(parents=True, exist_ok=True)
                with open(new_file_path, "w", encoding="utf-8") as file:
                    file.write(url)
                logger.info(
                    "【监控整理STRM生成】生成 STRM 文件成功: %s", str(new_file_path)
                )
                return True, str(new_file_path)
            except Exception as e:
                logger.error(
                    "【监控整理STRM生成】生成 STRM 文件失败: %s", e
                )
                return False, None

        if (
            not self._enabled
            or not self._transfer_monitor_enabled
            or not self._transfer_monitor_paths
            or not self._client
        ):
            return

        item = event.event_data
        if not item:
            return

        item_transfer: TransferInfo = item.get("transferinfo")
        mediainfo: MediaInfo = item.get("mediainfo")
        meta: MetaBase = item.get("meta")
        if not item_transfer or not getattr(item_transfer, "target_diritem", None):
            return

        # 存储过滤：只处理落到 OpenList 存储的整理事件，
        # 否则本地 Strm硬链接 等整理事件也会被匹配，形成反馈环生成错误直链
        dest_storage = getattr(
            getattr(item_transfer, "target_item", None), "storage", ""
        ) or ""
        if self._monitor_storage and dest_storage and dest_storage != self._monitor_storage:
            logger.debug(
                f"【监控整理STRM生成】存储 {dest_storage} 非目标 {self._monitor_storage}，跳过"
            )
            return

        # 网盘目的地目录（账号相对）
        itemdir_dest_path = self._account_rel(item_transfer.target_diritem.path)
        # 网盘目的地路径（包含文件名称，账号相对）
        item_dest_path = self._account_rel(item_transfer.target_item.path)
        # 网盘目的地文件名称
        item_dest_name: str = item_transfer.target_item.name
        # 网盘目的地文件名称（不包含后缀）
        item_dest_basename: str = item_transfer.target_item.basename

        # 是否蓝光原盘
        item_bluray = SystemUtils.is_bluray_dir(Path(itemdir_dest_path))
        # 目标字幕/音频文件清单（MP 给的是网盘路径）
        subtitle_list = getattr(item_transfer, "subtitle_list_new", []) or []
        audio_list = getattr(item_transfer, "audio_list_new", []) or []

        __status, local_media_dir, pan_media_dir = self.__get_media_path(
            self._transfer_monitor_paths, itemdir_dest_path
        )
        if not __status:
            logger.debug(
                f"【监控整理STRM生成】{item_dest_name} 路径匹配不符合，跳过整理"
            )
            return
        logger.debug("【监控整理STRM生成】匹配到网盘文件夹路径: %s", str(pan_media_dir))

        if item_bluray:
            logger.warning(
                f"【监控整理STRM生成】{item_dest_name} 为蓝光原盘，不支持生成 STRM 文件: {item_dest_path}"
            )
            return

        # 取签名生成永久直链
        try:
            sign = self._client.sign(item_dest_path)
        except Exception as e:
            logger.error(
                f"【监控整理STRM生成】{item_dest_name} 获取 OpenList 签名失败: {e}"
            )
            return
        strm_url = build_strm_url(
            self._ol_url, self._abs_prefix, item_dest_path, sign
        )

        status, strm_target_path = generate_strm_files(
            target_dir=local_media_dir,
            pan_media_dir=pan_media_dir,
            item_dest_path=Path(item_dest_path),
            basename=item_dest_basename,
            url=strm_url,
        )
        if not status:
            return

        # 下载字幕/音频等媒体信息文件
        try:
            _mediainfodownloader = MediaInfoDownloader(client=self._client)
            for _list, _label in ((subtitle_list, "字幕"), (audio_list, "音频")):
                if not _list:
                    continue
                logger.info(f"【监控整理STRM生成】开始下载{_label}文件")
                for _path in _list:
                    account_path = self._account_rel(_path)
                    download_url = self._client.raw_url(account_path)
                    if not download_url:
                        logger.error(
                            f"【监控整理STRM生成】{Path(_path).name} 下载链接获取失败，无法下载该文件"
                        )
                        continue
                    _file_path = Path(local_media_dir) / Path(
                        self._account_rel(_path)
                    ).relative_to(pan_media_dir)
                    _mediainfodownloader.save_mediainfo_file(
                        file_path=Path(_file_path),
                        file_name=_file_path.name,
                        download_url=download_url,
                    )
        except Exception as e:
            logger.error(f"【监控整理STRM生成】媒体信息文件下载出现未知错误: {e}")

        if self._transfer_monitor_scrape_metadata_enabled:
            self.media_scrape_metadata(
                path=strm_target_path,
                item_name=item_dest_name,
                mediainfo=mediainfo,
                meta=meta,
            )

        if self._transfer_monitor_media_server_refresh_enabled:
            if not self.service_infos:
                return

            logger.info("【监控整理STRM生成】开始刷新媒体服务器")

            if self._transfer_mp_mediaserver_paths:
                status, mediaserver_path, moviepilot_path = self.__get_media_path(
                    self._transfer_mp_mediaserver_paths, strm_target_path
                )
                if status:
                    logger.info("【监控整理STRM生成】刷新媒体服务器目录替换中...")
                    strm_target_path = strm_target_path.replace(
                        moviepilot_path, mediaserver_path
                    ).replace("\\", "/")
                    logger.info(
                        f"【监控整理STRM生成】刷新媒体服务器目录替换: {moviepilot_path} --> {mediaserver_path}"
                    )

            items = [
                RefreshMediaItem(
                    title=mediainfo.title,
                    year=mediainfo.year,
                    type=mediainfo.type,
                    category=mediainfo.category,
                    target_path=Path(strm_target_path),
                )
            ]

            for name, service in self.service_infos.items():
                if hasattr(service.instance, "refresh_library_by_items"):
                    service.instance.refresh_library_by_items(items)
                elif hasattr(service.instance, "refresh_root_library"):
                    service.instance.refresh_root_library()
                else:
                    logger.warning(f"【监控整理STRM生成】{name} 不支持刷新")

    def full_sync_strm_files(self):
        """
        全量同步
        """
        if not self._full_sync_strm_paths or not self._client:
            logger.warn("【全量STRM生成】OpenList 配置不完整或同步目录为空，跳过")
            return
        if not self._lock.acquire(blocking=False):
            logger.info("【全量STRM生成】已有任务在运行，跳过本次")
            return
        try:
            strm_helper = FullSyncStrmHelper(
                client=self._client,
                ol_url=self._ol_url,
                abs_prefix=self._abs_prefix,
                user_rmt_mediaext=self._user_rmt_mediaext,
                user_download_mediaext=self._user_download_mediaext,
                auto_download_mediainfo=self._full_sync_auto_download_mediainfo_enabled,
            )
            strm_helper.generate_strm_files(
                full_sync_strm_paths=self._full_sync_strm_paths,
                full_sync_overwrite_mode=self._full_sync_overwrite_mode,
            )
        finally:
            self._lock.release()

    def share_strm_files(self):
        """
        分享生成STRM。

        OpenList 的受限账号无法直接列举任意 123 分享，此处保留配置与入口，
        但实际不可用，给出明确提示后优雅返回。
        """
        # ponytail: 受限账号无法列分享，留作 stub；如需此功能请在 OpenList 中将分享挂为存储后用全量同步
        logger.warning(
            "【分享STRM生成】当前通过受限 OpenList 账号无法列举 123 分享。"
            "分享STRM需在 OpenList 中将该分享添加为存储后，再用全量同步功能生成。"
        )
        return

    def main_cleaner(self):
        """
        主清理模块
        """
        if self._clear_receive_path_enabled:
            self.clear_receive_path()
        if self._clear_recyclebin_enabled:
            self.clear_recyclebin()

    def clear_recyclebin(self):
        """
        清空回收站（受限 OpenList 账号无对应能力，no-op）
        """
        # ponytail: 受限账号无回收站 API，安全空转
        logger.info(
            "【回收站清理】受限 OpenList 账号无回收站接口，跳过（如需请在 OpenList 端手动清理）"
        )
        return

    def clear_receive_path(self):
        """
        清空我的秒传（受限 OpenList 账号无对应能力，no-op）
        """
        # ponytail: 受限账号无秒传目录概念，安全空转
        logger.info(
            "【我的秒传清理】受限 OpenList 账号无秒传目录接口，跳过（如需请在 OpenList 端手动清理）"
        )
        return

    def get_page(self) -> List[dict]:
        pass

    def __update_config(self):
        self.update_config(
            {
                "enabled": self._enabled,
                "once_full_sync_strm": self._once_full_sync_strm,
                "ol_url": self._ol_url,
                "ol_user": self._ol_user,
                "ol_pass": self._ol_pass,
                "abs_prefix": self._abs_prefix,
                "user_rmt_mediaext": self._user_rmt_mediaext,
                "user_download_mediaext": self._user_download_mediaext,
                "transfer_monitor_enabled": self._transfer_monitor_enabled,
                "transfer_monitor_paths": self._transfer_monitor_paths,
                "monitor_storage": self._monitor_storage,
                "transfer_monitor_scrape_metadata_enabled": self._transfer_monitor_scrape_metadata_enabled,
                "transfer_mp_mediaserver_paths": self._transfer_mp_mediaserver_paths,
                "transfer_monitor_media_server_refresh_enabled": self._transfer_monitor_media_server_refresh_enabled,
                "transfer_monitor_mediaservers": self._transfer_monitor_mediaservers,
                "timing_full_sync_strm": self._timing_full_sync_strm,
                "full_sync_auto_download_mediainfo_enabled": self._full_sync_auto_download_mediainfo_enabled,
                "cron_full_sync_strm": self._cron_full_sync_strm,
                "full_sync_strm_paths": self._full_sync_strm_paths,
                "full_sync_overwrite_mode": self._full_sync_overwrite_mode,
                "share_strm_enabled": self._share_strm_enabled,
                "share_strm_auto_download_mediainfo_enabled": self._share_strm_auto_download_mediainfo_enabled,
                "user_share_code": self._user_share_code,
                "user_share_pwd": self._user_share_pwd,
                "user_share_pan_path": self._user_share_pan_path,
                "user_share_local_path": self._user_share_local_path,
                "clear_recyclebin_enabled": self._clear_recyclebin_enabled,
                "clear_receive_path_enabled": self._clear_receive_path_enabled,
                "cron_clear": self._cron_clear,
            }
        )

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        _mediaserver_helper = MediaServerHelper()

        transfer_monitor_tab = [
            {
                "component": "VRow",
                "content": [
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 3},
                        "content": [
                            {
                                "component": "VSwitch",
                                "props": {
                                    "model": "transfer_monitor_enabled",
                                    "label": "整理事件监控",
                                },
                            }
                        ],
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 3},
                        "content": [
                            {
                                "component": "VSwitch",
                                "props": {
                                    "model": "transfer_monitor_scrape_metadata_enabled",
                                    "label": "STRM自动刮削",
                                },
                            }
                        ],
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 3},
                        "content": [
                            {
                                "component": "VSwitch",
                                "props": {
                                    "model": "transfer_monitor_media_server_refresh_enabled",
                                    "label": "媒体服务器刷新",
                                },
                            }
                        ],
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 3},
                        "content": [
                            {
                                "component": "VSelect",
                                "props": {
                                    "multiple": True,
                                    "chips": True,
                                    "clearable": True,
                                    "model": "transfer_monitor_mediaservers",
                                    "label": "媒体服务器",
                                    "items": [
                                        {"title": config.name, "value": config.name}
                                        for config in _mediaserver_helper.get_configs().values()
                                    ],
                                },
                            }
                        ],
                    },
                ],
            },
            {
                "component": "VRow",
                "content": [
                    {
                        "component": "VCol",
                        "props": {"cols": 12},
                        "content": [
                            {
                                "component": "VTextarea",
                                "props": {
                                    "model": "transfer_monitor_paths",
                                    "label": "整理事件监控目录",
                                    "rows": 5,
                                    "placeholder": "一行一个，格式：本地STRM目录#网盘媒体库目录(账号相对)\n例如：\n/volume1/strm/movies#/电影\n/volume1/strm/tv#/电视剧",
                                    "hint": "监控MoviePilot整理入库事件，自动在此处配置的本地目录生成对应的STRM文件。网盘目录为 OpenList 账号相对路径。",
                                    "persistent-hint": True,
                                },
                            },
                        ],
                    }
                ],
            },
            {
                "component": "VRow",
                "content": [
                    {
                        "component": "VCol",
                        "props": {"cols": 12},
                        "content": [
                            {
                                "component": "VTextarea",
                                "props": {
                                    "model": "transfer_mp_mediaserver_paths",
                                    "label": "媒体服务器映射替换",
                                    "rows": 2,
                                    "placeholder": "一行一个，格式：媒体库服务器映射目录#MP映射目录\n例如：\n/media#/data",
                                    "hint": "用于媒体服务器映射路径和MP映射路径不一样时自动刷新媒体服务器入库",
                                    "persistent-hint": True,
                                },
                            },
                        ],
                    }
                ],
            },
        ]

        full_sync_tab = [
            {
                "component": "VRow",
                "content": [
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 4},
                        "content": [
                            {
                                "component": "VSwitch",
                                "props": {
                                    "model": "once_full_sync_strm",
                                    "label": "立刻全量同步",
                                },
                            }
                        ],
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 4},
                        "content": [
                            {
                                "component": "VSwitch",
                                "props": {
                                    "model": "timing_full_sync_strm",
                                    "label": "定期全量同步",
                                },
                            }
                        ],
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 4},
                        "content": [
                            {
                                "component": "VCronField",
                                "props": {
                                    "model": "cron_full_sync_strm",
                                    "label": "运行全量同步周期",
                                },
                            }
                        ],
                    },
                ],
            },
            {
                "component": "VRow",
                "content": [
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 4},
                        "content": [
                            {
                                "component": "VSwitch",
                                "props": {
                                    "model": "full_sync_auto_download_mediainfo_enabled",
                                    "label": "下载媒体数据文件",
                                },
                            }
                        ],
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 4},
                        "content": [
                            {
                                "component": "VSelect",
                                "props": {
                                    "clearable": True,
                                    "model": "full_sync_overwrite_mode",
                                    "label": "覆盖模式",
                                    "items": [
                                        {"title": "总是", "value": "always"},
                                        {"title": "从不", "value": "never"},
                                    ],
                                },
                            }
                        ],
                    },
                ],
            },
            {
                "component": "VRow",
                "content": [
                    {
                        "component": "VCol",
                        "props": {"cols": 12},
                        "content": [
                            {
                                "component": "VTextarea",
                                "props": {
                                    "model": "full_sync_strm_paths",
                                    "label": "全量同步目录",
                                    "rows": 5,
                                    "placeholder": "一行一个，格式：本地STRM目录#网盘媒体库目录(账号相对)\n例如：\n/volume1/strm/movies#/电影\n/volume1/strm/tv#/电视剧",
                                    "hint": "全量扫描配置的 OpenList 账号相对目录，并在对应的本地目录生成STRM文件。",
                                    "persistent-hint": True,
                                },
                            },
                        ],
                    }
                ],
            },
        ]

        share_generate_tab = [
            {
                "component": "VRow",
                "content": [
                    {
                        "component": "VCol",
                        "props": {"cols": 12},
                        "content": [
                            {
                                "component": "VAlert",
                                "props": {
                                    "type": "warning",
                                    "variant": "tonal",
                                    "density": "compact",
                                    "text": "受限 OpenList 账号无法直接列举 123 分享。如需分享生成STRM，请在 OpenList 中将分享添加为存储后，使用“全量同步”功能。",
                                },
                            }
                        ],
                    }
                ],
            },
            {
                "component": "VRow",
                "content": [
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 4},
                        "content": [
                            {
                                "component": "VSwitch",
                                "props": {
                                    "model": "share_strm_enabled",
                                    "label": "运行分享生成STRM",
                                },
                            }
                        ],
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 4},
                        "content": [
                            {
                                "component": "VSwitch",
                                "props": {
                                    "model": "share_strm_auto_download_mediainfo_enabled",
                                    "label": "下载媒体数据文件",
                                },
                            }
                        ],
                    },
                ],
            },
            {
                "component": "VRow",
                "content": [
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 6},
                        "content": [
                            {
                                "component": "VTextField",
                                "props": {
                                    "model": "user_share_code",
                                    "label": "分享码",
                                },
                            }
                        ],
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 6},
                        "content": [
                            {
                                "component": "VTextField",
                                "props": {
                                    "model": "user_share_pwd",
                                    "label": "分享密码",
                                },
                            }
                        ],
                    },
                ],
            },
            {
                "component": "VRow",
                "content": [
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 6},
                        "content": [
                            {
                                "component": "VTextField",
                                "props": {
                                    "model": "user_share_pan_path",
                                    "label": "分享文件夹路径",
                                },
                            }
                        ],
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 6},
                        "content": [
                            {
                                "component": "VTextField",
                                "props": {
                                    "model": "user_share_local_path",
                                    "label": "本地生成STRM路径",
                                },
                            }
                        ],
                    },
                ],
            },
        ]

        cleanup_tab = [
            {
                "component": "VRow",
                "content": [
                    {
                        "component": "VCol",
                        "props": {"cols": 12},
                        "content": [
                            {
                                "component": "VAlert",
                                "props": {
                                    "type": "info",
                                    "variant": "tonal",
                                    "density": "compact",
                                    "text": "受限 OpenList 账号无回收站/秒传目录接口，相关清理为安全空操作，仅保留配置项以兼容。",
                                },
                            }
                        ],
                    }
                ],
            },
            {
                "component": "VRow",
                "content": [
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 4},
                        "content": [
                            {
                                "component": "VSwitch",
                                "props": {
                                    "model": "clear_recyclebin_enabled",
                                    "label": "清空回收站",
                                },
                            }
                        ],
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 4},
                        "content": [
                            {
                                "component": "VSwitch",
                                "props": {
                                    "model": "clear_receive_path_enabled",
                                    "label": "清空我的秒传目录",
                                },
                            }
                        ],
                    },
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "md": 4},
                        "content": [
                            {
                                "component": "VCronField",
                                "props": {"model": "cron_clear", "label": "清理周期"},
                            }
                        ],
                    },
                ],
            },
        ]

        return [
            {
                "component": "VCard",
                "props": {"variant": "outlined", "class": "mb-3"},
                "content": [
                    {
                        "component": "VCardTitle",
                        "props": {"class": "d-flex align-center"},
                        "content": [
                            {
                                "component": "VIcon",
                                "props": {
                                    "icon": "mdi-cog",
                                    "color": "primary",
                                    "class": "mr-2",
                                },
                            },
                            {"component": "span", "text": "基础设置"},
                        ],
                    },
                    {"component": "VDivider"},
                    {
                        "component": "VCardText",
                        "content": [
                            {
                                "component": "VRow",
                                "content": [
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 3},
                                        "content": [
                                            {
                                                "component": "VSwitch",
                                                "props": {
                                                    "model": "enabled",
                                                    "label": "启用插件",
                                                },
                                            }
                                        ],
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 9},
                                        "content": [
                                            {
                                                "component": "VTextField",
                                                "props": {
                                                    "model": "ol_url",
                                                    "label": "OpenList 地址",
                                                    "placeholder": "http://192.168.5.17:5244",
                                                },
                                            }
                                        ],
                                    },
                                ],
                            },
                            {
                                "component": "VRow",
                                "content": [
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 4},
                                        "content": [
                                            {
                                                "component": "VTextField",
                                                "props": {
                                                    "model": "ol_user",
                                                    "label": "OpenList 账号",
                                                },
                                            }
                                        ],
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 4},
                                        "content": [
                                            {
                                                "component": "VTextField",
                                                "props": {
                                                    "model": "ol_pass",
                                                    "label": "OpenList 密码",
                                                    "type": "password",
                                                },
                                            }
                                        ],
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 4},
                                        "content": [
                                            {
                                                "component": "VTextField",
                                                "props": {
                                                    "model": "abs_prefix",
                                                    "label": "绝对路径前缀(账号base)",
                                                    "placeholder": "/123pan",
                                                },
                                            }
                                        ],
                                    },
                                ],
                            },
                            {
                                "component": "VRow",
                                "content": [
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12},
                                        "content": [
                                            {
                                                "component": "VTextField",
                                                "props": {
                                                    "model": "user_rmt_mediaext",
                                                    "label": "可整理媒体文件扩展名",
                                                },
                                            }
                                        ],
                                    }
                                ],
                            },
                            {
                                "component": "VRow",
                                "content": [
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12},
                                        "content": [
                                            {
                                                "component": "VTextField",
                                                "props": {
                                                    "model": "user_download_mediaext",
                                                    "label": "可下载媒体数据文件扩展名",
                                                },
                                            }
                                        ],
                                    }
                                ],
                            },
                        ],
                    },
                ],
            },
            {
                "component": "VCard",
                "props": {"variant": "outlined"},
                "content": [
                    {
                        "component": "VTabs",
                        "props": {"model": "tab", "grow": True, "color": "primary"},
                        "content": [
                            {
                                "component": "VTab",
                                "props": {"value": "tab-transfer"},
                                "content": [
                                    {
                                        "component": "VIcon",
                                        "props": {
                                            "icon": "mdi-file-move-outline",
                                            "start": True,
                                            "color": "#1976D2",
                                        },
                                    },
                                    {"component": "span", "text": "监控MP整理"},
                                ],
                            },
                            {
                                "component": "VTab",
                                "props": {"value": "tab-sync"},
                                "content": [
                                    {
                                        "component": "VIcon",
                                        "props": {
                                            "icon": "mdi-sync",
                                            "start": True,
                                            "color": "#4CAF50",
                                        },
                                    },
                                    {"component": "span", "text": "全量同步"},
                                ],
                            },
                            {
                                "component": "VTab",
                                "props": {"value": "tab-share"},
                                "content": [
                                    {
                                        "component": "VIcon",
                                        "props": {
                                            "icon": "mdi-share-variant-outline",
                                            "start": True,
                                            "color": "#009688",
                                        },
                                    },
                                    {"component": "span", "text": "分享生成STRM"},
                                ],
                            },
                            {
                                "component": "VTab",
                                "props": {"value": "tab-cleanup"},
                                "content": [
                                    {
                                        "component": "VIcon",
                                        "props": {
                                            "icon": "mdi-broom",
                                            "start": True,
                                            "color": "#FF9800",
                                        },
                                    },
                                    {"component": "span", "text": "定期清理"},
                                ],
                            },
                        ],
                    },
                    {"component": "VDivider"},
                    {
                        "component": "VWindow",
                        "props": {"model": "tab"},
                        "content": [
                            {
                                "component": "VWindowItem",
                                "props": {"value": "tab-transfer"},
                                "content": [
                                    {
                                        "component": "VCardText",
                                        "content": transfer_monitor_tab,
                                    }
                                ],
                            },
                            {
                                "component": "VWindowItem",
                                "props": {"value": "tab-sync"},
                                "content": [
                                    {"component": "VCardText", "content": full_sync_tab}
                                ],
                            },
                            {
                                "component": "VWindowItem",
                                "props": {"value": "tab-share"},
                                "content": [
                                    {
                                        "component": "VCardText",
                                        "content": share_generate_tab,
                                    }
                                ],
                            },
                            {
                                "component": "VWindowItem",
                                "props": {"value": "tab-cleanup"},
                                "content": [
                                    {"component": "VCardText", "content": cleanup_tab}
                                ],
                            },
                        ],
                    },
                ],
            },
        ], {
            "enabled": False,
            "once_full_sync_strm": False,
            "ol_url": "http://192.168.5.17:5244",
            "ol_user": "moviepilot",
            "ol_pass": "",
            "abs_prefix": "/123pan",
            "user_rmt_mediaext": "mp4,mkv,ts,iso,rmvb,avi,mov,mpeg,mpg,wmv,3gp,asf,m4v,flv,m2ts,tp,f4v",
            "user_download_mediaext": "srt,ssa,ass,nfo,jpg,jpeg,png",
            "transfer_monitor_enabled": False,
            "transfer_monitor_paths": "",
            "monitor_storage": "alist",
            "transfer_monitor_scrape_metadata_enabled": False,
            "transfer_mp_mediaserver_paths": "",
            "transfer_monitor_media_server_refresh_enabled": False,
            "transfer_monitor_mediaservers": [],
            "timing_full_sync_strm": False,
            "full_sync_auto_download_mediainfo_enabled": False,
            "cron_full_sync_strm": "0 */7 * * *",
            "full_sync_strm_paths": "",
            "full_sync_overwrite_mode": "never",
            "share_strm_enabled": False,
            "share_strm_auto_download_mediainfo_enabled": False,
            "user_share_code": "",
            "user_share_pwd": "",
            "user_share_pan_path": "/",
            "user_share_local_path": "",
            "clear_recyclebin_enabled": False,
            "clear_receive_path_enabled": False,
            "cron_clear": "0 */7 * * *",
            "tab": "tab-transfer",
        }

    def stop_service(self):
        """
        退出插件
        """
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception as e:
            print(str(e))
