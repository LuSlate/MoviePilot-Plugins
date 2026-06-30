import json
import os
import threading
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from typing import Any, List, Dict, Tuple, Optional

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from app.core.config import settings
from app.core.event import eventmanager, Event
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import EventType


class _MonitorHandler(FileSystemEventHandler):
    def __init__(self, plugin):
        self.plugin = plugin

    def on_created(self, event):
        if not event.is_directory:
            self.plugin.on_dir_change(event.src_path)

    def on_moved(self, event):
        self.plugin.on_dir_change(getattr(event, "dest_path", ""))


class OpenListStrm(_PluginBase):
    # 插件元数据
    plugin_name = "OpenList Strm生成"
    plugin_desc = "遍历 OpenList(alist) 存储为云盘媒体生成带签名直链的 .strm；支持整理事件与目录监听增量生成，供 Emby/Jellyfin 直连 CDN 播放。"
    plugin_icon = "https://cdn.oplist.org/gh/OpenListTeam/Logo@main/logo/logo.png"
    plugin_version = "1.1"
    plugin_author = "lyndon"
    author_url = "https://github.com/LuSlate"
    plugin_config_prefix = "openliststrm_"
    plugin_order = 27
    auth_level = 1

    # 配置
    _enabled = False
    _onlyonce = False
    _cron = ""
    _ol_url = ""
    _ol_user = ""
    _ol_pass = ""
    _ol_root = ""
    _abs_prefix = ""
    _lib_root = ""
    _exts = ""
    _overwrite = False
    _on_transfer = False
    _storage_name = ""
    _monitor_dirs = ""
    _scheduler: Optional[BackgroundScheduler] = None
    _observers: list = []
    _lock = threading.Lock()

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = config.get("enabled")
            self._onlyonce = config.get("onlyonce")
            self._cron = config.get("cron")
            self._ol_url = (config.get("ol_url") or "").rstrip("/")
            self._ol_user = config.get("ol_user")
            self._ol_pass = config.get("ol_pass")
            self._ol_root = (config.get("ol_root") or "").rstrip("/")
            self._abs_prefix = (config.get("abs_prefix") or "").rstrip("/")
            self._lib_root = (config.get("lib_root") or "").rstrip("/")
            self._exts = config.get("exts") or "mp4,mkv,ts,iso,rmvb,avi,mov,mpeg,mpg,wmv,3gp,asf,m4v,flv,m2ts,tp,f4v"
            self._overwrite = config.get("overwrite")
            self._on_transfer = config.get("on_transfer")
            self._storage_name = config.get("storage_name") or ""
            self._monitor_dirs = config.get("monitor_dirs") or ""

        self.stop_service()

        if not (self._enabled or self._onlyonce):
            return

        self._scheduler = BackgroundScheduler(timezone=settings.TZ)

        # 目录监听
        if self._enabled and self._monitor_dirs:
            self._observers = []
            for d in self._monitor_dirs.split("\n"):
                d = d.strip()
                if not d or not os.path.isdir(d):
                    continue
                try:
                    ob = Observer()
                    ob.schedule(_MonitorHandler(self), d, recursive=True)
                    ob.daemon = True
                    ob.start()
                    self._observers.append(ob)
                    logger.info(f"OpenListStrm 已监听目录: {d}")
                except Exception as e:
                    logger.error(f"OpenListStrm 监听 {d} 失败: {e}")

        if self._onlyonce:
            logger.info("OpenListStrm 立即全量运行一次")
            self._scheduler.add_job(
                func=self.scan, trigger="date",
                run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=3),
                name="OpenListStrm 全量生成")
            self._onlyonce = False
            self.__update_config()

        if self._scheduler.get_jobs():
            self._scheduler.print_jobs()
            self._scheduler.start()

    def __update_config(self):
        self.update_config({
            "enabled": self._enabled, "onlyonce": self._onlyonce, "cron": self._cron,
            "ol_url": self._ol_url, "ol_user": self._ol_user, "ol_pass": self._ol_pass,
            "ol_root": self._ol_root, "abs_prefix": self._abs_prefix, "lib_root": self._lib_root,
            "exts": self._exts, "overwrite": self._overwrite,
            "on_transfer": self._on_transfer, "storage_name": self._storage_name,
            "monitor_dirs": self._monitor_dirs,
        })

    # ---------------- OpenList API ----------------
    def __login(self) -> str:
        data = json.dumps({"username": self._ol_user, "password": self._ol_pass}).encode()
        req = urllib.request.Request(self._ol_url + "/api/auth/login", data=data,
                                     headers={"Content-Type": "application/json"})
        r = json.loads(urllib.request.urlopen(req, timeout=30).read())
        if r.get("code") != 200:
            raise RuntimeError(f"OpenList 登录失败: {r.get('message')}")
        return r["data"]["token"]

    def __list(self, token: str, path: str) -> list:
        data = json.dumps({"path": path, "page": 1, "per_page": 0, "refresh": False}).encode()
        req = urllib.request.Request(self._ol_url + "/api/fs/list", data=data,
                                     headers={"Content-Type": "application/json", "Authorization": token})
        r = json.loads(urllib.request.urlopen(req, timeout=120).read())
        if r.get("code") != 200:
            logger.warn(f"OpenListStrm 列目录失败 {path}: {r.get('message')}")
            return []
        return (r.get("data") or {}).get("content") or []

    # ---------------- 核心：遍历生成 ----------------
    def scan(self):
        self.__run(self._ol_root or "/")

    def __run(self, start_rel: str):
        if not (self._ol_url and self._lib_root and self._ol_user):
            logger.error("OpenListStrm 配置不完整（OpenList地址/账号/媒体库根目录必填）")
            return
        if not self._lock.acquire(blocking=False):
            logger.info("OpenListStrm 已有任务在跑，跳过本次")
            return
        try:
            token = self.__login()
        except Exception as e:
            logger.error(f"OpenListStrm: {e}")
            self._lock.release()
            return
        try:
            exts = {"." + e.strip().lower().lstrip(".") for e in self._exts.split(",") if e.strip()}
            created = updated = skipped = 0
            stack = [start_rel or "/"]
            while stack:
                cur = stack.pop()
                for it in self.__list(token, cur):
                    name = it.get("name")
                    full = (cur.rstrip("/") + "/" + name) if cur != "/" else "/" + name
                    if it.get("is_dir"):
                        stack.append(full)
                        continue
                    if os.path.splitext(name)[1].lower() not in exts:
                        continue
                    sign = it.get("sign") or ""
                    rel = full[len(self._ol_root):] if self._ol_root and full.startswith(self._ol_root) else full
                    rel = rel.lstrip("/")
                    strm_path = os.path.join(self._lib_root, os.path.splitext(rel)[0] + ".strm")
                    url = self._ol_url + "/d" + urllib.parse.quote(self._abs_prefix + full)
                    if sign:
                        url += "?sign=" + sign
                    try:
                        if os.path.exists(strm_path):
                            if not self._overwrite and open(strm_path, encoding="utf-8").read().strip() == url:
                                skipped += 1
                                continue
                            open(strm_path, "w", encoding="utf-8").write(url)
                            updated += 1
                        else:
                            os.makedirs(os.path.dirname(strm_path), exist_ok=True)
                            open(strm_path, "w", encoding="utf-8").write(url)
                            created += 1
                    except Exception as e:
                        logger.error(f"OpenListStrm 写入失败 {strm_path}: {e}")
            logger.info(f"OpenListStrm 完成[{start_rel}]：新建 {created}，更新 {updated}，跳过 {skipped}")
            if created or updated:
                self.systemmessage.put(f"OpenListStrm：新建 {created} 更新 {updated} 跳过 {skipped}", title="OpenList Strm生成")
        finally:
            self._lock.release()

    # ---------------- 整理完成事件 ----------------
    @eventmanager.register(EventType.TransferComplete)
    def on_transfer_complete(self, event: Event):
        if not (self._enabled and self._on_transfer):
            return
        try:
            data = event.event_data or {}
            tr = data.get("transferinfo")
            if not tr or not getattr(tr, "target_diritem", None):
                return
            storage = getattr(getattr(tr, "target_item", None), "storage", "") or ""
            if self._storage_name and storage and storage != self._storage_name:
                return
            d = tr.target_diritem.path or ""
            if self._abs_prefix and d.startswith(self._abs_prefix):
                d = d[len(self._abs_prefix):]
            if not d.startswith("/"):
                d = "/" + d
            logger.info(f"OpenListStrm 整理完成事件 → 增量生成: {d}")
            self.__run(d)
        except Exception as e:
            logger.error(f"OpenListStrm 事件处理失败: {e}")

    # ---------------- 目录监听回调 ----------------
    def on_dir_change(self, path: str):
        if not (self._enabled and self._scheduler):
            return
        ext = os.path.splitext(path or "")[1].lower().lstrip(".")
        if ext and ext not in self._exts:
            return
        # 去抖：5 秒后跑一次全量增量
        try:
            self._scheduler.add_job(func=self.scan, trigger="date",
                                    run_date=datetime.now(tz=pytz.timezone(settings.TZ)) + timedelta(seconds=5),
                                    id="openliststrm_dirchange", name="OpenListStrm 目录变更增量",
                                    replace_existing=True)
        except Exception:
            pass

    # ---------------- MP 接口 ----------------
    def get_state(self) -> bool:
        return bool(self._enabled)

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        pass

    def get_api(self) -> List[Dict[str, Any]]:
        pass

    def get_service(self) -> List[Dict[str, Any]]:
        if self._enabled and self._cron:
            return [{"id": "OpenListStrm", "name": "OpenList Strm生成服务",
                     "trigger": CronTrigger.from_crontab(self._cron), "func": self.scan, "kwargs": {}}]
        return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        def col(width, comp):
            return {"component": "VCol", "props": {"cols": 12, "md": width}, "content": [comp]}

        def tf(model, label, placeholder=""):
            return {"component": "VTextField", "props": {"model": model, "label": label, "placeholder": placeholder}}

        return [
            {"component": "VForm", "content": [
                {"component": "VRow", "content": [
                    col(3, {"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}}),
                    col(3, {"component": "VSwitch", "props": {"model": "onlyonce", "label": "立即全量一次"}}),
                    col(3, {"component": "VSwitch", "props": {"model": "on_transfer", "label": "整理完成即生成"}}),
                    col(3, {"component": "VSwitch", "props": {"model": "overwrite", "label": "强制覆盖"}}),
                ]},
                {"component": "VRow", "content": [
                    col(6, tf("ol_url", "OpenList 地址", "http://192.168.5.17:5244")),
                    col(6, tf("cron", "生成周期 (cron)", "0 5 * * *")),
                ]},
                {"component": "VRow", "content": [
                    col(6, tf("ol_user", "OpenList 账号", "moviepilot")),
                    col(6, {"component": "VTextField", "props": {"model": "ol_pass", "label": "OpenList 密码", "type": "password"}}),
                ]},
                {"component": "VRow", "content": [
                    col(4, tf("ol_root", "遍历根(账号相对)", "/")),
                    col(4, tf("abs_prefix", "绝对路径前缀(账号base)", "/123pan")),
                    col(4, tf("lib_root", "媒体库根目录(容器内)", "/media/libraries")),
                ]},
                {"component": "VRow", "content": [
                    col(6, tf("storage_name", "整理事件匹配存储名(空=全部)", "OpenList")),
                    col(6, tf("exts", "媒体扩展名(逗号分隔)")),
                ]},
                {"component": "VRow", "content": [
                    col(12, tf("monitor_dirs", "监听本地目录(每行一个,可选)")),
                ]},
                {"component": "VRow", "content": [
                    col(12, {"component": "VAlert", "props": {"type": "info", "variant": "tonal",
                             "text": "OpenList存储根 与 媒体库根 目录结构需镜像。strm=OpenList /d 直链+sign(永久)。整理完成即生成需勾选并填对应存储名；首次开“立即全量”可迁移旧 strm。"}}),
                ]},
            ]}
        ], {
            "enabled": False, "onlyonce": False, "on_transfer": False, "overwrite": False,
            "cron": "0 5 * * *", "ol_url": "http://192.168.5.17:5244",
            "ol_user": "moviepilot", "ol_pass": "", "ol_root": "/", "abs_prefix": "/123pan",
            "lib_root": "/media/libraries", "storage_name": "OpenList", "monitor_dirs": "",
            "exts": "mp4,mkv,ts,iso,rmvb,avi,mov,mpeg,mpg,wmv,3gp,asf,m4v,flv,m2ts,tp,f4v",
        }

    def get_page(self) -> List[dict]:
        pass

    def stop_service(self):
        try:
            for ob in (self._observers or []):
                try:
                    ob.stop()
                    ob.join(timeout=3)
                except Exception:
                    pass
            self._observers = []
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown()
                self._scheduler = None
        except Exception as e:
            logger.error(f"OpenListStrm 停止服务失败: {e}")
