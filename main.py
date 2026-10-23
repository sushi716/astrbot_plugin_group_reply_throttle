# -*- coding: utf-8 -*-
import os
from pathlib import Path
"""群聊回复节流插件

问题：群聊多个主动回复机制（锚点 30%、iris 采样、2% 插话）各自独立触发，
同一群里短时间内的多条回复容易互相撞车。

方案：15 秒节流窗口——同一群在窗口内只保留第一条主动回复，
后续到达的主动回复被吞掉（@ 触发的必回不受影响）。
"""
import time

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star
from astrbot.core.message.message_event_result import ResultContentType
from astrbot.core.platform.message_type import MessageType

THROTTLE_SECONDS = 15  # 节流窗口（秒）


class GroupReplyThrottle(Star):
    def __init__(self, context: Context, config: dict = None):
        super().__init__(context)
        self.config = config or {}
        # gid -> 最近一次 bot 在该群发送消息的时间
        self._last_sent: dict[str, float] = {}

    @staticmethod
    def _absorb_shared_stamp() -> None:
        """2026-10-21（P2-12）：把 group_anchor_reply 写的发送时间并进来。

        以后不再依赖协议端回报"自发消息"来喂节流窗口：插件自己发完就写文件，
        这里每次收到群消息时读一次，谁发的都能看见。
        """
        try:
            import json
            from pathlib import Path

            p = SHARED_STAMP_FILE
            if not p.exists():
                return
            data = json.loads(p.read_text(encoding="utf-8-sig")) or {}
            for gid, ts in data.items():
                try:
                    ts = float(ts)
                except (TypeError, ValueError):
                    continue
                if ts > GroupReplyThrottle._shared_last.get(gid, 0):
                    GroupReplyThrottle._shared_last[gid] = ts
        except Exception:
            pass

    _shared_last: dict = {}

    @filter.after_message_sent()
    async def on_after_sent(self, event: AstrMessageEvent):
        """bot 消息实际发送后：记录该群发送时间"""
        try:
            if event.get_message_type() != MessageType.GROUP_MESSAGE:
                return
            gid = str(event.get_group_id() or "")
            if gid:
                self._last_sent[gid] = time.time()
                self._absorb_shared_stamp()
        except Exception as e:
            logger.debug(f"[group_reply_throttle] record fail: {e}")

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE, priority=99999)
    async def on_group_message(self, event: AstrMessageEvent):
        """把"她自己发出的群消息"也记进节流表（2026-10-03 修）

        背景：group_anchor_reply 是 context.send_message 直发、不经过管线，
        after_message_sent 不会触发 → 节流看不到那次发送 → 8 秒内会漏出第二条回复。
        NapCat 的 onebot11 配置开了 reportSelfMessage，她自己发的消息会作为事件回报，
        这里用"自发消息"回填时间戳，让**任何来源**的群发送都对节流可见。
        """
        try:
            if event.get_message_type() != MessageType.GROUP_MESSAGE:
                return
            sender = str(event.get_sender_id() or "")
            self_id = str(event.get_self_id() or "")
            if not sender or not self_id or sender != self_id:
                return
            gid = str(event.get_group_id() or "")
            if gid:
                self._last_sent[gid] = time.time()
                logger.info(f"[group_reply_throttle] self-send recorded in {gid} (throttle window reset)")
        except Exception as e:
            logger.debug(f"[group_reply_throttle] self-send record fail: {e}")

    @filter.on_decorating_result(priority=99998)
    async def on_result(self, event: AstrMessageEvent):
        """群聊主动回复：窗口内已有回复则吞掉本条"""
        try:
            if event.get_message_type() != MessageType.GROUP_MESSAGE:
                return
            # @ 触发的必回不节流
            try:
                if event.is_at_or_wake_command:
                    return
            except Exception:
                pass

            result = event.get_result()
            if not result or not result.chain:
                return
            if result.result_content_type == ResultContentType.STREAMING_RESULT:
                return

            gid = str(event.get_group_id() or "")
            if not gid:
                return
            now = time.time()
            last = self._last_sent.get(gid, 0)
            if now - last < THROTTLE_SECONDS:
                logger.info(f"[group_reply_throttle] throttled reply in {gid} ({now - last:.0f}s since last)")
                result.chain = []  # 空 chain 会被管线跳过发送
        except Exception as e:
            logger.error(f"[group_reply_throttle] error: {e}")
