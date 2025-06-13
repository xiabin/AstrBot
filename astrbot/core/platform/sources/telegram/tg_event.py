import os
import re
import asyncio
import telegramify_markdown
from astrbot.api.event import AstrMessageEvent, MessageChain
from astrbot.api.platform import AstrBotMessage, PlatformMetadata, MessageType
from astrbot.api.message_components import (
    Plain,
    Image,
    Reply,
    At,
    File,
    Record,
)
from telegram.ext import ExtBot
from astrbot.core.utils.io import download_file
from astrbot import logger
from astrbot.core.utils.astrbot_path import get_astrbot_data_path


class TelegramPlatformEvent(AstrMessageEvent):
    # Telegram 的最大消息长度限制
    MAX_MESSAGE_LENGTH = 4096

    SPLIT_PATTERNS = {
        "paragraph": re.compile(r"\n\n"),
        "line": re.compile(r"\n"),
        "sentence": re.compile(r"[.!?。！？]"),
        "word": re.compile(r"\s"),
    }

    def __init__(
        self,
        message_str: str,
        message_obj: AstrBotMessage,
        platform_meta: PlatformMetadata,
        session_id: str,
        client: ExtBot,
        business_connections: dict = None,
    ):
        super().__init__(message_str, message_obj, platform_meta, session_id)
        self.client = client
        self.business_connections = business_connections or {}

    def _split_message(self, text: str) -> list[str]:
        if len(text) <= self.MAX_MESSAGE_LENGTH:
            return [text]

        chunks = []
        while text:
            if len(text) <= self.MAX_MESSAGE_LENGTH:
                chunks.append(text)
                break

            split_point = self.MAX_MESSAGE_LENGTH
            segment = text[: self.MAX_MESSAGE_LENGTH]

            for _, pattern in self.SPLIT_PATTERNS.items():
                if matches := list(pattern.finditer(segment)):
                    last_match = matches[-1]
                    split_point = last_match.end()
                    break

            chunks.append(text[:split_point])
            text = text[split_point:].lstrip()

        return chunks

    @staticmethod
    async def send_with_client(
        client: ExtBot, message: MessageChain, user_name: str, business_connections: dict = None
    ):
        if business_connections is None:
            business_connections = {}
            
        image_path = None

        has_reply = False
        reply_message_id = None
        at_user_id = None
        for i in message.chain:
            if isinstance(i, Reply):
                has_reply = True
                reply_message_id = i.id
            if isinstance(i, At):
                at_user_id = i.name

        at_flag = False
        message_thread_id = None
        business_connection_id = None
        
        # Parse session_id to extract business connection info
        if "#business#" in user_name:
            parts = user_name.split("#business#")
            user_name = parts[0]
            business_connection_id = parts[1]
            
            # Check business connection permissions
            if business_connection_id in business_connections:
                connection_info = business_connections[business_connection_id]
                if not connection_info.get('can_reply', False):
                    logger.warning(f"Bot cannot reply in business connection {business_connection_id}")
                    return
                if not connection_info.get('is_enabled', False):
                    logger.warning(f"Business connection {business_connection_id} is disabled")
                    return
            else:
                logger.warning(f"Business connection {business_connection_id} not found in stored connections, but still trying to send with business_connection_id")
        elif "#" in user_name:
            # it's a supergroup chat with message_thread_id
            user_name, message_thread_id = user_name.split("#")
            
        for i in message.chain:
            payload = {
                "chat_id": user_name,
            }
            if has_reply:
                payload["reply_to_message_id"] = reply_message_id
            if message_thread_id:
                payload["message_thread_id"] = message_thread_id
            if business_connection_id:
                payload["business_connection_id"] = business_connection_id

            if isinstance(i, Plain):
                if at_user_id and not at_flag:
                    i.text = f"@{at_user_id} {i.text}"
                    at_flag = True
                chunks = TelegramPlatformEvent._split_message_static(i.text)
                for chunk in chunks:
                    try:
                        md_text = telegramify_markdown.markdownify(
                            chunk, max_line_length=None, normalize_whitespace=False
                        )
                        await client.send_message(
                            text=md_text, parse_mode="MarkdownV2", **payload
                        )
                    except Exception as e:
                        logger.warning(
                            f"MarkdownV2 send failed: {e}. Using plain text instead."
                        )
                        await client.send_message(text=chunk, **payload)
            elif isinstance(i, Image):
                image_path = await i.convert_to_file_path()
                await client.send_photo(photo=image_path, **payload)
            elif isinstance(i, File):
                if i.file.startswith("https://"):
                    temp_dir = os.path.join(get_astrbot_data_path(), "temp")
                    path = os.path.join(temp_dir, i.name)
                    await download_file(i.file, path)
                    i.file = path

                await client.send_document(document=i.file, filename=i.name, **payload)
            elif isinstance(i, Record):
                path = await i.convert_to_file_path()
                await client.send_voice(voice=path, **payload)

    @staticmethod
    def _split_message_static(text: str) -> list[str]:
        """Static version of _split_message for use in static method"""
        MAX_MESSAGE_LENGTH = 4096
        SPLIT_PATTERNS = {
            "paragraph": re.compile(r"\n\n"),
            "line": re.compile(r"\n"),
            "sentence": re.compile(r"[.!?。！？]"),
            "word": re.compile(r"\s"),
        }
        
        if len(text) <= MAX_MESSAGE_LENGTH:
            return [text]

        chunks = []
        while text:
            if len(text) <= MAX_MESSAGE_LENGTH:
                chunks.append(text)
                break

            split_point = MAX_MESSAGE_LENGTH
            segment = text[:MAX_MESSAGE_LENGTH]

            for _, pattern in SPLIT_PATTERNS.items():
                if matches := list(pattern.finditer(segment)):
                    last_match = matches[-1]
                    split_point = last_match.end()
                    break

            chunks.append(text[:split_point])
            text = text[split_point:].lstrip()

        return chunks

    async def send(self, message: MessageChain):
        await self.send_with_client(self.client, message, self.get_session_id(), self.business_connections)
        await super().send(message)

    async def send_streaming(self, generator, use_fallback: bool = False):
        message_thread_id = None
        business_connection_id = None

        # Parse session_id to extract business connection info
        session_id = self.get_session_id()
        user_name = session_id  # Default to session_id
        
        if "#business#" in session_id:
            parts = session_id.split("#business#")
            user_name = parts[0]
            business_connection_id = parts[1]
            
            # Check business connection permissions
            if business_connection_id in self.business_connections:
                connection_info = self.business_connections[business_connection_id]
                if not connection_info.get('can_reply', False):
                    logger.warning(f"Bot cannot reply in business connection {business_connection_id}")
                    return await super().send_streaming(generator, use_fallback)
                if not connection_info.get('is_enabled', False):
                    logger.warning(f"Business connection {business_connection_id} is disabled")
                    return await super().send_streaming(generator, use_fallback)
            else:
                logger.warning(f"Business connection {business_connection_id} not found in stored connections, but still trying to send with business_connection_id")
        elif "#" in session_id and self.get_message_type() == MessageType.GROUP_MESSAGE:
            # it's a supergroup chat with message_thread_id
            user_name, message_thread_id = session_id.split("#")
            
        payload = {
            "chat_id": user_name,
        }
        if message_thread_id:
            payload["message_thread_id"] = message_thread_id
        if business_connection_id:
            payload["business_connection_id"] = business_connection_id

        delta = ""
        current_content = ""
        message_id = None
        last_edit_time = 0  # 上次编辑消息的时间
        throttle_interval = 0.6  # 编辑消息的间隔时间 (秒)

        async for chain in generator:
            if isinstance(chain, MessageChain):
                # 处理消息链中的每个组件
                for i in chain.chain:
                    if isinstance(i, Plain):
                        delta += i.text
                    elif isinstance(i, Image):
                        image_path = await i.convert_to_file_path()
                        await self.client.send_photo(photo=image_path, **payload)
                        continue
                    elif isinstance(i, File):
                        if i.file.startswith("https://"):
                            temp_dir = os.path.join(get_astrbot_data_path(), "temp")
                            path = os.path.join(temp_dir, i.name)
                            await download_file(i.file, path)
                            i.file = path

                        await self.client.send_document(
                            document=i.file, filename=i.name, **payload
                        )
                        continue
                    elif isinstance(i, Record):
                        path = await i.convert_to_file_path()
                        await self.client.send_voice(voice=path, **payload)
                        continue
                    else:
                        logger.warning(f"不支持的消息类型: {type(i)}")
                        continue

                # Plain
                if message_id and len(delta) <= self.MAX_MESSAGE_LENGTH:
                    current_time = asyncio.get_event_loop().time()
                    time_since_last_edit = current_time - last_edit_time

                    # 如果距离上次编辑的时间 >= 设定的间隔，等待一段时间
                    if time_since_last_edit >= throttle_interval:
                        # 编辑消息
                        try:
                            await self.client.edit_message_text(
                                text=delta,
                                chat_id=payload["chat_id"],
                                message_id=message_id,
                            )
                            current_content = delta
                        except Exception as e:
                            logger.warning(f"编辑消息失败(streaming): {e!s}")
                        last_edit_time = (
                            asyncio.get_event_loop().time()
                        )  # 更新上次编辑的时间
                else:
                    # delta 长度一般不会大于 4096，因此这里直接发送
                    try:
                        msg = await self.client.send_message(text=delta, **payload)
                        current_content = delta
                        delta = ""
                    except Exception as e:
                        logger.warning(f"发送消息失败(streaming): {e!s}")
                    message_id = msg.message_id
                    last_edit_time = (
                        asyncio.get_event_loop().time()
                    )  # 记录初始消息发送时间

        try:
            if delta and current_content != delta:
                try:
                    markdown_text = telegramify_markdown.markdownify(
                        delta, max_line_length=None, normalize_whitespace=False
                    )
                    await self.client.edit_message_text(
                        text=markdown_text,
                        chat_id=payload["chat_id"],
                        message_id=message_id,
                        parse_mode="MarkdownV2",
                    )
                except Exception as e:
                    logger.warning(f"Markdown转换失败，使用普通文本: {e!s}")
                    await self.client.edit_message_text(
                        text=delta, chat_id=payload["chat_id"], message_id=message_id
                    )
        except Exception as e:
            logger.warning(f"编辑消息失败(streaming): {e!s}")

        return await super().send_streaming(generator, use_fallback)
