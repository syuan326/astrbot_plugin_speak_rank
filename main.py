import json
import aiosqlite
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, register
from astrbot.api import logger, AstrBotConfig
from astrbot.api.message_components import Plain, Image
from astrbot.api.star import StarTools


@register(
    name="astrbot_plugin_speak_rank",
    author="Your Name",
    desc="一个发言排行统计插件，按天统计群内成员的文本和图片发言情况，定时发送排行榜图片并自动清理数据。",
    version="1.0.0",
    repo="https://github.com/yourusername/astrbot_plugin_speak_rank"
)
class SpeakRankPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        """
        初始化插件
        
        Args:
            context: 插件上下文
            config: 插件配置对象
        """
        super().__init__(context)
        self.config = config
        
        # 获取插件数据目录
        self.data_dir = StarTools.get_data_dir("astrbot_plugin_speak_rank")
        
        # 初始化数据库
        self.db_path = f"{self.data_dir}/speak_data.db"
        asyncio.create_task(self.init_database())
        
        # 读取配置
        self.whitelist_groups = set(self.config.get("whitelist_groups", []))
        self.schedule_time = self.config.get("schedule_time", "22:00")
        self.max_users_in_rank = self.config.get("max_users_in_rank", 10)
        self.image_template = self.config.get("image_template", "")
        self.group_sessions: Dict[str, str] = {}
        
        # 注册定时任务
        asyncio.create_task(self.schedule_task())
        
        logger.info("SpeakRankPlugin initialized successfully.")

    async def init_database(self):
        """初始化SQLite数据库"""
        async with aiosqlite.connect(self.db_path) as conn:
            cursor = await conn.cursor()
            
            # 创建表：存储每日发言数据
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS daily_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    user_name TEXT,
                    text_count INTEGER DEFAULT 0,
                    image_count INTEGER DEFAULT 0,
                    session_id TEXT,
                    date DATE NOT NULL,
                    UNIQUE(group_id, user_id, date)
                )
            """)

            # 兼容旧版本数据库
            await cursor.execute("PRAGMA table_info(daily_stats)")
            columns = {row[1] for row in await cursor.fetchall()}
            if "session_id" not in columns:
                await cursor.execute("ALTER TABLE daily_stats ADD COLUMN session_id TEXT")
            
            await conn.commit()

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    @filter.platform_adapter_type(filter.PlatformAdapterType.ALL)
    async def on_group_message(self, event: AstrMessageEvent):
        """
        监听群消息事件，统计发言数据
        
        Args:
            event: 消息事件对象
        """
        try:
            group_id = event.get_group_id()
            
            # 检查是否在白名单中
            if self.whitelist_groups and group_id not in self.whitelist_groups:
                return
                
            user_id = event.get_sender_id()
            user_name = event.get_sender_name()
            session_id = self._extract_group_session_id(event)
            message_type = self._classify_message(event)
            
            if message_type is None:
                return
                
            # 更新统计数据
            await self.update_user_stats(group_id, user_id, user_name, message_type, session_id)
            
        except Exception as e:
            logger.error(f"处理群消息时出错: {e}")

    def _classify_message(self, event: AstrMessageEvent) -> str:
        """
        分类消息类型
        
        Args:
            event: 消息事件对象
            
        Returns:
            str: 消息类型 ("text" 或 "image")，如果不是这两种类型则返回 None
        """
        message_chain = event.message_obj.message
        
        # 统计文本和图片消息
        has_text = any(isinstance(comp, Plain) for comp in message_chain)
        has_image = any(isinstance(comp, Image) for comp in message_chain)
        
        if has_image:
            return "image"
        elif has_text:
            return "text"
        else:
            return None

    def _extract_group_session_id(self, event: AstrMessageEvent) -> Optional[str]:
        """从事件中提取可用于 send_message 的 session_id。"""
        candidates = []

        for name in ("unified_msg_origin", "session_id"):
            value = getattr(event, name, None)
            if value:
                candidates.append(value)

        for method in ("get_unified_msg_origin", "get_session_id"):
            fn = getattr(event, method, None)
            if callable(fn):
                try:
                    value = fn()
                    if value:
                        candidates.append(value)
                except Exception:
                    continue

        message_obj = getattr(event, "message_obj", None)
        if message_obj:
            for name in ("unified_msg_origin", "session_id"):
                value = getattr(message_obj, name, None)
                if value:
                    candidates.append(value)

        for item in candidates:
            if isinstance(item, str) and item.count(":") >= 2:
                group_id = str(event.get_group_id())
                segments = item.split(":")
                if segments[-1] == group_id or group_id in segments:
                    self.group_sessions[group_id] = item
                    return item

        return None

    async def update_user_stats(self, group_id: str, user_id: str, user_name: str, msg_type: str, session_id: Optional[str] = None):
        """
        更新用户统计数据
        
        Args:
            group_id: 群组ID
            user_id: 用户ID
            user_name: 用户昵称
            msg_type: 消息类型 ("text" 或 "image")
        """
        try:
            today = datetime.now().date()
            async with aiosqlite.connect(self.db_path) as conn:
                cursor = await conn.cursor()
                
                # 查找现有记录
                await cursor.execute("""
                    SELECT text_count, image_count FROM daily_stats
                    WHERE group_id=? AND user_id=? AND date=?
                """, (group_id, user_id, today))
                
                row = await cursor.fetchone()
                
                if row:
                    # 更新现有记录
                    text_count, image_count = row
                    if msg_type == "text":
                        text_count += 1
                    elif msg_type == "image":
                        image_count += 1
                        
                    await cursor.execute("""
                        UPDATE daily_stats SET text_count=?, image_count=?, user_name=?, session_id=COALESCE(?, session_id)
                        WHERE group_id=? AND user_id=? AND date=?
                    """, (text_count, image_count, user_name, session_id, group_id, user_id, today))
                else:
                    # 插入新记录
                    text_count = 1 if msg_type == "text" else 0
                    image_count = 1 if msg_type == "image" else 0
                    
                    await cursor.execute("""
                        INSERT INTO daily_stats (group_id, user_id, user_name, text_count, image_count, session_id, date)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (group_id, user_id, user_name, text_count, image_count, session_id, today))
                    
                await conn.commit()
                
        except Exception as e:
            logger.error(f"更新用户统计数据时出错: {e}")

    async def schedule_task(self):
        """定时任务处理器"""
        while True:
            try:
                now = datetime.now()
                target_time = datetime.strptime(self.schedule_time, "%H:%M").time()
                
                # 计算下次执行时间
                next_run = now.replace(hour=target_time.hour, minute=target_time.minute, second=0, microsecond=0)
                if next_run <= now:
                    next_run += timedelta(days=1)
                    
                # 等待到目标时间
                wait_seconds = (next_run - now).total_seconds()
                await asyncio.sleep(wait_seconds)
                
                # 执行定时任务
                await self.generate_and_send_rankings()
                
                # 清理数据
                await self.clear_yesterday_data()
                
            except Exception as e:
                logger.error(f"定时任务执行出错: {e}")
                await asyncio.sleep(60)  # 出错后等待一分钟再试

    async def generate_and_send_rankings(self):
        """生成并发送排行榜"""
        try:
            yesterday = (datetime.now() - timedelta(days=1)).date()
            
            # 获取所有群组的统计数据
            stats = await self.get_daily_stats(yesterday)
            
            for group_id, users_data in stats.items():
                try:
                    session_id = await self.get_group_session(group_id, yesterday)

                    # 生成排行榜图片
                    image_url = await self.generate_ranking_image(group_id, users_data, yesterday)
                    
                    # 发送到群聊
                    if image_url:
                        message_chain = [Image.fromURL(image_url)]
                        sent = await self._send_ranking_to_group(group_id, session_id, message_chain)
                        if not sent:
                            logger.warning(f"群 {group_id} 排行榜发送失败：未找到可用 session")
                        
                except Exception as e:
                    logger.error(f"处理群 {group_id} 的排行榜时出错: {e}")
                    
        except Exception as e:
            logger.error(f"生成排行榜时出错: {e}")

    async def get_daily_stats(self, date) -> Dict[str, List[Dict]]:
        """
        获取指定日期的统计数据
        
        Args:
            date: 日期对象
            
        Returns:
            Dict[str, List[Dict]]: 按群组分类的用户统计数据
        """
        try:
            async with aiosqlite.connect(self.db_path) as conn:
                cursor = await conn.cursor()
                
                await cursor.execute("""
                    SELECT group_id, user_id, user_name, text_count, image_count
                    FROM daily_stats
                    WHERE date=?
                    ORDER BY group_id, (text_count + image_count) DESC
                """, (date,))
                
                rows = await cursor.fetchall()
                
                # 按群组整理数据
                result = {}
                for row in rows:
                    group_id, user_id, user_name, text_count, image_count = row
                    
                    if group_id not in result:
                        result[group_id] = []
                        
                    # 限制最多用户数
                    if len(result[group_id]) < self.max_users_in_rank:
                        result[group_id].append({
                            "user_id": user_id,
                            "user_name": user_name or f"用户{user_id[-4:]}",
                            "text_count": text_count,
                            "image_count": image_count,
                            "avatar": f"https://q1.qlogo.cn/g?b=qq&nk={user_id}&s=640"  # 示例头像URL
                        })
                        
                return result
                
        except Exception as e:
            logger.error(f"获取统计数据时出错: {e}")
            return {}

    async def get_group_session(self, group_id: str, date) -> Optional[str]:
        """优先从内存和数据库中获取群聊 session_id。"""
        cached = self.group_sessions.get(group_id)
        if cached:
            return cached

        try:
            async with aiosqlite.connect(self.db_path) as conn:
                cursor = await conn.cursor()
                await cursor.execute(
                    """
                    SELECT session_id
                    FROM daily_stats
                    WHERE group_id=? AND date=? AND session_id IS NOT NULL AND session_id != ''
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (group_id, date),
                )
                row = await cursor.fetchone()
                if row and row[0]:
                    self.group_sessions[group_id] = row[0]
                    return row[0]

                # 兜底：如果昨日没保留会话，尝试使用该群最近一次可用会话
                await cursor.execute(
                    """
                    SELECT session_id
                    FROM daily_stats
                    WHERE group_id=? AND session_id IS NOT NULL AND session_id != ''
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (group_id,),
                )
                row = await cursor.fetchone()
                if row and row[0]:
                    self.group_sessions[group_id] = row[0]
                    return row[0]
        except Exception as e:
            logger.error(f"获取群 {group_id} session_id 时出错: {e}")

        return None

    def _build_session_candidates(self, group_id: str, session_id: Optional[str]) -> List[str]:
        """构建发送目标候选列表，优先真实会话，最后尝试常见格式。"""
        candidates: List[str] = []

        if session_id:
            candidates.append(session_id)

        known_prefixes = ["aiocqhttp:group", "onebot:group", "qq:group"]
        for prefix in known_prefixes:
            candidates.append(f"{prefix}:{group_id}")

        deduped: List[str] = []
        for candidate in candidates:
            if candidate and candidate not in deduped:
                deduped.append(candidate)
        return deduped

    async def _send_ranking_to_group(self, group_id: str, session_id: Optional[str], message_chain: List[Image]) -> bool:
        """尝试多个 session 发送，提升定时发送成功率。"""
        for target in self._build_session_candidates(group_id, session_id):
            try:
                await self.context.send_message(target, message_chain)
                self.group_sessions[group_id] = target
                return True
            except Exception as e:
                logger.warning(f"发送群 {group_id} 排行榜失败，目标 {target}，原因: {e}")

        return False

    async def generate_ranking_image(self, group_id: str, users_data: List[Dict], date) -> str:
        """
        生成排行榜图片
        
        Args:
            group_id: 群组ID
            users_data: 用户统计数据列表
            date: 统计日期
            
        Returns:
            str: 图片URL
        """
        try:
            total_users = len(users_data)
            total_messages = sum(u["text_count"] + u["image_count"] for u in users_data)

            ranked_users = []
            for idx, user in enumerate(users_data, start=1):
                user_total = user["text_count"] + user["image_count"]
                percent = round((user_total / total_messages) * 100, 1) if total_messages else 0
                ranked_users.append({
                    **user,
                    "rank": idx,
                    "total_count": user_total,
                    "percent": percent,
                })

            podium = {
                "first": ranked_users[0] if len(ranked_users) > 0 else None,
                "second": ranked_users[1] if len(ranked_users) > 1 else None,
                "third": ranked_users[2] if len(ranked_users) > 2 else None,
            }

            # 构造模板数据
            template_data = {
                "group_name": f"群{group_id[-4:]}",
                "total_users": total_users,
                "total_messages": total_messages,
                "users": ranked_users,
                "podium": podium,
                "date": date.strftime("%Y-%m-%d")
            }

            # 使用HTML模板生成图片
            if self.image_template:
                html_content = self.image_template
            else:
                # 默认模板（暗色排行榜）
                html_content = """
                <style>
                  * { box-sizing: border-box; }
                  body {
                    margin: 0;
                    font-family: "PingFang SC", "Microsoft YaHei", Arial, sans-serif;
                    background: radial-gradient(circle at 20% 0%, #222a64 0%, #0a0d1a 55%, #06080e 100%);
                    color: #eaf0ff;
                    width: 700px;
                    padding: 24px;
                  }
                  .card { background: rgba(11, 14, 24, 0.82); border: 1px solid rgba(90, 118, 255, 0.25); border-radius: 20px; padding: 20px; box-shadow: 0 15px 35px rgba(0,0,0,.35); }
                  .header { display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 18px; }
                  .title { font-size: 34px; font-weight: 700; margin: 0; color: #f3f6ff; }
                  .sub { margin-top: 8px; color: #aeb8d9; font-size: 14px; }
                  .total { text-align: right; }
                  .total-num { font-size: 48px; font-weight: 800; color: #ffffff; line-height: 1; }
                  .total-label { color: #99a4c8; font-size: 13px; margin-top: 6px; }
                  .podium { display: flex; justify-content: center; align-items: flex-end; gap: 16px; margin: 12px 0 18px; }
                  .podium-user { text-align: center; min-width: 140px; }
                  .avatar { width: 74px; height: 74px; border-radius: 50%; border: 3px solid #6f7ec0; object-fit: cover; }
                  .podium-first .avatar { width: 92px; height: 92px; border-color: #ffd34d; }
                  .rank-badge { display:inline-flex; align-items:center; justify-content:center; margin-top: 6px; width: 24px; height: 24px; border-radius: 50%; font-size: 13px; font-weight: 700; background: #2a3258; }
                  .podium-first .rank-badge { background: #f3ba2f; color: #251a00; }
                  .podium-name { margin-top: 6px; color: #f2f5ff; font-weight: 600; font-size: 15px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
                  .podium-count { font-size: 28px; font-weight: 800; color: #ffd34d; line-height: 1.1; }
                  .podium-meta { color: #8ea0cb; font-size: 12px; }
                  .table { margin-top: 10px; border-radius: 14px; overflow: hidden; border: 1px solid rgba(90,118,255,.22); }
                  .row { display:flex; align-items:center; padding: 10px 14px; background: rgba(18, 24, 41, 0.72); border-bottom: 1px solid rgba(255,255,255,0.05); }
                  .row:last-child { border-bottom: none; }
                  .col-rank { width: 34px; color:#d2dbff; font-weight:700; }
                  .col-user { display:flex; align-items:center; flex:1; min-width:0; }
                  .mini-avatar { width: 34px; height: 34px; border-radius: 50%; margin-right: 10px; object-fit: cover; border: 1px solid rgba(255,255,255,0.2); }
                  .name-wrap { min-width: 0; }
                  .name { color:#edf2ff; font-size:14px; font-weight:600; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
                  .tags { margin-top: 2px; font-size: 11px; color: #8ea0cb; }
                  .col-count { width: 72px; text-align:right; color:#f5f7ff; font-weight:700; }
                  .col-percent { width: 56px; text-align:right; color:#95a5cc; font-size:12px; }
                  .footer { margin-top: 10px; text-align:center; color:#6f7da9; font-size:12px; }
                </style>

                <div class="card">
                  <div class="header">
                    <div>
                      <h1 class="title">今日水群排行榜</h1>
                      <div class="sub">群聊：{{group_name}} · 日期：{{date}}</div>
                    </div>
                    <div class="total">
                      <div class="total-num">{{total_messages}}</div>
                      <div class="total-label">总消息数 / {{total_users}} 人</div>
                    </div>
                  </div>

                  <div class="podium">
                    {% if podium.second %}
                    <div class="podium-user">
                      <img class="avatar" src="{{podium.second.avatar}}" />
                      <div class="rank-badge">2</div>
                      <div class="podium-name">{{podium.second.user_name}}</div>
                      <div class="podium-count">{{podium.second.total_count}}</div>
                      <div class="podium-meta">文本 {{podium.second.text_count}} / 图片 {{podium.second.image_count}}</div>
                    </div>
                    {% endif %}

                    {% if podium.first %}
                    <div class="podium-user podium-first">
                      <img class="avatar" src="{{podium.first.avatar}}" />
                      <div class="rank-badge">1</div>
                      <div class="podium-name">{{podium.first.user_name}}</div>
                      <div class="podium-count">{{podium.first.total_count}}</div>
                      <div class="podium-meta">文本 {{podium.first.text_count}} / 图片 {{podium.first.image_count}}</div>
                    </div>
                    {% endif %}

                    {% if podium.third %}
                    <div class="podium-user">
                      <img class="avatar" src="{{podium.third.avatar}}" />
                      <div class="rank-badge">3</div>
                      <div class="podium-name">{{podium.third.user_name}}</div>
                      <div class="podium-count">{{podium.third.total_count}}</div>
                      <div class="podium-meta">文本 {{podium.third.text_count}} / 图片 {{podium.third.image_count}}</div>
                    </div>
                    {% endif %}
                  </div>

                  <div class="table">
                    {% for user in users %}
                    <div class="row">
                      <div class="col-rank">{{user.rank}}</div>
                      <div class="col-user">
                        <img class="mini-avatar" src="{{user.avatar}}" />
                        <div class="name-wrap">
                          <div class="name">{{user.user_name}}</div>
                          <div class="tags">文本 {{user.text_count}} · 图片 {{user.image_count}}</div>
                        </div>
                      </div>
                      <div class="col-count">{{user.total_count}}条</div>
                      <div class="col-percent">{{user.percent}}%</div>
                    </div>
                    {% endfor %}
                  </div>

                  <div class="footer">Powered by AstrBot Speak Rank</div>
                </div>
                """
                
            # 渲染HTML为图片
            image_url = await self.html_render(html_content, template_data)
            return image_url
            
        except Exception as e:
            logger.error(f"生成排行榜图片时出错: {e}")
            return ""

    async def clear_yesterday_data(self):
        """清理昨天的数据"""
        try:
            yesterday = (datetime.now() - timedelta(days=1)).date()
            async with aiosqlite.connect(self.db_path) as conn:
                cursor = await conn.cursor()
                
                await cursor.execute("DELETE FROM daily_stats WHERE date=?", (yesterday,))
                
                await conn.commit()
                
                logger.info(f"已清理 {yesterday} 的统计数据")
                
        except Exception as e:
            logger.error(f"清理数据时出错: {e}")

    async def terminate(self):
        """插件终止时的清理工作"""
        logger.info("SpeakRankPlugin terminated.")
