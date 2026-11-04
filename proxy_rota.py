import asyncio
import threading
import tkinter as tk
from tkinter import scrolledtext
import aiohttp
import os
import sys
from dotenv import load_dotenv
import botpy
from botpy.message import GroupMessage

# ==================== 加载配置 ====================
load_dotenv()

APP_ID = os.getenv("APP_ID")
APP_SECRET = os.getenv("APP_SECRET")
GROUP_OPENID = os.getenv("GROUP_OPENID")
ROTABOT_OPENID = os.getenv("ROTABOT_OPENID")

SAVE_PATH_B40 = os.getenv("SAVE_PATH_B40", "b40_latest.png")
# 支持 {cmd} 占位符，例如 img_{cmd}.png，会自动替换为 img__r_chart.png
SAVE_PATH_OTHER_TEMPLATE = os.getenv("SAVE_PATH_OTHER", "img_{cmd}.png")

if not all([APP_ID, APP_SECRET, GROUP_OPENID, ROTABOT_OPENID]):
    print("❌ 错误: 缺少必要的环境变量配置。请检查 .env 文件是否存在且填写完整。")
    sys.exit(1)
# ================================================


class ProxyRotaBot(botpy.Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # 核心机制：请求匹配列表、任务队列、状态锁
        self.pending_requests = []
        self.command_queue = asyncio.Queue()
        
        # 登录状态锁与取消事件
        self.is_logging_in = False
        self.login_cancel_event = asyncio.Event()
        self.is_running = True

    async def on_ready(self):
        print(f"✅ ProxyRota 已启动，机器人: {self.robot.name}")
        # 启动消费者（单线程队列处理）
        asyncio.create_task(self.queue_worker())
        # 启动定时任务 (默认 300 秒，可自行修改)
        asyncio.create_task(self.scheduled_loop(interval=300))

    # ================= 0. GROUP_MESSAGE_CREATE兼容 =================
    async def on_group_message_create(self, message: GroupMessage):
        """处理全量群消息事件 (GROUP_MESSAGE_CREATE)"""
        # 直接复用 on_group_at_message_create 的逻辑
        await self.on_group_at_message_create(message)
    
    # ================= 1. 消息接收与匹配 =================
    async def on_group_at_message_create(self, message: GroupMessage):
        print(f"🔥🔥🔥 收到群消息！content='{message.content}', author={message.author.member_openid}")
        # ================= 自助获取 ID 功能（双端输出） =================
        if message.content and "/sync_id" in (message.content or ""):
            group_id = message.group_openid
            # rotabot_id = "未找到RotaBOT，请确保你同时@了它"
            
            # 遍历 mentions 数组，寻找被 @ 的机器人
            if message.mentions:
                for m in message.mentions:
                    for m in message.mentions:
                        print(f"昵称: {m.username}, OpenID: {m.member_openid}, 是机器人: {m.bot}")
                        # 暴力破解：只要名字不是你的机器人，就当做 RotaBOT 抓过来！
                        # 注意：用 self.robot.name 替代 self.robot.id 防止某些版本 id 为空
            print(f"------------------------------")
            
            # 组装输出文本
            result_text = (
                f"=== OpenID 获取成功 ===\n"
                f"GROUP_OPENID={group_id}\n"
                # f"ROTABOT_OPENID={rotabot_id}\n"
                f"======================"
            )
            
            # 1. 打印到终端 / Tkinter 日志
            print(f"\n{result_text}\n")
            
            # 2. 顺便也在群里回复一份，方便直接复制
            try:
                await message.reply(content=result_text)
            except Exception as e:
                print(f"⚠️ 群内回复失败: {e}")
            
            
            return  # 结束处理
        # ====================================================
        # 下面是你原来的代码...
        
        # 只处理 RotaBOT 发来的消息
        if message.author.member_openid != ROTABOT_OPENID:
            # ######## DEBUG START #########
            # # 临时调试：打印被@的所有成员
            # if message.mentions:
            #     print("--- 捕获到 Mentions ---")
            #     for user in message.mentions:
            #         print(f"昵称: {user.username}, OpenID: {user.member_openid}, 是机器人: {user.bot}")
            # ######## DEBUG  END  #########
            return

        text = message.content or ""
        
        # 登录状态特判：拦截登录相关回显
        if self.is_logging_in:
            if "绑定成功" in text or "登录超时" in text or "错误" in text:
                for req in self.pending_requests:
                    if req["name"] == "登录结果" and not req["future"].done():
                        req["future"].set_result(message)
                        break
            return  # 登录期间忽略其他无关消息

        # 正常状态下的消息匹配
        for req in self.pending_requests:
            if not req["future"].done() and req["check"](message):
                req["future"].set_result(message)
                break

    # ================= 2. 底层请求封装 =================
    async def send_and_wait(self, command: str, check_func, timeout: float, req_name: str):
        """发送指令并等待特定的回显（利用 Future 阻塞）"""
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        req = {"name": req_name, "check": check_func, "future": fut}
        self.pending_requests.append(req)

        try:
            await self.api.post_group_message(
                group_openid=GROUP_OPENID,
                msg_type=0,
                content=f"<@ {ROTABOT_OPENID}> {command}"
            )
            print(f"📤 [发送] {req_name} ({command})，等待回复...")
            return await asyncio.wait_for(fut, timeout=timeout)
        except asyncio.TimeoutError:
            print(f"❌ [超时] 等待 {req_name} 失败！")
            raise
        finally:
            if req in self.pending_requests:
                self.pending_requests.remove(req)

    # ================= 3. 业务任务 =================
    async def task_fetch_b40(self):
        """完整的 B40 拉取流程：更新数据 -> 生成图片 -> 本地保存"""
        try:
            await self.send_and_wait(
                command="/r update",
                check_func=lambda m: "Rating:" in (m.content or ""),
                timeout=15, req_name="更新数据"
            )
            msg = await self.send_and_wait(
                command="/b40",
                check_func=lambda m: len(m.attachments) > 0,
                timeout=15, req_name="生成B40"
            )
            if msg.attachments:
                await self.download_image(msg.attachments[0].url, SAVE_PATH_B40)
                print(f"🖼️ B40 图片已保存至 {SAVE_PATH_B40}")
        except asyncio.TimeoutError:
            pass

    async def task_login(self, server: str):
        """扫码登录流程（带状态锁与超时放弃机制）"""
        if self.is_logging_in:
            return

        self.is_logging_in = True
        self.login_cancel_event.clear()
        print(f"🔑 开始登录流程: {server} 服")

        loop = asyncio.get_running_loop()
        login_fut = loop.create_future()
        req = {"name": "登录结果", "check": lambda m: True, "future": login_fut}
        
        try:
            # 1. 发送登录指令等待二维码生成
            await self.send_and_wait(
                command=f"/login {server}",
                check_func=lambda m: "TapTap登录二维码已生成" in (m.content or ""),
                timeout=10, req_name="生成登录二维码"
            )
            print("📱 二维码已发至群聊，请扫码。等待结果...")

            # 2. 并发等待：扫码结果 / 手动取消 / 程序端兜底超时 (130秒)
            self.pending_requests.append(req)
            cancel_task = asyncio.create_task(self.login_cancel_event.wait())
            
            done, pending = await asyncio.wait(
                [login_fut, cancel_task], timeout=130, return_when=asyncio.FIRST_COMPLETED
            )

            if cancel_task in pending: cancel_task.cancel()

            if cancel_task in done:
                print("🚫 用户手动取消了登录。")
            elif login_fut in done:
                text = login_fut.result().content or ""
                if "绑定成功" in text:
                    print(f"✅ 登录成功！")
                else:
                    print(f"❌ 登录失败/超时。({text})")

        except asyncio.TimeoutError:
            print("❌ 获取二维码超时，登录终止。")
        finally:
            self.is_logging_in = False
            if req in self.pending_requests:
                self.pending_requests.remove(req)

    async def task_quick_login(self, code: str):
        """快捷登录流程（好友码 / 会话码）"""
        if self.is_logging_in:
            print("⚠️ [拦截] 当前正在扫码登录流程中，请先完成或放弃扫码，再尝试快捷登录。")
            return

        print(f"🔑 正在尝试快捷登录，凭证: {code}")
        try:
            msg = await self.send_and_wait(
                command=f"/login {code}",
                check_func=lambda m: "绑定成功" in (m.content or "") or "错误" in (m.content or "") or "失败" in (m.content or ""),
                timeout=10, req_name="快捷登录(好友码/会话码)"
            )
            text = msg.content or ""
            if "绑定成功" in text:
                print(f"✅ 快捷登录成功！回显: {text}")
            else:
                print(f"❌ 快捷登录失败！回显: {text}")
        except asyncio.TimeoutError:
            print("❌ 快捷登录超时：RotaBOT 未在 10 秒内返回结果。")

    # ================= 4. 队列与调度 =================
    async def queue_worker(self):
        """绝对单线程执行：从队列拿任务并执行，彻底避免并发冲突"""
        while self.is_running:
            try:
                task_func, args = await self.command_queue.get()
                await task_func(*args)
            except Exception as e:
                print(f"⚠️ [Worker] 任务异常: {e}")
            finally:
                self.command_queue.task_done()

    async def scheduled_loop(self, interval: int):
        """定时调度：非登录状态下，定期拉取 B40"""
        while self.is_running:
            if GROUP_OPENID and GROUP_OPENID != "你的群openid" and GROUP_OPENID != "0":
                if not self.is_logging_in:
                    self.command_queue.put_nowait((self.task_fetch_b40, ()))
            await asyncio.sleep(interval)

    # ================= 5. 指令解析与入库 =================
    async def submit_command(self, cmd: str):
        """解析 UI 输入的指令并加入队列，包含各类登录逻辑的精确分流"""
        cmd = cmd.strip()
        if not cmd: return

        # 核心拦截：/login 家族专属处理
        if cmd.startswith("/login"):
            parts = cmd.split()
            
            # 1. 有参数的情况
            if len(parts) > 1:
                raw_param = parts[1].lower()
                
                # 1.1 扫码登录白名单
                if raw_param in ["cn", "国服", "global", "国际服"]:
                    if self.is_logging_in:
                        print("⚠️ [拦截] 当前正在扫码登录中。")
                        return
                    server = "cn" if raw_param in ["cn", "国服"] else "global"
                    self.command_queue.put_nowait((self.task_login, (server,)))
                    print(f"📥 [拦截成功] 转交内部扫码登录，服务器: {server}")
                    return
                
                # 1.2 快捷登录 (好友码 / 会话码)
                else:
                    self.command_queue.put_nowait((self.task_quick_login, (parts[1],)))
                    return
            
            # 2. 无参数的情况 (只输入了 /login)，原样放行看帮助
            else:
                pass 

        # 登录锁保护：如果在登录中，拒绝其他指令
        if self.is_logging_in and not cmd.startswith("/login"):
            print(f"⚠️ [拦截] 扫码登录期间禁止执行其他指令 ({cmd})。")
            return

        # 普通指令与带参 /login 走原样放行通道
        async def send_raw():
            # 好友码等可能瞬间回复，普通指令如 /r me 会发图片
            msg = await self.send_and_wait(cmd, lambda m: True, timeout=15, req_name=f"手动指令 {cmd}")
            
            # 自动处理附带图片的下载
            if msg and getattr(msg, 'attachments', None):
                safe_cmd = cmd.replace("/", "_").replace(" ", "_").strip("_")
                base_filename = SAVE_PATH_OTHER_TEMPLATE.format(cmd=safe_cmd)
                
                for i, attachment in enumerate(msg.attachments):
                    if attachment.url:
                        # 如果是多图，自动加上序号
                        filename = base_filename if len(msg.attachments) == 1 else base_filename.replace(".png", f"_{i}.png")
                        await self.download_image(attachment.url, filename)
                        print(f"🖼️ 附加图片已保存: {filename}")

        self.command_queue.put_nowait((send_raw, ()))

    def abandon_login(self):
        """供 UI 调用的放弃扫码登录接口"""
        if self.is_logging_in:
            self.login_cancel_event.set()

    async def download_image(self, url, filename):
        """异步流式下载图片，内存安全"""
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status == 200:
                    with open(filename, "wb") as f:
                        async for chunk in resp.content.iter_chunked(8192):
                            f.write(chunk)


# ==================== Tkinter GUI ====================
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
bot_client = None

class App:
    def __init__(self, root):
        self.root = root
        self.root.title("ProxyRota 控制台")
        self.root.geometry("700x550")

        # 状态栏
        self.status_var = tk.StringVar(value="等待连接...")
        tk.Label(root, textvariable=self.status_var, fg="blue", font=("Arial", 12, "bold")).pack(pady=5)

        # 日志区
        self.log_area = scrolledtext.ScrolledText(root, height=15, state="disabled")
        self.log_area.pack(padx=10, pady=5, fill="both", expand=True)

        # 登录控制区
        frame_login = tk.LabelFrame(root, text="账号登录 (自动拦截指令)")
        frame_login.pack(padx=10, pady=5, fill="x")
        tk.Button(frame_login, text="登录 国服", command=lambda: self.trigger_login("cn")).pack(side="left", padx=10, pady=5)
        tk.Button(frame_login, text="登录 国际服", command=lambda: self.trigger_login("global")).pack(side="left", padx=10, pady=5)
        tk.Button(frame_login, text="放弃登录", fg="red", command=self.cancel_login).pack(side="right", padx=10, pady=5)

        # 指令控制区
        frame_cmd = tk.Frame(root)
        frame_cmd.pack(padx=10, pady=5, fill="x")
        tk.Label(frame_cmd, text="指令:").pack(side="left")
        self.cmd_entry = tk.Entry(frame_cmd)
        self.cmd_entry.pack(side="left", fill="x", expand=True, padx=5)
        self.cmd_entry.bind("<Return>", lambda e: self.send_command()) # 支持回车发送
        tk.Button(frame_cmd, text="发送", command=self.send_command).pack(side="left")

        # 启动状态更新定时器
        self.update_status()

    def log(self, msg):
        self.log_area.config(state="normal")
        self.log_area.insert("end", msg + "\n")
        self.log_area.see("end")
        self.log_area.config(state="disabled")

    def trigger_login(self, server):
        if bot_client:
            asyncio.run_coroutine_threadsafe(bot_client.submit_command(f"/login {server}"), loop)

    def cancel_login(self):
        if bot_client:
            bot_client.abandon_login()
            self.log("⏹ 已发出取消登录信号...")

    def send_command(self):
        cmd = self.cmd_entry.get()
        if bot_client:
            asyncio.run_coroutine_threadsafe(bot_client.submit_command(cmd), loop)
            self.cmd_entry.delete(0, "end")

    def update_status(self):
        if bot_client:
            if bot_client.is_logging_in:
                self.status_var.set("🟡 正在等待扫码登录中...")
            else:
                self.status_var.set("🟢 空闲就绪 (定时任务运行中)")
        self.root.after(500, self.update_status)

def start_bot():
    global bot_client
    intents = botpy.Intents(public_messages=True)
    bot_client = ProxyRotaBot(intents=intents)

    async def _run():
        # 将 print 重定向到 GUI 日志
        def custom_print(*args, **kwargs):
            msg = " ".join(map(str, args))
            app.log(msg)
        print = custom_print
        
        await bot_client.start(appid=APP_ID, secret=APP_SECRET)

    asyncio.run_coroutine_threadsafe(_run(), loop)

if __name__ == "__main__":
    # 启动后台 asyncio 事件循环
    threading.Thread(target=loop.run_forever, daemon=True).start()
    
    # 启动 Tkinter 主线程
    root = tk.Tk()
    app = App(root)
    
    # 延迟 500ms 启动机器人服务
    root.after(500, start_bot)
    
    root.mainloop()