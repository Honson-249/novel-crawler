#!/usr/bin/env python3
"""
浏览器管理模块 - 负责浏览器和上下表的 lifecycle 管理
"""
import asyncio
import random
from pathlib import Path
from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from loguru import logger

from .config import SpiderConfig


class BrowserManager:
    """浏览器管理器 - 负责浏览器和上下文的生命周期管理"""

    # 将 JavaScript 注入脚本提取为类属性，避免重复
    HIDE_AUTOMATION_SCRIPT = """
        // 隐藏 webdriver
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        });

        // 伪装 plugins - 更真实的插件列表
        Object.defineProperty(navigator, 'plugins', {
            get: () => [
                {
                    name: 'Chrome PDF Plugin',
                    filename: 'internal-pdf-viewer',
                    description: 'Portable Document Format',
                    length: 1
                },
                {
                    name: 'Chrome PDF Viewer',
                    filename: 'internal-pdf-viewer',
                    description: '',
                    length: 1
                },
                {
                    name: 'Native Client',
                    filename: 'internal-nacl-plugin',
                    description: '',
                    length: 1
                }
            ]
        });

        // 伪装 languages
        Object.defineProperty(navigator, 'languages', {
            get: () => ['zh-CN', 'zh', 'en-US', 'en']
        });

        // 伪装 platform
        Object.defineProperty(navigator, 'platform', {
            get: () => 'Win32'
        });

        // 伪装硬件信息
        Object.defineProperty(navigator, 'hardwareConcurrency', {
            get: () => 8
        });
        Object.defineProperty(navigator, 'deviceMemory', {
            get: () => 8
        });

        // 伪装 chrome 对象 - 更完整
        window.chrome = {
            app: {
                isInstalled: false,
                InstallState: {
                    DISABLED: 'disabled',
                    INSTALLED: 'installed',
                    NOT_INSTALLED: 'not_installed'
                },
                RunningState: {
                    CANNOT_RUN: 'cannot_run',
                    READY_TO_RUN: 'ready_to_run',
                    RUNNING: 'running'
                }
            },
            runtime: {
                OnInstalledReason: {
                    CHROME_UPDATE: 'chrome_update',
                    INSTALL: 'install',
                    SHARED_MODULE_UPDATE: 'shared_module_update',
                    UPDATE: 'update'
                },
                OnRestartRequiredReason: {
                    APP_UPDATE: 'app_update',
                    OS_UPDATE: 'os_update',
                    PERIODIC: 'periodic'
                },
                PlatformArch: {
                    ARM: 'arm',
                    ARM64: 'arm64',
                    X86_32: 'x86-32',
                    X86_64: 'x86-64'
                },
                PlatformNaclArch: {
                    ARM: 'arm',
                    ARM64: 'arm64',
                    X86_32: 'x86-32',
                    X86_64: 'x86-64'
                },
                PlatformOs: {
                    ANDROID: 'android',
                    CROS: 'cros',
                    LINUX: 'linux',
                    MAC: 'mac',
                    OPENBSD: 'openbsd',
                    WIN: 'win'
                },
                RequestUpdateCheckStatus: {
                    NO_UPDATE: 'no_update',
                    THROTTLED: 'throttled',
                    UPDATE_AVAILABLE: 'update_available'
                }
            },
            loadTimes: function() {
                return {
                    connectionType: '4g',
                    npnNegotiatedProtocol: 'h2',
                    wasAlternateProtocolAvailable: false,
                    wasFetchedViaSpdy: false,
                    wasNpnNegotiated: false
                };
            },
            csi: function() {
                return {
                    onloadT: Date.now(),
                    pageT: Date.now() - performance.timing.navigationStart
                };
            }
        };
        window.navigator.chrome = window.chrome;

        // 伪装 permissions
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = function(parameters) {
            if (parameters.name === 'notifications') {
                return Promise.resolve({ state: Notification.permission });
            }
            return originalQuery.call(window.navigator.permissions, parameters);
        };

        // 伪装 canvas 指纹
        const originalGetImageData = CanvasRenderingContext2D.prototype.getImageData;
        CanvasRenderingContext2D.prototype.getImageData = function(...args) {
            const data = originalGetImageData.apply(this, args);
            for (let i = 0; i < data.data.length; i += 100) {
                data.data[i] += Math.random() * 0.1 - 0.05;
            }
            return data;
        };

        // 伪装 webgl
        const getParameter = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function(param) {
            if (param === 37445) {
                return 'Intel Inc.';
            }
            if (param === 37446) {
                return 'Intel Iris OpenGL Engine';
            }
            return getParameter.call(this, param);
        };

        // 移除 navigator 上的自动化特征
        delete navigator.__proto__.webdriver;

        // 修复 toString
        const elementDescriptor = Object.getOwnPropertyDescriptor(HTMLElement.prototype, 'offsetHeight');
        Object.defineProperty(HTMLDivElement.prototype, 'offsetHeight', {
            ...elementDescriptor,
            get: function() {
                return elementDescriptor.get.call(this);
            }
        });
    """

    # 随机化版本的注入脚本（用于刷新指纹）
    RANDOMIZED_SCRIPT = """
        // 隐藏 webdriver
        Object.defineProperty(navigator, 'webdriver', {
            get: () => undefined
        });

        // 伪装 plugins
        Object.defineProperty(navigator, 'plugins', {
            get: () => [
                { name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer', description: 'Portable Document Format', length: 1 },
                { name: 'Chrome PDF Viewer', filename: 'internal-pdf-viewer', description: '', length: 1 },
                { name: 'Native Client', filename: 'internal-nacl-plugin', description: '', length: 1 }
            ]
        });

        // 伪装 languages
        Object.defineProperty(navigator, 'languages', {
            get: () => ['zh-CN', 'zh', 'en-US', 'en']
        });

        // 伪装 platform
        Object.defineProperty(navigator, 'platform', {
            get: () => 'Win32'
        });

        // 伪装硬件信息 - 随机值
        const cpuCores = [4, 6, 8, 12];
        const memory = [4, 8, 16, 32];
        Object.defineProperty(navigator, 'hardwareConcurrency', {
            get: () => cpuCores[Math.floor(Math.random() * cpuCores.length)]
        });
        Object.defineProperty(navigator, 'deviceMemory', {
            get: () => memory[Math.floor(Math.random() * memory.length)]
        });

        // 伪装 chrome 对象
        window.chrome = {
            app: { isInstalled: false, InstallState: { DISABLED: 'disabled', INSTALLED: 'installed', NOT_INSTALLED: 'not_installed' }, RunningState: { CANNOT_RUN: 'cannot_run', READY_TO_RUN: 'ready_to_run', RUNNING: 'running' } },
            runtime: { OnInstalledReason: { CHROME_UPDATE: 'chrome_update', INSTALL: 'install', SHARED_MODULE_UPDATE: 'shared_module_update', UPDATE: 'update' }, OnRestartRequiredReason: { APP_UPDATE: 'app_update', OS_UPDATE: 'os_update', PERIODIC: 'periodic' }, PlatformArch: { ARM: 'arm', ARM64: 'arm64', X86_32: 'x86-32', X86_64: 'x86-64' }, PlatformNaclArch: { ARM: 'arm', ARM64: 'arm64', X86_32: 'x86-32', X86_64: 'x86-64' }, PlatformOs: { ANDROID: 'android', CROS: 'cros', LINUX: 'linux', MAC: 'mac', OPENBSD: 'openbsd', WIN: 'win' }, RequestUpdateCheckStatus: { NO_UPDATE: 'no_update', THROTTLED: 'throttled', UPDATE_AVAILABLE: 'update_available' } },
            loadTimes: function() { return { connectionType: '4g', npnNegotiatedProtocol: 'h2', wasAlternateProtocolAvailable: false, wasFetchedViaSpdy: false, wasNpnNegotiated: false }; },
            csi: function() { return { onloadT: Date.now(), pageT: Date.now() - performance.timing.navigationStart }; }
        };
        window.navigator.chrome = window.chrome;

        // 伪装 permissions
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = function(parameters) {
            if (parameters.name === 'notifications') {
                return Promise.resolve({ state: Notification.permission });
            }
            return originalQuery.call(window.navigator.permissions, parameters);
        };

        // 随机 canvas 噪声
        const originalGetImageData = CanvasRenderingContext2D.prototype.getImageData;
        CanvasRenderingContext2D.prototype.getImageData = function(...args) {
            const data = originalGetImageData.apply(this, args);
            for (let i = 0; i < data.data.length; i += 100) {
                data.data[i] += Math.random() * 0.1 - 0.05;
            }
            return data;
        };

        // 随机 webgl 指纹
        const getParameter = WebGLRenderingContext.prototype.getParameter;
        const vendors = ['Intel Inc.', 'NVIDIA Corporation', 'AMD', 'Google Inc.'];
        const renderers = ['Intel Iris OpenGL Engine', 'NVIDIA GeForce GTX', 'AMD Radeon', 'ANGLE (Intel)'];
        WebGLRenderingContext.prototype.getParameter = function(param) {
            if (param === 37445) {
                return vendors[Math.floor(Math.random() * vendors.length)];
            }
            if (param === 37446) {
                return renderers[Math.floor(Math.random() * renderers.length)];
            }
            return getParameter.call(this, param);
        };

        // 移除自动化特征
        delete navigator.__proto__.webdriver;
    """

    def __init__(self, config: SpiderConfig, user_data_dir: Path = None):
        self.config = config
        self.user_data_dir = user_data_dir
        self.browser: Browser = None
        self.context: BrowserContext = None
        self.page: Page = None
        self._playwright = None

    async def init_browser(self) -> Page:
        """初始化浏览器 - 严格隐藏自动化特征"""
        try:
            self._playwright = await async_playwright().start()
        except asyncio.CancelledError:
            logger.warning("浏览器初始化被取消")
            raise
        except KeyboardInterrupt:
            logger.warning("浏览器初始化被用户中断")
            raise

        # 生成随机指纹
        ua = random.choice(self.config.user_agents)

        # 用户数据目录（可选，传入时自动创建）
        if self.user_data_dir:
            self.user_data_dir.mkdir(exist_ok=True)

        # 启动浏览器
        try:
            self.browser = await self._playwright.chromium.launch(
                headless=self.config.headless,
                args=self.config.browser_args,
            )
        except asyncio.CancelledError:
            logger.warning("浏览器启动被取消")
            await self.close()
            raise
        except KeyboardInterrupt:
            logger.warning("浏览器启动被用户中断")
            await self.close()
            raise

        # 创建浏览器上下文
        self.context = await self.browser.new_context(
            viewport={
                "width": random.choice(self.config.viewport_widths),
                "height": random.choice(self.config.viewport_heights)
            },
            user_agent=ua,
            locale="zh-CN",
            timezone_id="Asia/Shanghai",
            color_scheme="light",
            device_scale_factor=random.choice(self.config.device_scales),
            has_touch=False,
            is_mobile=False,
            extra_http_headers=self.config.extra_http_headers,
        )

        # 注入 JavaScript 隐藏自动化特征
        await self.context.add_init_script(self.HIDE_AUTOMATION_SCRIPT)

        # 创建初始页面
        self.page = await self.context.new_page()

        logger.info(f"浏览器初始化完成 (UA={ua[:50]}...)")
        return self.page

    async def refresh_fingerprint(self) -> Page:
        """刷新浏览器指纹 - 遇到封锁时切换指纹"""
        logger.info("刷新浏览器指纹...")

        # 生成新的随机指纹
        ua = random.choice(self.config.user_agents)
        viewport_width = random.choice(self.config.viewport_widths)
        viewport_height = random.choice(self.config.viewport_heights)
        device_scale = random.choice(self.config.device_scales)

        # 创建新的上下文
        new_context = await self.browser.new_context(
            viewport={"width": viewport_width, "height": viewport_height},
            user_agent=ua,
            locale="zh-CN",
            timezone_id="Asia/Shanghai",
            color_scheme="light",
            device_scale_factor=device_scale,
            has_touch=False,
            is_mobile=False,
            extra_http_headers=self.config.extra_http_headers,
        )

        # 注入 JavaScript 隐藏自动化特征（随机化版本）
        await new_context.add_init_script(self.RANDOMIZED_SCRIPT)

        # 关闭旧页面
        if self.page:
            await self.page.close()

        # 替换 context
        self.context = new_context

        # 创建新页面
        self.page = await self.context.new_page()

        logger.info(f"指纹刷新完成：UA={ua[:50]}..., 视口={viewport_width}x{viewport_height}")
        return self.page

    async def close(self):
        """关闭浏览器"""
        try:
            if self.page:
                await self.page.close()
            if self.context:
                await self.context.close()
            if self.browser:
                await self.browser.close()
                logger.info("浏览器已关闭")
            if self._playwright:
                await self._playwright.stop()
                logger.info("Playwright 已停止")
        except asyncio.CancelledError:
            logger.warning("关闭浏览器时被取消")
            raise
        except Exception as e:
            logger.error(f"关闭浏览器时出错：{e}")

    async def __aenter__(self):
        await self.init_browser()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
