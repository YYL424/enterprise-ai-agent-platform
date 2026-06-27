# src/data_plane/mcp_client_proxy.py
import asyncio
import sys
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from loguru import logger

class DataPlaneMCPClient:
    """数据平面 MCP 客户端代理，专门为控制平面和 main 注入外部感知工具"""
    
    def __init__(self):
        # 动态绑定咱们自己刚写好的那个纯 Python 服务端
        self.server_params = StdioServerParameters(
            command=sys.executable,
            args=["src/data_plane/web_mcp_server.py"]
        )

    async def _async_fetch(self, url: str) -> str:
        """底层真正的异步管道抓取"""
        try:
            async with stdio_client(self.server_params) as (read_stream, write_stream):
                async with ClientSession(read_stream, write_stream) as session:
                    await session.initialize()
                    mcp_response = await session.call_tool(
                        "fetch_and_clean_web", 
                        arguments={"url": url}
                    )
                    # 提取服务端洗净后的文本返回
                    if mcp_response and mcp_response.content:
                        return mcp_response.content[0].text
                    return "[DataPlane] MCP 返回内容为空"
        except Exception as e:
            logger.error(f"[MCP Proxy] 运行时物理通信崩溃: {str(e)}")
            return f"[DataPlane 管道异常]: {str(e)}"

    def fetch_and_clean_web(self, url: str) -> str:
        """对外暴露的纯同步高内聚接口（向下兼容成员 A 纯同步的 LangGraph 节点和工具字典）"""
        try:
            # 解决在部分已有 event_loop 的环境（如主进程）中嵌套调用的问题
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
        if loop.is_running():
            # 如果主线程的 loop 已经在跑（比如异步框架中），通过新线程或 future 强行拿到结果
            import nest_asyncio
            nest_asyncio.apply()
            return loop.run_until_complete(self._async_fetch(url))
        else:
            return loop.run_until_complete(self._async_fetch(url))