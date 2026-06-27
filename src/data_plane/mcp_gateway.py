# src/data_plane/mcp_gateway.py
import asyncio
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

class MCPGateway:
    """
    数据平面 MCP 客户端网关 (成员B专属)
    用于免去手写 Function Call 胶水代码，直接吞噬社区现成的 MCP Server 生态
    """
    def __init__(self):
        self.sessions = {}

    async def connect_to_server(self, server_name: str, command: str, args: list):
        """动态连接一个现成的第三方 MCP 服务器 (例如社区提供的 brave-search 或者 git-server)"""
        server_params = StdioServerParameters(command=command, args=args)
        
        # 1. 建立 stdio 管道通信
        async with stdio_client(server_params) as (read_stream, write_stream):
            async with ClientSession(read_stream, write_stream) as session:
                # 2. 初始化握手
                await session.initialize()
                self.sessions[server_name] = session
                
                # 3. 动态拉取该厂商服务器上所有能提供的工具列表
                tools = await session.list_tools()
                print(f"[MCP 网关] 成功接入厂商 {server_name}! 动态获取到工具: {tools}")
                return tools

    async def call_vendor_tool(self, server_name: str, tool_name: str, arguments: dict):
        """当大模型发出请求时，数据平面通过 MCP 协议转发给对应的厂商服务"""
        session = self.sessions.get(server_name)
        if not session:
            raise Exception(f"厂商服务 {server_name} 未连接")
            
        # 遵循 MCP 标准协议标准请求厂商服务
        result = await session.call_tool(tool_name, arguments=arguments)
        return result.content