# test_mcp_real.py
import asyncio
import sys
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

async def run_real_mcp_call():
    #  核心修复：抛弃 npx，直接用当前 Conda 环境的 Python 解释器拉起我们自己的 MCP 服务
    server_params = StdioServerParameters(
        command=sys.executable,  # 动态获取当前正在运行的 python 路径
        args=["src/data_plane/web_mcp_server.py"]  # 指向我们的纯 Python 服务端
    )
    
    print(" 正在通过本机的 stdio 管道建立与【数据平面原生MCP服务】的物理连接...")
    
    async with stdio_client(server_params) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            
            # 协议握手初始化
            await session.initialize()
            print(" 纯 Python 本地 MCP 协议握手成功！")
            
            # 动态探知工具
            available_tools = await session.list_tools()
            print("\n 该 MCP 服务向大模型开放了以下高内聚工具：")
            for tool in available_tools.tools:
                print(f"  - 工具名: {tool.name}")
                print(f"    描述: {tool.description}")
            
            # 真实模拟大模型发起 Function Call 调用
            print("\n 正在向本地 MCP 服务器发送执行请求：抓取并净化东南大学官网...")
            mcp_response = await session.call_tool(
                "fetch_and_clean_web", 
                arguments={"url": "https://news.seu.edu.cn/"}
            )
            
            print("\n==================  本地 MCP 返回的洗净数据 ==================")
            for content_item in mcp_response.content:
                print(content_item.text[:1500])  # 打印前 1500 字看清洗效果
            print("===============================================================")

if __name__ == "__main__":
    asyncio.run(run_real_mcp_call())