# src/data_plane/web_mcp_server.py
import urllib.request
import re
from mcp.server.fastmcp import FastMCP

# 1. 初始化高级 FastMCP 服务端实例
mcp = FastMCP("data-plane-native-cleaner")

# 2. 注册工具：FastMCP 会自动将函数名转为工具名，自动将 type hints 和 docstring 编译成 JSON Schema
@mcp.tool()
def fetch_and_clean_web(url: str) -> str:
    """
    【数据平面专属工具】抓取任意网页并强力剥离 HTML 噪音，返回干净文本，专治 Agent 死循环。
    
    Args:
        url: 目标网页的完整 URL
    """
    try:
        # 纯 Python 原生网络请求
        req = urllib.request.Request(
            url, 
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        )
        with urllib.request.urlopen(req, timeout=10) as response:
            html = response.read().decode('utf-8', errors='ignore')
            
        # 强力防御性清洗逻辑
        html = re.sub(re.compile(r'<script[^>]*>.*?</script>', re.DOTALL), '', html)
        html = re.sub(re.compile(r'<style[^>]*>.*?</style>', re.DOTALL), '', html)
        html = re.sub(re.compile(r'<head[^>]*>.*?</head>', re.DOTALL), '', html)
        
        # 剥离所有 HTML 标签
        clean_text = re.sub(r'<[^>]+>', ' ', html)
        
        # 过滤单字噪音行，保留高价值正文
        lines = [line.strip() for line in clean_text.splitlines()]
        clean_lines = [line for line in lines if len(line) > 1]
        final_text = "\n".join(clean_lines)
        
        # 上游大模型安全截断保护
        if len(final_text) > 3000:
            final_text = final_text[:3000] + "\n\n...(余下内容因超出大模型单步窗口被数据平面拦截截断)..."
            
        return final_text
        
    except Exception as e:
        return f"[DataPlane 运行期异常]: 抓取失败，原因: {str(e)}"

if __name__ == "__main__":
    # 3. 启动标准输入输出（stdio）传输模式
    mcp.run(transport="stdio")