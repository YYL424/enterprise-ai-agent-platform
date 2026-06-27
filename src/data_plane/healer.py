# src/data_plane/healer.py
import re
import json
from typing import Any, Dict, List, Optional, Union
from loguru import logger
from src.common.interfaces.data_plane_api import IDataHealer

class DataHealer(IDataHealer):
    """三级数据自愈引擎实现 (企业级增强版)"""

    def heal_truncated_json(self, raw_text: str) -> Optional[str]:
        """第一级：Regex贪婪剥离 + 第二级：栈扫描补齐"""
        
        # --- 第一级：兼容 Dict {} 与 List [] 的 JSON 起始端捕获 ---
        json_match = re.search(r"([\{\[].*)", raw_text, re.DOTALL)
        if not json_match:
            return None
            
        # 强行抹除大模型经常附带的尾部 markdown 代码块标记
        truncated_json = re.sub(r"```.*$", "", json_match.group(1), flags=re.DOTALL).strip()

        # --- 第二级：O(N) 线性时间复杂度状态栈单次扫描算法 ---
        stack: List[str] = []
        in_string = False
        escape = False

        for char in truncated_json:
            if escape:
                escape = False
                continue
            if char == '\\':
                escape = True
                continue
            if char == '"':
                in_string = not in_string
                continue
            
            if not in_string:
                if char in ('{', '['):
                    stack.append(char)
                elif char in ('}', ']'):
                    if stack:
                        stack.pop()

        closure = ""
        if in_string:
            closure += '"'  # 补全未闭合的字符串引号

        # [核心优化点]：清理截断边缘的非法 JSON 游离态
        if not in_string:
            truncated_json = truncated_json.rstrip()
            if truncated_json.endswith(','):
                # 应对 {"a": 1, 截断 -> 砍掉逗号变成 {"a": 1
                truncated_json = truncated_json[:-1]
            elif truncated_json.endswith(':'):
                # 应对 {"a": 截断 -> 强行注入 null 变成 {"a": null
                truncated_json += 'null'

        # 逆向弹栈，生成闭合骨架
        while stack:
            top = stack.pop()
            if top == '{':
                closure += '}'
            elif top == '[':
                closure += ']'

        fixed_json = truncated_json + closure
        
        try:
            json.loads(fixed_json)  # 严格验证语法可用性
            return fixed_json
        except json.JSONDecodeError as e:
            # 引入日志：捕获未能自愈的极端 Case，便于安全面 (成员C) 审计溯源
            logger.debug(f"[DataHealer] JSONDecodeError bypass: {e}. Raw: {fixed_json}")
            return None

    def heal_missing_fields(self, partial_data: Dict[str, Any], error_path: List[Union[str, int]]) -> Dict[str, Any]:
        """
        第三级：基于 ValidationError 错误路径的靶向安全拦截自愈
        【深度优化】：全面兼容 Pydantic 报错路径中包含列表索引 (List[int]) 的深层降级处理
        """
        current = partial_data
        
        try:
            # 顺着报错路径深层遍历到故障叶子节点的父节点
            for i, path_node in enumerate(error_path[:-1]):
                next_node = error_path[i+1]
                
                if isinstance(current, dict):
                    if path_node not in current:
                        # 预判下一个节点的类型：如果是整数则注入空列表，否则注入空字典
                        current[path_node] = [] if isinstance(next_node, int) else {}
                    current = current[path_node]
                    
                elif isinstance(current, list) and isinstance(path_node, int):
                    # 处理列表中嵌套元素缺失的情况，安全扩容列表
                    while len(current) <= path_node:
                        current.append([] if isinstance(next_node, int) else {})
                    current = current[path_node]

            target_field = error_path[-1]
            
            # 强行注入安全的兜底脏数据占位符，防御并解决下层 C++ 物理沙箱崩溃
            if isinstance(current, dict):
                current[target_field] = "MUTED_FIELD_DEFAULT_BY_HEALER"
            elif isinstance(current, list) and isinstance(target_field, int):
                while len(current) <= target_field:
                    current.append("MUTED_FIELD_DEFAULT_BY_HEALER")
                    
            return partial_data
            
        except Exception as e:
            # 当遇到不可预测的结构崩溃时，由抛出异常转为记录日志，确保非阻塞（Non-blocking）
            logger.error(f"[DataHealer] Failed to targeted heal at path {error_path}: {e}")
            return partial_data