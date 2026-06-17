# src/data_plane/contract_compiler.py
import json
from typing import Dict, Any, Literal
from pydantic import TypeAdapter, ValidationError, create_model, Field

class DynamicContractCompiler:
    """成员B核心组件：基于 Pydantic v2 元编程的跨场景数据契约编译器"""
    def __init__(self, schema_path: str):
        with open(schema_path, "r", encoding="utf-8") as f:
            self.raw_schema: Dict[str, Any] = json.load(f)
            
        # --- 核心黑科技：在内存中将 JSON Schema 动态翻译为 Pydantic 强类型模型 ---
        fields = {}
        properties = self.raw_schema.get("properties", {})
        required_fields = self.raw_schema.get("required", [])

        for field_name, attrs in properties.items():
            # 1. 解析基础类型
            json_type = attrs.get("type")
            if json_type == "number":
                py_type = float
            elif json_type == "integer":
                py_type = int
            else:
                py_type = Any

            # 2. 解析枚举类型，转化为 Python 的 Literal 强约束
            if "enum" in attrs:
                py_type = Literal[tuple(attrs["enum"])]

            # 3. 解析边界条件 (minimum -> ge, maximum -> le)
            field_kwargs = {}
            if "minimum" in attrs:
                field_kwargs["ge"] = attrs["minimum"]
            if "maximum" in attrs:
                field_kwargs["le"] = attrs["maximum"]

            # 4. 判断是否必填
            default_value = ... if field_name in required_fields else None

            # 组装动态字段
            fields[field_name] = (py_type, Field(default=default_value, **field_kwargs))

        # 5. 利用 create_model 在内存中瞬间生成一个对齐当前业务的 Pydantic 模型
        model_name = self.raw_schema.get("title", "DynamicContractModel")
        self.DynamicModel = create_model(model_name, **fields)
        
        # 6. 挂载至 TypeAdapter 激活 Rust 校验引擎
        self.adapter = TypeAdapter(self.DynamicModel)

    def validate_llm_json(self, incoming_json: str) -> Dict[str, Any]:
        """拦截并洗净大模型输出的结构化JSON字符串"""
        try:
            data = json.loads(incoming_json)
            # 投入 Rust 引擎进行强类型和边界校验
            purified_obj = self.adapter.validate_python(data)
            # 返回洗净后的标准字典
            return purified_obj.model_dump()
        except json.JSONDecodeError as je:
            raise ValueError(f"JSON 语法层畸变: {str(je)}")
        except ValidationError as ve:
            # 向上层抛出标准的契约越界错误，为后续自愈留出监控桩
            raise ve