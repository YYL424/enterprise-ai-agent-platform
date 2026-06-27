# src/data_plane/engine.py
import json
import os
from typing import Any, Dict
from pydantic import create_model, BaseModel
from src.common.interfaces.data_plane_api import ISchemaEngine
from src.common.interfaces.types import DomainSchema

class SchemaEngine(ISchemaEngine):
    """Schema解析引擎实现（成员B独占）"""
    
    def __init__(self):
        # 内存运行期模型缓存池，用于实现免重启热注入
        self._compiled_models: Dict[str, type[BaseModel]] = {}

    def load_schema(self, domain_name: str) -> DomainSchema:
        """从 contracts/domains/ 动态加载并基于 Pydantic v2 编译 Schema"""
        path = f"contracts/domains/{domain_name}.json"
        if not os.path.exists(path):
            raise FileNotFoundError(f"Domain contract file not found: {path}")
            
        with open(path, "r", encoding="utf-8") as f:
            raw_structure = json.load(f)
            
        # Pydantic v2 动态字段编译映射
        fields: Dict[str, Any] = {}
        type_mapping = {"str": str, "int": int, "float": float, "dict": dict}
        
        for k, v in raw_structure.items():
            if isinstance(v, str):
                fields[k] = (type_mapping.get(v, Any), ...)
            elif isinstance(v, dict):
                fields[k] = (dict, ...)
            else:
                fields[k] = (Any, ...)

        # 动态组装 Pydantic 模型
        compiled_model = create_model(f"Dynamic_{domain_name}", **fields)
        self._compiled_models[domain_name] = compiled_model
        
        #  核心修复：按 types.py 中 TypedDict 的标准返回纯字典，并向下兼容 domain_name
        return {
            "meta_config": raw_structure.get("meta_config", {}),
            "runtime_parameters": raw_structure.get("runtime_parameters", {}),
            "output_alignment": raw_structure.get("output_alignment", {}),
            "domain_name": domain_name  # 偷偷塞入 domain_name 供内部缓存流转使用
        } # type: ignore

    def validate_payload(self, payload: Dict[str, Any], schema: DomainSchema) -> Dict[str, Any]:
        """利用内存编译的强类型模型进行运行时验证"""
        #  核心修复：使用字典 get 方法提取 domain_name，解决对象属性调用导致的崩溃
        domain_name = schema.get("domain_name")
        if not domain_name:
             raise ValueError("[DataPlane] Schema 丢失 domain_name 标识！")

        model = self._compiled_models.get(domain_name)
        if not model:
            self.load_schema(domain_name)
            model = self._compiled_models[domain_name]
            
        # 触发 Pydantic 运行时严格校验
        instance = model(**payload)
        return instance.model_dump()

    def hot_inject(self, schema_path: str) -> None:
        """热注入新 Schema，无需重启系统直接刷新内存映射"""
        domain_name = os.path.basename(schema_path).replace(".json", "")
        self.load_schema(domain_name)