# tests/data_plane/test_data_contract.py
import pytest
import os
from pydantic import ValidationError
from src.data_plane.contract_compiler import DynamicContractCompiler

@pytest.fixture
def compiler():
    schema_path = os.path.join(os.path.dirname(__file__), "../src/data_plane/contracts/domain_schema.json")
    return DynamicContractCompiler(schema_path)

def test_data_plane_isolation():
    """验收指标：验证数据平面代码中绝无对控制平面的物理导入，确保单向解耦"""
    file_path = os.path.join(os.path.dirname(__file__), "../src/data_plane/contract_compiler.py")
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()
    assert "control_plane" not in content, "架构破产！数据面不应直接导入控制面代码！"

def test_pydantic_v2_dynamic_validation(compiler):
    """验证标准输入与延迟指标"""
    import time
    correct_input = '{"learning_rate": 0.001, "batch_size": 32}'
    
    t0 = time.perf_counter()
    result = compiler.validate_llm_json(correct_input)
    latency_ms = (time.perf_counter() - t0) * 1000
    
    assert result["batch_size"] == 32
    assert latency_ms < 2.0, f"Pydantic校验长尾延迟过高: {latency_ms}ms"

def test_malformed_llm_output_capture(compiler):
    """验证畸变JSON拦截拦截能力（为后面的自愈网关打桩）"""
    bad_input = '{"learning_rate": 0.5, "batch_size": 99}' # learning_rate超界，batch_size不在enum里
    with pytest.raises(ValidationError) as exc_info:
        compiler.validate_llm_json(bad_input)
    
    # 确认能够精准捕获字段级错误路径 (JSON Path)
    errors = exc_info.value.errors()
    error_fields = [err["loc"] for err in errors]
    assert ("learning_rate",) in error_fields
    assert ("batch_size",) in error_fields