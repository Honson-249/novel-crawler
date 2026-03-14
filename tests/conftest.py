"""
Pytest 配置文件
"""
import sys
from pathlib import Path

# 添加项目根目录到路径
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))


def pytest_configure(config):
    """Pytest 配置"""
    # 设置测试标记
    config.addinivalue_line(
        "markers", "unit: 单元测试标记"
    )
    config.addinivalue_line(
        "markers", "integration: 集成测试标记"
    )
    config.addinivalue_line(
        "markers", "e2e: 端到端测试标记"
    )
    config.addinivalue_line(
        "markers", "slow: 慢速测试标记"
    )
