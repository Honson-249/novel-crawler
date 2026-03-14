"""
数据校验模块
"""
from typing import List, Dict, Tuple
from loguru import logger

from src.novel_crawler.config import VALIDATOR_CONFIG, ALERT_CONFIG


class DataValidator:
    """数据校验器"""
    
    def __init__(self):
        self.min_rows = VALIDATOR_CONFIG.get("min_rows", 10)
        self.max_empty_top1 = VALIDATOR_CONFIG.get("max_empty_top1", True)
        self.check_duplicate = VALIDATOR_CONFIG.get("check_duplicate", True)
        self.alert_enabled = ALERT_CONFIG.get("enabled", False)
        self.feishu_webhook = ALERT_CONFIG.get("feishu_webhook", "")
    
    def validate_fanqie(self, batch_date: str, records: List[Dict]) -> Tuple[bool, List[str]]:
        """
        校验番茄小说数据质量
        
        Args:
            batch_date: 批次日期
            records: 数据记录列表
        
        Returns:
            (是否通过校验，问题列表)
        """
        issues = []
        
        # 1. 检查行数
        if len(records) < self.min_rows:
            issue = f"番茄小说数据行数异常：{len(records)} < {self.min_rows}"
            issues.append(issue)
            logger.error(issue)
        
        # 2. 检查 TOP1 热度值
        if records and self.max_empty_top1:
            top1_heat = records[0].get("heat_value")
            if not top1_heat:
                issue = "TOP1 热度值为空"
                issues.append(issue)
                logger.warning(issue)
        
        # 3. 检查重复数据
        if self.check_duplicate and records:
            book_ids = [r.get("book_id") for r in records if r.get("book_id")]
            if len(book_ids) != len(set(book_ids)):
                issue = f"数据可能重复：{len(book_ids)} 条记录中有重复 book_id"
                issues.append(issue)
                logger.warning(issue)
        
        # 4. 发送告警
        if issues and self.alert_enabled and self.feishu_webhook:
            self.send_alert("fanqie", issues, batch_date)
        
        passed = len(issues) == 0
        if passed:
            logger.info(f"番茄小说数据质量校验通过：{len(records)} 条记录")
        else:
            logger.warning(f"番茄小说数据质量校验失败：{len(issues)} 个问题")
        
        return passed, issues
    
    def send_alert(self, source: str, issues: List[str], batch_date: str):
        """发送告警"""
        if not self.alert_enabled or not self.feishu_webhook:
            logger.warning("告警未启用或 webhook 未配置")
            return
        
        message = f"【{source}】数据质量告警\n批次：{batch_date}\n问题：\n" + "\n".join(issues)
        logger.warning(f"发送告警：{message}")


# 全局校验器实例
_validator = DataValidator()


def validate_fanqie(batch_date: str, records: List[Dict]) -> Tuple[bool, List[str]]:
    """校验番茄小说数据"""
    return _validator.validate_fanqie(batch_date, records)


def send_alert(source: str, issues: List[str], batch_date: str):
    """发送告警"""
    _validator.send_alert(source, issues, batch_date)
