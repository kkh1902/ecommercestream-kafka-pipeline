"""
Kafka Load Testing Module

Kafka Producer와 Consumer의 성능을 측정하는 테스트 스위트입니다.
"""

__version__ = "1.0.0"
__author__ = "Data Engineering Team"

# 주요 모듈들
from pathlib import Path

BASE_DIR = Path(__file__).parent
RESULTS_DIR = BASE_DIR / "results"

# 결과 디렉토리 생성
RESULTS_DIR.mkdir(exist_ok=True)

__all__ = ["BASE_DIR", "RESULTS_DIR"]
