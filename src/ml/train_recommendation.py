"""
추천 시스템 전체 파이프라인
1. 데이터 준비
2. 모델 학습
3. 평가
"""

import sys
import os
from pathlib import Path

# 상위 디렉토리 경로 추가
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.ml.recommendation_data import RecommendationDataPreparator
from src.ml.recommendation_model import RecommendationModelTrainer
from src.utils.logger import get_logger

logger = get_logger(__name__)


def main():
    """전체 파이프라인 실행"""
    logger.info("=" * 70)
    logger.info("추천 시스템 학습 파이프라인 시작")
    logger.info("=" * 70)

    try:
        # 1️⃣ 데이터 준비
        logger.info("\n[Step 1] 데이터 준비")
        logger.info("-" * 70)

        preparator = RecommendationDataPreparator()
        X_train, X_test, y_train, y_test, feature_cols = preparator.prepare(
            limit=None,  # 전체 데이터 사용
            train_ratio=0.8
        )

        # 2️⃣ 모델 학습
        logger.info("\n[Step 2] 모델 학습")
        logger.info("-" * 70)

        trainer = RecommendationModelTrainer()
        model = trainer.train(X_train, y_train)

        # 3️⃣ 모델 평가
        logger.info("\n[Step 3] 모델 평가")
        logger.info("-" * 70)

        metrics, y_pred, y_pred_proba = trainer.evaluate(X_test, y_test)

        # 4️⃣ 특성 중요도
        logger.info("\n[Step 4] 특성 중요도")
        logger.info("-" * 70)

        importance = trainer.get_feature_importance(top_n=10)

        # 5️⃣ 모델 저장
        logger.info("\n[Step 5] 모델 저장")
        logger.info("-" * 70)

        trainer.save_model()

        logger.info("\n" + "=" * 70)
        logger.info("✅ 추천 시스템 학습 완료!")
        logger.info("=" * 70)

        return metrics

    except Exception as e:
        logger.error(f"파이프라인 실행 실패: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
