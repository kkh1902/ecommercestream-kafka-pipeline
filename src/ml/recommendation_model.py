"""
추천 시스템 XGBoost 모델
구매 확률 예측 모델 학습 및 저장
"""

import sys
import os
from pathlib import Path
import numpy as np
import joblib
from sklearn.metrics import (
    precision_score, recall_score, f1_score,
    roc_auc_score, confusion_matrix, classification_report
)
import xgboost as xgb

# 상위 디렉토리 경로 추가
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.utils.logger import get_logger

logger = get_logger(__name__)


class RecommendationModelTrainer:
    """XGBoost 기반 추천 모델 학습"""

    def __init__(self, model_save_path=None):
        """
        초기화

        Args:
            model_save_path: 모델 저장 경로 (기본값: src/ml/models/)
        """
        if model_save_path is None:
            model_save_path = project_root / "src" / "ml" / "models"

        self.model_save_path = Path(model_save_path)
        self.model_save_path.mkdir(parents=True, exist_ok=True)

        self.model = None
        self.feature_columns = None

    def create_model(self, n_estimators=100, max_depth=6, learning_rate=0.1):
        """
        XGBoost 모델 생성

        Args:
            n_estimators: 부스팅 라운드 수
            max_depth: 트리 최대 깊이
            learning_rate: 학습률

        Returns:
            XGBClassifier 모델
        """
        logger.info("XGBoost 모델 생성 중...")

        model = xgb.XGBClassifier(
            n_estimators=n_estimators,
            max_depth=max_depth,
            learning_rate=learning_rate,
            scale_pos_weight=4,  # 클래스 불균형 처리 (구매:미구매 ~1:4)
            random_state=42,
            verbosity=1,
            n_jobs=-1  # 병렬 처리
        )

        logger.info(f"✅ 모델 생성 완료 (n_estimators={n_estimators}, max_depth={max_depth})")

        return model

    def train(self, X_train, y_train, X_val=None, y_val=None):
        """
        모델 학습

        Args:
            X_train: 훈련 특성
            y_train: 훈련 타겟
            X_val: 검증 특성 (선택사항)
            y_val: 검증 타겟 (선택사항)

        Returns:
            학습된 모델
        """
        logger.info("=" * 60)
        logger.info("XGBoost 모델 학습 시작")
        logger.info("=" * 60)

        try:
            # 모델 생성
            self.model = self.create_model()

            # Early stopping 세트 준비
            eval_set = None
            if X_val is not None and y_val is not None:
                eval_set = [(X_val, y_val)]
                eval_metric = ["logloss"]
            else:
                eval_metric = ["logloss"]

            # 학습
            self.model.fit(
                X_train, y_train,
                eval_set=eval_set,
                eval_metric=eval_metric,
                verbose=False
            )

            logger.info("✅ 모델 학습 완료")

            return self.model

        except Exception as e:
            logger.error(f"모델 학습 실패: {e}", exc_info=True)
            raise

    def evaluate(self, X_test, y_test):
        """
        모델 평가

        Args:
            X_test: 테스트 특성
            y_test: 테스트 타겟

        Returns:
            평가 지표 딕셔너리
        """
        logger.info("=" * 60)
        logger.info("모델 평가 중")
        logger.info("=" * 60)

        try:
            # 예측
            y_pred = self.model.predict(X_test)
            y_pred_proba = self.model.predict_proba(X_test)[:, 1]

            # 평가 지표
            metrics = {
                'accuracy': (y_pred == y_test).mean(),
                'precision': precision_score(y_test, y_pred, zero_division=0),
                'recall': recall_score(y_test, y_pred, zero_division=0),
                'f1': f1_score(y_test, y_pred, zero_division=0),
                'auc_roc': roc_auc_score(y_test, y_pred_proba)
            }

            # 혼동 행렬
            cm = confusion_matrix(y_test, y_pred)
            tn, fp, fn, tp = cm.ravel()
            metrics['tn'] = tn
            metrics['fp'] = fp
            metrics['fn'] = fn
            metrics['tp'] = tp

            # 로그 출력
            logger.info(f"Accuracy:  {metrics['accuracy']:.4f}")
            logger.info(f"Precision: {metrics['precision']:.4f}")
            logger.info(f"Recall:    {metrics['recall']:.4f}")
            logger.info(f"F1 Score:  {metrics['f1']:.4f}")
            logger.info(f"AUC-ROC:   {metrics['auc_roc']:.4f}")
            logger.info(f"\n혼동 행렬:")
            logger.info(f"  TN={tn}, FP={fp}")
            logger.info(f"  FN={fn}, TP={tp}")
            logger.info("=" * 60)

            return metrics, y_pred, y_pred_proba

        except Exception as e:
            logger.error(f"평가 실패: {e}", exc_info=True)
            raise

    def get_feature_importance(self, top_n=10):
        """
        특성 중요도 조회

        Args:
            top_n: 상위 N개 특성

        Returns:
            특성 중요도 (정렬됨)
        """
        if self.model is None:
            logger.warning("학습된 모델이 없습니다")
            return None

        importance = self.model.feature_importances_
        feature_names = self.model.get_booster().feature_names

        # 정렬
        indices = np.argsort(importance)[::-1][:top_n]

        logger.info(f"\n상위 {top_n}개 중요 특성:")
        for i, idx in enumerate(indices, 1):
            logger.info(f"  {i}. {feature_names[idx]}: {importance[idx]:.4f}")

        return dict(zip([feature_names[i] for i in indices], importance[indices]))

    def save_model(self, filename="recommendation_xgboost.pkl"):
        """
        모델 저장

        Args:
            filename: 저장 파일명
        """
        if self.model is None:
            logger.warning("저장할 모델이 없습니다")
            return

        save_path = self.model_save_path / filename

        try:
            joblib.dump(self.model, save_path)
            logger.info(f"✅ 모델 저장 완료: {save_path}")

        except Exception as e:
            logger.error(f"모델 저장 실패: {e}", exc_info=True)
            raise

    def load_model(self, filename="recommendation_xgboost.pkl"):
        """
        모델 로드

        Args:
            filename: 로드 파일명

        Returns:
            로드된 모델
        """
        load_path = self.model_save_path / filename

        try:
            self.model = joblib.load(load_path)
            logger.info(f"✅ 모델 로드 완료: {load_path}")
            return self.model

        except Exception as e:
            logger.error(f"모델 로드 실패: {e}", exc_info=True)
            raise


if __name__ == "__main__":
    # 테스트 예시
    from recommendation_data import RecommendationDataPreparator

    # 데이터 준비
    preparator = RecommendationDataPreparator()
    X_train, X_test, y_train, y_test, feature_cols = preparator.prepare(limit=10000)

    # 모델 학습
    trainer = RecommendationModelTrainer()
    model = trainer.train(X_train, y_train)

    # 평가
    metrics, y_pred, y_pred_proba = trainer.evaluate(X_test, y_test)

    # 특성 중요도
    trainer.get_feature_importance(top_n=10)

    # 저장
    trainer.save_model()
