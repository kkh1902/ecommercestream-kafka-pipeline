"""
추천 엔진
학습된 모델을 사용해 사용자에게 상품 추천 생성
"""

import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np
import joblib
from itertools import product

# 상위 디렉토리 경로 추가
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from config.settings import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD
)
from src.utils.logger import get_logger

logger = get_logger(__name__)


class RecommendationEngine:
    """학습된 모델로 추천 생성"""

    def __init__(self, model_path="src/ml/models/recommendation_xgboost.pkl"):
        """
        초기화

        Args:
            model_path: 모델 파일 경로
        """
        self.postgres_url = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        self.model = self.load_model(model_path)

    def load_model(self, model_path):
        """
        모델 로드

        Args:
            model_path: 모델 파일 경로

        Returns:
            로드된 모델
        """
        try:
            model = joblib.load(model_path)
            logger.info(f"✅ 모델 로드 완료: {model_path}")
            return model

        except Exception as e:
            logger.error(f"모델 로드 실패: {e}", exc_info=True)
            raise

    def get_user_viewed_items(self, visitorid):
        """
        사용자가 본 상품 목록 조회

        Args:
            visitorid: 사용자 ID

        Returns:
            본 상품 ID 리스트
        """
        query = f"""
            SELECT DISTINCT itemid
            FROM ml_prepared_events
            WHERE visitorid = {visitorid}
        """

        try:
            df = pd.read_sql(query, self.postgres_url)
            return set(df['itemid'].tolist())

        except Exception as e:
            logger.error(f"사용자 본 상품 조회 실패: {e}", exc_info=True)
            return set()

    def get_all_items(self):
        """
        모든 상품 목록 조회

        Returns:
            상품 ID 리스트
        """
        query = "SELECT DISTINCT itemid FROM ml_prepared_events ORDER BY itemid"

        try:
            df = pd.read_sql(query, self.postgres_url)
            return df['itemid'].tolist()

        except Exception as e:
            logger.error(f"상품 목록 조회 실패: {e}", exc_info=True)
            return []

    def get_user_features(self, visitorid):
        """
        사용자의 평균 특성 조회

        Args:
            visitorid: 사용자 ID

        Returns:
            사용자 특성 딕셔너리
        """
        query = f"""
            SELECT
                AVG(event_hour)::INT as avg_event_hour,
                AVG(event_dow)::INT as avg_event_dow,
                AVG(event_month)::INT as avg_event_month,
                AVG(user_event_count)::INT as avg_user_event_count,
                MAX(is_user_first_event)::INT as is_user_first_event,
                COUNT(DISTINCT categoryid) as num_categories_viewed
            FROM ml_prepared_events
            WHERE visitorid = {visitorid}
        """

        try:
            result = pd.read_sql(query, self.postgres_url)
            return result.iloc[0].to_dict() if len(result) > 0 else {}

        except Exception as e:
            logger.error(f"사용자 특성 조회 실패: {e}", exc_info=True)
            return {}

    def get_item_features(self, itemid):
        """
        상품의 평균 특성 조회

        Args:
            itemid: 상품 ID

        Returns:
            상품 특성 딕셔너리
        """
        query = f"""
            SELECT
                categoryid,
                AVG(item_event_count)::INT as avg_item_event_count,
                MAX(is_item_first_event)::INT as is_item_first_event
            FROM ml_prepared_events
            WHERE itemid = {itemid}
            GROUP BY categoryid
            LIMIT 1
        """

        try:
            result = pd.read_sql(query, self.postgres_url)
            return result.iloc[0].to_dict() if len(result) > 0 else {}

        except Exception as e:
            logger.error(f"상품 특성 조회 실패: {e}", exc_info=True)
            return {}

    def recommend(self, visitorid, top_n=10, exclude_viewed=True):
        """
        사용자에게 상품 추천

        Args:
            visitorid: 사용자 ID
            top_n: 추천 개수
            exclude_viewed: 본 상품 제외 여부

        Returns:
            추천 상품 리스트 (상품ID, 확률)
        """
        logger.info(f"사용자 {visitorid}에게 추천 생성 중...")

        try:
            # 1. 사용자가 본 상품 조회
            if exclude_viewed:
                viewed_items = self.get_user_viewed_items(visitorid)
            else:
                viewed_items = set()

            # 2. 모든 상품 조회
            all_items = self.get_all_items()
            candidate_items = [i for i in all_items if i not in viewed_items]

            if not candidate_items:
                logger.warning(f"사용자 {visitorid}의 추천 대상 상품이 없습니다")
                return []

            # 3. 사용자 특성
            user_features = self.get_user_features(visitorid)

            # 4. 각 상품에 대해 구매 확률 계산
            predictions = []

            for itemid in candidate_items:
                # 상품 특성
                item_features = self.get_item_features(itemid)

                # 특성 벡터 구성
                features = {
                    'visitorid': visitorid,
                    'itemid': itemid,
                    'categoryid': item_features.get('categoryid', 0),
                    'event_encoded': 0,  # 구매로 가정
                    'event_hour': user_features.get('avg_event_hour', 12),
                    'event_dow': user_features.get('avg_event_dow', 3),
                    'event_month': user_features.get('avg_event_month', 6),
                    'user_event_count': user_features.get('avg_user_event_count', 1),
                    'is_user_first_event': user_features.get('is_user_first_event', 0),
                    'item_event_count': item_features.get('avg_item_event_count', 1),
                    'is_item_first_event': item_features.get('is_item_first_event', 0),
                }

                # 모델 예측 (구매 확률)
                feature_values = np.array(list(features.values())).reshape(1, -1)
                proba = self.model.predict_proba(feature_values)[0, 1]

                predictions.append({
                    'itemid': itemid,
                    'probability': float(proba)
                })

            # 5. 확률 높은 순 정렬
            predictions.sort(key=lambda x: x['probability'], reverse=True)

            # 6. Top N 반환
            result = predictions[:top_n]

            logger.info(f"✅ {len(result)}개 상품 추천 완료")
            for i, rec in enumerate(result, 1):
                logger.info(f"  {i}. 상품 {rec['itemid']}: {rec['probability']:.4f}")

            return result

        except Exception as e:
            logger.error(f"추천 생성 실패: {e}", exc_info=True)
            return []


if __name__ == "__main__":
    # 테스트
    engine = RecommendationEngine()

    # 사용자 1001에게 추천
    recommendations = engine.recommend(visitorid=1001, top_n=10)
    print(f"\n추천 결과: {recommendations}")
