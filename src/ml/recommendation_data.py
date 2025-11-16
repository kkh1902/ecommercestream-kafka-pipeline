"""
추천 시스템 데이터 준비 모듈
ml_prepared_events에서 학습/테스트 데이터 로드 및 분할
"""

import sys
import os
from pathlib import Path
import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder

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


class RecommendationDataPreparator:
    """추천 시스템 데이터 준비"""

    def __init__(self):
        """초기화"""
        self.postgres_url = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        self.train_data = None
        self.test_data = None
        self.X_train = None
        self.X_test = None
        self.y_train = None
        self.y_test = None

    def load_raw_events_and_prepare(self, limit=None):
        """
        raw_events에서 데이터 로드 후 특성 준비

        Args:
            limit: 테스트용 샘플 크기 (None이면 전체)

        Returns:
            DataFrame
        """
        logger.info("raw_events 로드 및 준비 중...")

        query = """
            SELECT
                id, timestamp, visitorid, itemid, event,
                transactionid
            FROM raw_events
            WHERE
                timestamp > 0
                AND visitorid IS NOT NULL
                AND itemid IS NOT NULL
                AND event IS NOT NULL
            ORDER BY timestamp ASC
        """

        if limit:
            query += f" LIMIT {limit}"

        try:
            df = pd.read_sql(query, self.postgres_url)
            logger.info(f"✅ {len(df)}개 원본 레코드 로드 완료")

            # 데이터 정제 및 특성 생성
            df = self._prepare_raw_data(df)

            return df

        except Exception as e:
            logger.error(f"데이터 로드 실패: {e}", exc_info=True)
            raise

    def _prepare_raw_data(self, df):
        """
        raw_events 데이터 정제 및 특성 생성

        Args:
            df: raw_events DataFrame

        Returns:
            정제 및 특성이 추가된 DataFrame
        """
        logger.info("데이터 정제 및 특성 생성 중...")

        df = df.copy()

        # 1. 시간 관련 특성 추가
        df['event_timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
        df['event_date'] = df['event_timestamp'].dt.strftime('%Y-%m-%d')
        df['event_hour'] = df['event_timestamp'].dt.hour
        df['event_dow'] = df['event_timestamp'].dt.dayofweek + 1  # 1-7
        df['event_month'] = df['event_timestamp'].dt.month

        # 2. 구매 여부 (1 if transactionid is not null, 0 otherwise)
        df['is_buyer'] = (~df['transactionid'].isna()).astype(int)

        # 3. 사용자별 누적 이벤트 수
        df['user_event_count'] = df.groupby('visitorid').cumcount() + 1
        df['is_user_first_event'] = (df['user_event_count'] == 1).astype(int)

        # 4. 상품별 누적 이벤트 수
        df['item_event_count'] = df.groupby('itemid').cumcount() + 1
        df['is_item_first_event'] = (df['item_event_count'] == 1).astype(int)

        # 5. categoryid 추가 (item_properties에서 JOIN)
        # 먼저 item_properties 로드
        try:
            category_query = "SELECT itemid, categoryid FROM item_properties"
            categories = pd.read_sql(category_query, self.postgres_url)
            df = df.merge(categories, on='itemid', how='left')
            logger.info(f"✅ 카테고리 정보 JOIN 완료")
        except Exception as e:
            logger.warning(f"카테고리 JOIN 실패 (item_properties 테이블 없을 수 있음): {e}")
            df['categoryid'] = np.nan

        logger.info(f"✅ 데이터 정제 및 특성 생성 완료 ({len(df)} 레코드)")

        return df

    def split_train_test(self, df, train_ratio=0.8):
        """
        시간순 분할 (Train/Test)

        Args:
            df: 데이터프레임
            train_ratio: 훈련 데이터 비율 (기본 80%)

        Returns:
            train_df, test_df
        """
        logger.info(f"Train/Test 분할 중 ({train_ratio:.0%}/{1-train_ratio:.0%})...")

        split_idx = int(len(df) * train_ratio)
        train_df = df.iloc[:split_idx].copy()
        test_df = df.iloc[split_idx:].copy()

        logger.info(f"✅ Train: {len(train_df)}, Test: {len(test_df)}")

        return train_df, test_df

    def prepare_features(self, df):
        """
        특성 준비 및 엔지니어링

        Args:
            df: 데이터프레임 (raw_events로부터 준비된)

        Returns:
            특성만 추출한 DataFrame, 특성 컬럼 목록
        """
        logger.info("특성 준비 중...")

        df_features = df.copy()

        # 1. 범주형 변수 인코딩
        # event 컬럼을 숫자로 인코딩
        if 'event' in df_features.columns:
            if df_features['event'].dtype == 'object':
                event_encoder = LabelEncoder()
                df_features['event_encoded'] = event_encoder.fit_transform(df_features['event'])
            else:
                df_features['event_encoded'] = df_features['event']
        else:
            df_features['event_encoded'] = 0

        # 2. 선택할 특성 목록
        feature_columns = [
            'visitorid',
            'itemid',
            'categoryid',
            'event_encoded',
            'event_hour',
            'event_dow',
            'event_month',
            'user_event_count',
            'is_user_first_event',
            'item_event_count',
            'is_item_first_event',
            'is_buyer'  # 구매 여부 (타겟)
        ]

        # 3. NULL 값 처리 (categoryid는 item_properties에서 JOIN 실패 시 NaN이 될 수 있음)
        for col in feature_columns:
            if col in df_features.columns:
                df_features[col] = df_features[col].fillna(0)
            else:
                logger.warning(f"컬럼 '{col}'이(가) 데이터프레임에 없습니다. 0으로 채웁니다.")
                df_features[col] = 0

        logger.info(f"✅ {len(feature_columns)}개 특성 준비 완료")

        return df_features, feature_columns

    def prepare_training_data(self, df_train, df_test, feature_columns):
        """
        X, y 분리 및 준비

        Args:
            df_train: 훈련 데이터
            df_test: 테스트 데이터
            feature_columns: 특성 컬럼 목록

        Returns:
            X_train, X_test, y_train, y_test
        """
        logger.info("X, y 분리 중...")

        # 특성 X 준비
        X_train = df_train[feature_columns[:-1]].copy()  # is_buyer 제외
        X_test = df_test[feature_columns[:-1]].copy()

        # 타겟 y 준비 (다음 구매 여부 예측)
        # Train: 현재 is_buyer가 타겟
        y_train = df_train['is_buyer'].astype(int)
        y_test = df_test['is_buyer'].astype(int)

        logger.info(f"X_train: {X_train.shape}, y_train: {y_train.shape}")
        logger.info(f"X_test: {X_test.shape}, y_test: {y_test.shape}")
        logger.info(f"클래스 분포 (Train): {y_train.value_counts().to_dict()}")

        return X_train, X_test, y_train, y_test

    def prepare(self, limit=None, train_ratio=0.8):
        """
        전체 데이터 준비 파이프라인

        Args:
            limit: 샘플 크기 (테스트용)
            train_ratio: 훈련 데이터 비율

        Returns:
            X_train, X_test, y_train, y_test, feature_columns
        """
        logger.info("=" * 60)
        logger.info("추천 시스템 데이터 준비 시작")
        logger.info("=" * 60)

        try:
            # 1. 데이터 로드 및 준비
            df = self.load_raw_events_and_prepare(limit=limit)

            # 2. Train/Test 분할
            df_train, df_test = self.split_train_test(df, train_ratio=train_ratio)

            # 3. 특성 준비
            df_train_features, feature_columns = self.prepare_features(df_train)
            df_test_features, _ = self.prepare_features(df_test)

            # 4. X, y 분리
            X_train, X_test, y_train, y_test = self.prepare_training_data(
                df_train_features, df_test_features, feature_columns
            )

            self.X_train = X_train
            self.X_test = X_test
            self.y_train = y_train
            self.y_test = y_test

            logger.info("=" * 60)
            logger.info("✅ 데이터 준비 완료")
            logger.info("=" * 60)

            return X_train, X_test, y_train, y_test, feature_columns[:-1]  # is_buyer 제외

        except Exception as e:
            logger.error(f"데이터 준비 실패: {e}", exc_info=True)
            raise


if __name__ == "__main__":
    preparator = RecommendationDataPreparator()
    X_train, X_test, y_train, y_test, feature_cols = preparator.prepare(
        limit=10000  # 테스트용 샘플
    )
    print(f"준비 완료: X_train {X_train.shape}, X_test {X_test.shape}")
