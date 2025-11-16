"""
추천 시스템 평가 지표
Precision@K, Recall@K, NDCG@K 등
"""

import numpy as np
from src.utils.logger import get_logger

logger = get_logger(__name__)


class RecommendationMetrics:
    """추천 시스템 평가 지표"""

    @staticmethod
    def precision_at_k(recommendations, ground_truth, k=10):
        """
        Precision@K: 추천 상위 K개 중 실제 구매가 몇 개?

        Args:
            recommendations: 모델 추천 상품 리스트 (순서대로)
            ground_truth: 실제 구매 상품 리스트
            k: 상위 K개

        Returns:
            Precision@K 값
        """
        if len(recommendations) == 0:
            return 0.0

        # 상위 K개 추천
        top_k = recommendations[:k]

        # 실제 구매와 교집합
        hits = len([item for item in top_k if item in ground_truth])

        # Precision = 교집합 / K
        precision = hits / k

        return precision

    @staticmethod
    def recall_at_k(recommendations, ground_truth, k=10):
        """
        Recall@K: 사용자의 모든 구매 중 추천으로 몇 %를 포함?

        Args:
            recommendations: 모델 추천 상품 리스트 (순서대로)
            ground_truth: 실제 구매 상품 리스트
            k: 상위 K개

        Returns:
            Recall@K 값
        """
        if len(ground_truth) == 0:
            return 0.0

        # 상위 K개 추천
        top_k = recommendations[:k]

        # 실제 구매와 교집합
        hits = len([item for item in top_k if item in ground_truth])

        # Recall = 교집합 / 전체 구매 수
        recall = hits / len(ground_truth)

        return recall

    @staticmethod
    def ndcg_at_k(recommendations, ground_truth, k=10):
        """
        NDCG@K (Normalized Discounted Cumulative Gain)
        추천 순위가 얼마나 좋았는지 측정 (순위 고려)

        Args:
            recommendations: 모델 추천 상품 리스트 (순서대로)
            ground_truth: 실제 구매 상품 리스트
            k: 상위 K개

        Returns:
            NDCG@K 값 (0~1, 높을수록 좋음)
        """
        if len(ground_truth) == 0:
            return 0.0

        # DCG 계산 (실제 추천)
        dcg = 0.0
        for i, item in enumerate(recommendations[:k], 1):
            if item in ground_truth:
                # log2(i+1)로 감소 (순위가 뒤로 갈수록 낮은 가중치)
                dcg += 1.0 / np.log2(i + 1)

        # IDCG 계산 (이상적인 경우 = 상위부터 모두 맞는 경우)
        idcg = 0.0
        for i in range(min(len(ground_truth), k)):
            idcg += 1.0 / np.log2(i + 2)

        # NDCG = DCG / IDCG
        if idcg == 0:
            return 0.0

        ndcg = dcg / idcg

        return ndcg

    @staticmethod
    def map_at_k(recommendations, ground_truth, k=10):
        """
        MAP@K (Mean Average Precision)

        Args:
            recommendations: 모델 추천 상품 리스트 (순서대로)
            ground_truth: 실제 구매 상품 리스트
            k: 상위 K개

        Returns:
            MAP@K 값
        """
        if len(ground_truth) == 0:
            return 0.0

        # 상위 K개 추천에서 정확도 계산
        ap = 0.0
        hits = 0

        for i, item in enumerate(recommendations[:k], 1):
            if item in ground_truth:
                hits += 1
                # 현재까지의 정확도
                ap += hits / i

        # 평균
        map_score = ap / len(ground_truth)

        return map_score

    @staticmethod
    def evaluate_recommendations(all_recommendations, all_ground_truths, k=10):
        """
        전체 추천에 대해 평가 지표 계산

        Args:
            all_recommendations: 모든 추천 결과 리스트
                [{'visitorid': 1001, 'recommendations': [248, 512, ...]}, ...]
            all_ground_truths: 실제 구매 리스트
                [{'visitorid': 1001, 'ground_truth': [248, 456, ...]}, ...]
            k: 상위 K개

        Returns:
            평가 지표 딕셔너리
        """
        precisions = []
        recalls = []
        ndcgs = []
        maps = []

        for rec in all_recommendations:
            visitorid = rec['visitorid']
            recommendations = rec['recommendations']

            # 해당 사용자의 실제 구매 찾기
            ground_truth = None
            for gt in all_ground_truths:
                if gt['visitorid'] == visitorid:
                    ground_truth = gt['ground_truth']
                    break

            if ground_truth is None:
                continue

            # 각 지표 계산
            precision = RecommendationMetrics.precision_at_k(
                recommendations, ground_truth, k
            )
            recall = RecommendationMetrics.recall_at_k(
                recommendations, ground_truth, k
            )
            ndcg = RecommendationMetrics.ndcg_at_k(
                recommendations, ground_truth, k
            )
            map_score = RecommendationMetrics.map_at_k(
                recommendations, ground_truth, k
            )

            precisions.append(precision)
            recalls.append(recall)
            ndcgs.append(ndcg)
            maps.append(map_score)

        # 평균 계산
        result = {
            'precision@10': np.mean(precisions) if precisions else 0.0,
            'recall@10': np.mean(recalls) if recalls else 0.0,
            'ndcg@10': np.mean(ndcgs) if ndcgs else 0.0,
            'map@10': np.mean(maps) if maps else 0.0,
        }

        logger.info(f"Precision@{k}: {result[f'precision@{k}']:.4f}")
        logger.info(f"Recall@{k}:    {result[f'recall@{k}']:.4f}")
        logger.info(f"NDCG@{k}:      {result[f'ndcg@{k}']:.4f}")
        logger.info(f"MAP@{k}:       {result[f'map@{k}']:.4f}")

        return result


if __name__ == "__main__":
    # 테스트
    from src.utils.logger import get_logger

    # 예제 데이터
    recommendations = [
        {'visitorid': 1001, 'recommendations': [248, 512, 456, 123, 789]},
        {'visitorid': 1002, 'recommendations': [456, 248, 890]},
    ]

    ground_truths = [
        {'visitorid': 1001, 'ground_truth': [248, 456, 789]},
        {'visitorid': 1002, 'ground_truth': [456, 890, 512]},
    ]

    metrics = RecommendationMetrics.evaluate_recommendations(
        recommendations, ground_truths, k=10
    )
    print(f"\n평가 결과: {metrics}")
