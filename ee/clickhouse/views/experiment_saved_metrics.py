import pydantic
from django.db.models.functions import Lower
from rest_framework import serializers, viewsets
from rest_framework.exceptions import ValidationError
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework import status

from ee.clickhouse.views.convert_legacy_metric import convert_legacy_metric

from posthog.api.routing import TeamAndOrgViewSetMixin
from posthog.api.shared import UserBasicSerializer
from posthog.api.tagged_item import TaggedItemSerializerMixin
from posthog.models.experiment import ExperimentSavedMetric, ExperimentToSavedMetric
from posthog.schema import (
    ExperimentFunnelMetric,
    ExperimentFunnelsQuery,
    ExperimentMeanMetric,
    ExperimentMetricType,
    ExperimentTrendsQuery,
)


class ExperimentToSavedMetricSerializer(serializers.ModelSerializer):
    query = serializers.JSONField(source="saved_metric.query", read_only=True)
    name = serializers.CharField(source="saved_metric.name", read_only=True)

    class Meta:
        model = ExperimentToSavedMetric
        fields = [
            "id",
            "experiment",
            "saved_metric",
            "metadata",
            "created_at",
            "query",
            "name",
        ]
        read_only_fields = [
            "id",
            "created_at",
        ]


class ExperimentSavedMetricSerializer(TaggedItemSerializerMixin, serializers.ModelSerializer):
    created_by = UserBasicSerializer(read_only=True)

    class Meta:
        model = ExperimentSavedMetric
        fields = [
            "id",
            "name",
            "description",
            "query",
            "created_by",
            "created_at",
            "updated_at",
            "tags",
        ]
        read_only_fields = [
            "id",
            "created_by",
            "created_at",
            "updated_at",
        ]

    def validate_query(self, value):
        if not value:
            raise ValidationError("Query is required to create a saved metric")

        metric_query = value

        if metric_query.get("kind") not in ["ExperimentMetric", "ExperimentTrendsQuery", "ExperimentFunnelsQuery"]:
            raise ValidationError(
                "Metric query kind must be 'ExperimentMetric', 'ExperimentTrendsQuery' or 'ExperimentFunnelsQuery'"
            )

        # pydantic models are used to validate the query
        try:
            if metric_query["kind"] == "ExperimentMetric":
                if "metric_type" not in metric_query:
                    raise ValidationError("ExperimentMetric requires a metric_type")
                if metric_query["metric_type"] == ExperimentMetricType.MEAN:
                    ExperimentMeanMetric(**metric_query)
                elif metric_query["metric_type"] == ExperimentMetricType.FUNNEL:
                    ExperimentFunnelMetric(**metric_query)
                else:
                    raise ValidationError("ExperimentMetric metric_type must be 'mean' or 'funnel'")
            elif metric_query["kind"] == "ExperimentTrendsQuery":
                ExperimentTrendsQuery(**metric_query)
            elif metric_query["kind"] == "ExperimentFunnelsQuery":
                ExperimentFunnelsQuery(**metric_query)
        except pydantic.ValidationError as e:
            raise ValidationError(str(e.errors())) from e

        return value

    def create(self, validated_data):
        request = self.context["request"]
        validated_data["created_by"] = request.user
        validated_data["team_id"] = self.context["team_id"]
        return super().create(validated_data)

    @action(detail=True, methods=["POST"])
    def migrate(self, request, *args, **kwargs):
        original_metric = self.get_object()

        # Check if already migrated using the queryset from parent class
        existing_migration = (
            super().get_queryset().filter(team_id=original_metric.team_id, query__has_key="legacy_id").first()
        )

        if existing_migration:
            return Response(
                {"error": "Metric already migrated", "migrated_metric_id": existing_migration.id},
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Convert legacy query and add legacy_id
        new_query = convert_legacy_metric(original_metric.query)
        new_query["legacy_id"] = original_metric.id

        # Prepare data for serializer
        data = {
            "name": original_metric.name,
            "description": original_metric.description,
            "query": new_query,
        }

        # Create new metric
        serializer = self.get_serializer(
            data=data,
            context={
                **self.get_serializer_context(),
                "team_id": original_metric.team_id,
                "request": request,
            },
        )
        serializer.is_valid(raise_exception=True)
        new_metric = serializer.save()

        return Response(self.get_serializer(new_metric).data, status=status.HTTP_201_CREATED)


class ExperimentSavedMetricViewSet(TeamAndOrgViewSetMixin, viewsets.ModelViewSet):
    scope_object = "experiment"
    queryset = ExperimentSavedMetric.objects.prefetch_related("created_by").order_by(Lower("name")).all()
    serializer_class = ExperimentSavedMetricSerializer
