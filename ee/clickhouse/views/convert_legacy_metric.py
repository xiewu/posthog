def convert_legacy_metric(legacy_metrics):
    """
    Converts the old JSON structure to the new format.

    Transformation rules:
    1. ExperimentFunnelsQuery -> ExperimentMetric with metric_type "funnel"
       - Remove name fields from series items (except when needed)
    2. ExperimentTrendsQuery -> ExperimentMetric with metric_type "mean"
       - Remove math_property_type
       - Remove name if there's no math field
    """
    new_structure = []

    for legacy_metric in legacy_metrics:
        if legacy_metric["kind"] == "ExperimentFunnelsQuery":
            # Extract and simplify series
            series = []
            for step in legacy_metric["funnels_query"]["series"]:
                step_copy = {}
                for key, value in step.items():
                    if key != "name":  # Skip the name field
                        step_copy[key] = value
                series.append(step_copy)

            new_metric = {"kind": "ExperimentMetric", "series": series, "metric_type": "funnel"}
            if name := legacy_metric.get("name"):
                new_metric["name"] = name

            new_structure.append(new_metric)

        elif legacy_metric["kind"] == "ExperimentTrendsQuery":
            source = legacy_metric["count_query"]["series"][0].copy()

            # Remove math_property_type if it exists
            if "math_property_type" in source:
                del source["math_property_type"]

            # Remove name if there's no math field
            if "math" not in source and "name" in source:
                del source["name"]

            new_metric = {"kind": "ExperimentMetric", "source": source, "metric_type": "mean"}

            if name := legacy_metric.get("name"):
                new_metric["name"] = name

            new_structure.append(new_metric)

    return new_structure
