from typing import Dict, List, Tuple, Any
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, FloatType, BooleanType, IntegerType

def detect_schema_drift(expected_schema: Dict[str, str], actual_schema: Dict[str, str]) -> Dict[str, Any]:
    new_columns = {k: v for k, v in actual_schema.items() if k not in expected_schema}
    removed_columns = {k: v for k, v in expected_schema.items() if k not in actual_schema}
    type_changes = {k: (expected_schema[k], actual_schema[k]) for k in expected_schema if expected_schema[k]!= actual_schema[k]}
    drift_severity = 'NONE'
    if new_columns:
        if any("null" not in v for v in new_columns.values()):
            drift_severity = 'HIGH'
        else:
            drift_severity = 'LOW'
    if removed_columns:
        drift_severity = 'BREAKING'
    return {
        "new_columns": new_columns,
        "removed_columns": removed_columns,
        "type_changes": type_changes,
        "drift_severity": drift_severity
    }

def decide_action(drift_report: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    decisions = {}
    for column, dtype in drift_report["new_columns"].items():
        if dtype == "string":
            decisions[column] = {"action": "ADD_TO_SCHEMA", "reason": "New nullable string column", "risk_level": "LOW"}
        elif dtype in ["float", "double"]:
            decisions[column] = {"action": "FLAG_ANOMALY", "reason": "New float column", "risk_level": "HIGH"}
        elif dtype == "boolean":
            decisions[column] = {"action": "ADD_TO_SCHEMA", "reason": "New boolean column", "risk_level": "LOW"}
    for column, (old_type, new_type) in drift_report["type_changes"].items():
        if old_type!= new_type and "int" in (old_type, new_type):
            if old_type == "int" and new_type == "float":
                decisions[column] = {"action": "ADD_TO_SCHEMA", "reason": "Type widening", "risk_level": "LOW"}
            elif old_type == "float" and new_type == "int":
                decisions[column] = {"action": "FLAG_ANOMALY", "reason": "Type narrowing", "risk_level": "HIGH"}
    for column in drift_report["removed_columns"]:
        decisions[column] = {"action": "HALT", "reason": "Removed column", "risk_level": "BREAKING"}
    return decisions

def apply_schema_evolution(spark_df: DataFrame, decisions: Dict[str, Dict[str, Any]], updated_schema: Dict[str, str]) -> Tuple[DataFrame, List[str]]:
    migration_notes = []
    for column, decision in decisions.items():
        if decision["action"] == "DROP_SILENTLY":
            spark_df = spark_df.drop(column)
            migration_notes.append(f"Column '{column}' silently dropped.")
        elif decision["action"] == "ADD_TO_SCHEMA":
            migration_notes.append(f"Column '{column}' added to schema.")
        elif decision["action"] == "FLAG_ANOMALY":
            spark_df = spark_df.withColumn(f"{column}_anomaly", spark_df[column].isNull().cast("boolean"))
            migration_notes.append(f"Column '{column}' flagged for anomaly.")
        elif decision["action"] == "HALT":
            raise ValueError(f"Schema drift cannot be applied: Column '{column}' has been removed.")
    return spark_df, migration_notes

def handle_drift(expected_schema: Dict[str, str], actual_schema: Dict[str, str], spark_df: DataFrame = None) -> Dict[str, Any]:
    drift_report = detect_schema_drift(expected_schema, actual_schema)
    decisions = decide_action(drift_report)
    print("Schema Drift Report:")
    print(f"New Columns: {drift_report['new_columns']}")
    print(f"Removed Columns: {drift_report['removed_columns']}")
    print(f"Type Changes: {drift_report['type_changes']}")
    print(f"Drift Severity: {drift_report['drift_severity']}")
    if spark_df is not None:
        evolved_df, migration_notes = apply_schema_evolution(spark_df, decisions, actual_schema)
        return {"drift_report": drift_report, "decisions": decisions, "migration_notes": migration_notes, "evolved_df": evolved_df}
    return {"drift_report": drift_report, "decisions": decisions}
