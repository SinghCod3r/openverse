from datetime import datetime, timedelta, timezone
from unittest import mock

import pytest
from airflow.exceptions import AirflowFailException
from airflow.utils.trigger_rule import TriggerRule

from catalog.dags.elasticsearch_cluster.healthcheck_dag import (
    _clear_alarm_variable,
    _check_if_throttled,
    _set_alarm_variable,
    ALERT_THROTTLE_WINDOW,
    create_es_health_check_dag,
    ELASTICSEARCH_HEALTH_IN_ALARM_VAR,
)


DAG = create_es_health_check_dag("test")


def test_dag_structure():
    """
    Test the DAG structure and dependencies to ensure tasks are wired correctly.
    """
    assert DAG.dag_id == "test_elasticsearch_health_check"
    health_check = DAG.get_task("check_es_health")
    clear_alarm = DAG.get_task("clear_alarm_variable")
    check_throttle = DAG.get_task("check_if_throttled")
    notify_failure = DAG.get_task("notify_failure")
    set_alarm = DAG.get_task("set_alarm_variable")

    # Check success path
    assert health_check.downstream_task_ids == {"clear_alarm_variable", "check_if_throttled"}
    assert clear_alarm.upstream_task_ids == {"check_es_health"}
    assert clear_alarm.trigger_rule == TriggerRule.ALL_SUCCESS

    # Check failure path
    assert check_throttle.upstream_task_ids == {"check_es_health"}
    assert check_throttle.trigger_rule == TriggerRule.ALL_FAILED
    assert notify_failure.upstream_task_ids == {"check_if_throttled"}
    assert set_alarm.upstream_task_ids == {"notify_failure"}


@mock.patch("catalog.dags.elasticsearch_cluster.healthcheck_dag.ElasticsearchPythonHook")
def test_check_es_health_green(mock_es_hook):
    """Test that the health check passes with green status."""
    mock_es_hook.return_value.get_conn.return_value.cluster.health.return_value = {
        "status": "green"
    }
    health_check_task = DAG.get_task("check_es_health")
    health_check_task.execute(context={})


@mock.patch("catalog.dags.elasticsearch_cluster.healthcheck_dag.ElasticsearchPythonHook")
def test_check_es_health_red_fails(mock_es_hook):
    """Test that the health check fails as expected with red status."""
    mock_es_hook.return_value.get_conn.return_value.cluster.health.return_value = {
        "status": "red"
    }
    health_check_task = DAG.get_task("check_es_health")
    with pytest.raises(AirflowFailException):
        health_check_task.execute(context={})


@mock.patch("airflow.models.Variable.get")
def test_check_if_throttled_no_variable(mock_variable_get):
    """Test that throttling is off when no variable is set (first failure)."""
    mock_variable_get.return_value = None
    assert _check_if_throttled() is True
    mock_variable_get.assert_called_once_with(
        ELASTICSEARCH_HEALTH_IN_ALARM_VAR, default_var=None
    )


@mock.patch("airflow.models.Variable.get")
def test_check_if_throttled_within_window(mock_variable_get):
    """Test that throttling is on when the last alert was recent."""
    now = datetime.now(timezone.utc)
    last_alert_time = (now - timedelta(hours=1)).isoformat()
    mock_variable_get.return_value = last_alert_time
    assert _check_if_throttled() is False


@mock.patch("airflow.models.Variable.get")
def test_check_if_throttled_outside_window(mock_variable_get):
    """Test that throttling is off when the last alert was a long time ago."""
    old_alert_time = (
        datetime.now(timezone.utc) - ALERT_THROTTLE_WINDOW - timedelta(minutes=1)
    ).isoformat()
    mock_variable_get.return_value = old_alert_time
    assert _check_if_throttled() is True


@mock.patch("airflow.models.Variable.set")
def test_set_alarm_variable(mock_variable_set):
    """Test that the alarm variable is set correctly."""
    _set_alarm_variable()
    mock_variable_set.assert_called_once()
    # Basic check to ensure it's called with the variable name and an ISO timestamp string
    args, _ = mock_variable_set.call_args
    assert args[0] == ELASTICSEARCH_HEALTH_IN_ALARM_VAR
    assert isinstance(args[1], str)


@mock.patch("airflow.models.Variable.delete")
def test_clear_alarm_variable(mock_variable_delete):
    """Test that the alarm variable is deleted correctly."""
    _clear_alarm_variable()
    mock_variable_delete.assert_called_once_with(ELASTICSEARCH_HEALTH_IN_ALARM_VAR)
