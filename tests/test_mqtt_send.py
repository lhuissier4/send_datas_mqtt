from pathlib import Path
from unittest.mock import MagicMock

import pytest

import mqtt_send


def test_resolve_jsonl_path_returns_existing_absolute_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    jsonl_file = tmp_path / "replay.jsonl"
    jsonl_file.write_text("")
    monkeypatch.setattr(mqtt_send, "JSONL_PATH", str(jsonl_file))

    assert mqtt_send.resolve_jsonl_path() == jsonl_file


def test_resolve_jsonl_path_raises_when_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(mqtt_send, "JSONL_PATH", str(tmp_path / "missing.jsonl"))

    with pytest.raises(FileNotFoundError):
        mqtt_send.resolve_jsonl_path()


def test_build_client_wires_callbacks_and_connects(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = MagicMock()
    mock_client_class = MagicMock(return_value=mock_client)
    monkeypatch.setattr(mqtt_send.mqtt, "Client", mock_client_class)
    monkeypatch.setattr(mqtt_send, "MQTT_USERNAME", None)

    client = mqtt_send.build_client()

    assert client is mock_client
    assert mock_client.on_connect is mqtt_send._on_connect
    assert mock_client.on_disconnect is mqtt_send._on_disconnect
    mock_client.connect_async.assert_called_once_with(
        mqtt_send.MQTT_HOST, mqtt_send.MQTT_PORT, mqtt_send.MQTT_KEEPALIVE
    )
    mock_client.username_pw_set.assert_not_called()


def test_build_client_sets_credentials_when_username_configured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_client = MagicMock()
    monkeypatch.setattr(mqtt_send.mqtt, "Client", MagicMock(return_value=mock_client))
    monkeypatch.setattr(mqtt_send, "MQTT_USERNAME", "alice")
    monkeypatch.setattr(mqtt_send, "MQTT_PASSWORD", "secret")

    mqtt_send.build_client()

    mock_client.username_pw_set.assert_called_once_with("alice", "secret")


def test_publish_line_at_qos_zero_counts_delivered_without_waiting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(mqtt_send, "MQTT_QOS", 0)
    client = MagicMock()
    info = MagicMock(rc=0)
    client.publish.return_value = info

    delivered, failed = mqtt_send.publish_line(
        client, [{"timestamp": "t", "id_machine": "MCH-001", "iot_temp": 10}]
    )

    assert (delivered, failed) == (1, 0)
    info.wait_for_publish.assert_not_called()


def test_publish_line_at_qos_one_waits_for_puback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mqtt_send, "MQTT_QOS", 1)
    client = MagicMock()
    info = MagicMock(rc=0)
    info.is_published.return_value = True
    client.publish.return_value = info

    delivered, failed = mqtt_send.publish_line(
        client, [{"timestamp": "t", "id_machine": "MCH-001", "iot_temp": 10}]
    )

    assert (delivered, failed) == (1, 0)
    info.wait_for_publish.assert_called_once()


def test_publish_line_counts_publish_queueing_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mqtt_send, "MQTT_QOS", 1)
    client = MagicMock()
    info = MagicMock(rc=1)
    client.publish.return_value = info

    delivered, failed = mqtt_send.publish_line(
        client, [{"timestamp": "t", "id_machine": "MCH-001", "iot_temp": 10}]
    )

    assert (delivered, failed) == (0, 1)


def test_publish_line_counts_missing_puback_as_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mqtt_send, "MQTT_QOS", 1)
    client = MagicMock()
    info = MagicMock(rc=0)
    info.is_published.return_value = False
    client.publish.return_value = info

    delivered, failed = mqtt_send.publish_line(
        client, [{"timestamp": "t", "id_machine": "MCH-001", "iot_temp": 10}]
    )

    assert (delivered, failed) == (0, 1)


def test_publish_line_skips_measures_without_a_sensor_field() -> None:
    client = MagicMock()

    delivered, failed = mqtt_send.publish_line(
        client, [{"timestamp": "t", "id_machine": "MCH-001"}]
    )

    assert (delivered, failed) == (0, 0)
    client.publish.assert_not_called()


def test_on_connect_sets_connected_event_on_success() -> None:
    mqtt_send._connected.clear()

    mqtt_send._on_connect(MagicMock(), None, {}, 0, None)

    assert mqtt_send._connected.is_set()


def test_on_connect_clears_connected_event_on_failure() -> None:
    mqtt_send._connected.set()

    mqtt_send._on_connect(MagicMock(), None, {}, 1, None)

    assert not mqtt_send._connected.is_set()


def test_on_disconnect_clears_connected_event() -> None:
    mqtt_send._connected.set()

    mqtt_send._on_disconnect(MagicMock(), None, None, 0, None)

    assert not mqtt_send._connected.is_set()
