from unittest.mock import MagicMock

import pytest

import mqtt_live_monitor


def test_measure_to_line_encodes_tags_and_fields() -> None:
    line = mqtt_live_monitor.measure_to_line(
        {"timestamp": "2026-06-01T10:00:00Z", "id_machine": "MCH-001", "iot_temp": 12.5}
    )

    assert line is not None
    assert line.startswith(
        'sensor_data,id_machine=MCH-001,sensor=iot_temp '
        'sensor_timestamp="2026-06-01T10:00:00Z",value=12.5 '
    )


def test_measure_to_line_returns_none_without_a_sensor_field() -> None:
    line = mqtt_live_monitor.measure_to_line(
        {"timestamp": "2026-06-01T10:00:00Z", "id_machine": "MCH-001"}
    )

    assert line is None


def test_build_mqtt_client_wires_callbacks_and_connects(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_client = MagicMock()
    monkeypatch.setattr(mqtt_live_monitor.mqtt, "Client", MagicMock(return_value=mock_client))
    monkeypatch.setattr(mqtt_live_monitor, "MQTT_USERNAME", None)

    client = mqtt_live_monitor.build_mqtt_client()

    assert client is mock_client
    assert mock_client.on_connect is mqtt_live_monitor._on_connect
    assert mock_client.on_disconnect is mqtt_live_monitor._on_disconnect
    mock_client.connect_async.assert_called_once_with(
        mqtt_live_monitor.MQTT_HOST,
        mqtt_live_monitor.MQTT_PORT,
        mqtt_live_monitor.MQTT_KEEPALIVE,
    )


def test_on_connect_subscribes_to_the_topic_prefix() -> None:
    client = MagicMock()

    mqtt_live_monitor._on_connect(client, None, {}, 0, None)

    client.subscribe.assert_called_once_with(
        f"{mqtt_live_monitor.MQTT_TOPIC_PREFIX}/#", qos=mqtt_live_monitor.MQTT_QOS
    )


def test_on_connect_does_not_subscribe_on_failure() -> None:
    client = MagicMock()

    mqtt_live_monitor._on_connect(client, None, {}, 1, None)

    client.subscribe.assert_not_called()


def test_build_on_message_buffers_a_decodable_measure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mqtt_live_monitor, "_buffer", [])
    on_message = mqtt_live_monitor.build_on_message()
    msg = MagicMock()
    msg.payload = (
        b'{"timestamp": "2026-06-01T10:00:00Z", "id_machine": "MCH-001", "iot_temp": 12.5}'
    )
    msg.topic = "usine/iot/MCH-001/iot_temp"

    on_message(MagicMock(), None, msg)

    assert len(mqtt_live_monitor._buffer) == 1


def test_build_on_message_ignores_undecodable_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mqtt_live_monitor, "_buffer", [])
    on_message = mqtt_live_monitor.build_on_message()
    msg = MagicMock()
    msg.payload = b"not json"
    msg.topic = "usine/iot/MCH-001/iot_temp"

    on_message(MagicMock(), None, msg)

    assert mqtt_live_monitor._buffer == []


def test_flush_buffer_posts_and_clears_the_batch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mqtt_live_monitor, "_buffer", ["line1", "line2"])
    session = MagicMock()
    session.post.return_value = MagicMock(status_code=204)

    mqtt_live_monitor.flush_buffer(session, "http://influx/api/v3/write_lp", "sensor_live")

    session.post.assert_called_once()
    assert session.post.call_args.args[0] == "http://influx/api/v3/write_lp"
    assert session.post.call_args.kwargs["data"] == b"line1\nline2"
    assert mqtt_live_monitor._buffer == []


def test_flush_buffer_does_nothing_when_buffer_is_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mqtt_live_monitor, "_buffer", [])
    session = MagicMock()

    mqtt_live_monitor.flush_buffer(session, "http://influx/api/v3/write_lp", "sensor_live")

    session.post.assert_not_called()
