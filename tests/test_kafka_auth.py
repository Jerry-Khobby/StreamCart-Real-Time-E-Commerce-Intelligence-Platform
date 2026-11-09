"""Kafka client logins come from the environment, in both client libraries' formats."""

from producer.config import kafka_auth, spark_kafka_auth


def test_without_a_user_clients_connect_in_plaintext(monkeypatch):
    monkeypatch.delenv("KAFKA_SASL_USERNAME", raising=False)
    assert kafka_auth() == {} and spark_kafka_auth() == {}


def test_each_service_logs_in_as_its_own_user(monkeypatch):
    monkeypatch.setenv("KAFKA_SASL_USERNAME", "streamcart_sink")
    monkeypatch.setenv("KAFKA_SASL_PASSWORD", "s3cret")
    assert kafka_auth() == {"security.protocol": "SASL_PLAINTEXT", "sasl.mechanism": "PLAIN",
                            "sasl.username": "streamcart_sink", "sasl.password": "s3cret"}
    jaas = spark_kafka_auth()["kafka.sasl.jaas.config"]
    assert 'username="streamcart_sink"' in jaas and 'password="s3cret"' in jaas and jaas.endswith(";")
