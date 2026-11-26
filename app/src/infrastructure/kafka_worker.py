"""
Kafka-воркер: регистрация на платформе, чтение свечей,
расчёт SMA-сигнала, публикация рекомендаций и heartbeat.
"""

import json
import logging
import threading
import time
import uuid
from typing import Any

from confluent_kafka import Consumer, Producer

from app.src.core.config import AppConfig
from app.src.domain.entities import (
    CandleClosedPayload,
    HeartbeatPayload,
    MessageEnvelope,
    RegistrationRequestPayload,
    RegistrationResultPayload,
    RecommendationPayload,
)
from app.src.domain.sma import SmaState

logger = logging.getLogger("kafka_worker")


class KafkaWorker:
    def __init__(self, cfg: AppConfig) -> None:
        self.cfg = cfg
        self.instance_id = str(uuid.uuid4())
        self.started_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        # Состояние для /health
        self.status = "DOWN"
        self.error_details: str = "Not started"

        # Статистика
        self._processed_candles = 0
        self._published_recommendations = 0
        self._last_sequence_number: int | None = None

        # SMA-состояние по тикерам
        self._sma: dict[str, SmaState] = {}

        self._stop = threading.Event()
        self._thread = threading.Thread(
            target=self._run_loop, daemon=True, name="kafka-worker"
        )

    # ------------------------------------------------------------------
    # Публичный интерфейс
    # ------------------------------------------------------------------

    def start(self) -> None:
        self._thread.start()
        logger.info("KafkaWorker запущен (instance=%s)", self.instance_id)

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=10)
        logger.info("KafkaWorker остановлен")

    # ------------------------------------------------------------------
    # Построение клиентов
    # ------------------------------------------------------------------

    def _base_kafka_cfg(self) -> dict[str, Any]:
        return {
            "bootstrap.servers": self.cfg.bootstrap_servers,
            "security.protocol": self.cfg.kafka_security_protocol,
            "sasl.mechanism": self.cfg.kafka_sasl_mechanism,
            "sasl.username": self.cfg.kafka_username,
            "sasl.password": self.cfg.kafka_password,
            "enable.ssl.certificate.verification": True,
            "ssl.endpoint.identification.algorithm": "https",
        }

    def _make_producer(self) -> Producer:
        return Producer({**self._base_kafka_cfg(), "enable.idempotence": True})

    def _make_consumer(self, suffix: str) -> Consumer:
        return Consumer({
            **self._base_kafka_cfg(),
            "group.id": f"{self.cfg.kafka_group_id}.{suffix}",
            "auto.offset.reset": self.cfg.kafka_auto_offset_reset,
            "enable.auto.commit": False,
            "allow.auto.create.topics": False,
        })

    # ------------------------------------------------------------------
    # Вспомогательные методы
    # ------------------------------------------------------------------

    def _publish(self, producer: Producer, topic: str, envelope: MessageEnvelope) -> None:
        payload = json.dumps(envelope.to_dict(), ensure_ascii=False).encode("utf-8")
        errors: list[Exception] = []

        def _on_delivery(err, _msg):
            if err:
                errors.append(err)

        producer.produce(
            topic=topic,
            key=self.cfg.service_name.encode("utf-8"),
            value=payload,
            on_delivery=_on_delivery,
        )
        producer.flush(timeout=5.0)
        if errors:
            raise RuntimeError(f"Ошибка доставки в {topic}: {errors[0]}")

    def _send_heartbeat(self, producer: Producer) -> None:
        payload = HeartbeatPayload(
            secretKey=self.cfg.secret_key,
            accountId=self.cfg.account_id,
            groupId=self.cfg.group_id,
            strategyId=self.cfg.strategy_id,
            serviceName=self.cfg.service_name,
            instanceId=self.instance_id,
            status=self.status,
            startedAt=self.started_at,
            sentAt=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            processedCandles=self._processed_candles,
            publishedRecommendations=self._published_recommendations,
            lastProcessedSequenceNumber=self._last_sequence_number,
            metadata={"appVersion": self.cfg.app_version},
        )
        envelope = MessageEnvelope(
            eventType="account.service.heartbeat",
            producer=self.cfg.service_name,
            payload=payload.to_dict(),
        )
        self._publish(producer, self.cfg.topic_heartbeat, envelope)
        logger.debug("Heartbeat отправлен (candles=%d)", self._processed_candles)

    def _send_registration(self, producer: Producer) -> MessageEnvelope:
        payload = RegistrationRequestPayload(
            secretKey=self.cfg.secret_key,
            accountId=self.cfg.account_id,
            accountName=self.cfg.account_name,
            groupId=self.cfg.group_id,
            strategyId=self.cfg.strategy_id,
            strategyName="SMA Crossover Strategy",
            serviceName=self.cfg.service_name,
            description="Стратегия пересечения быстрой и медленной SMA",
        )
        envelope = MessageEnvelope(
            eventType="account.registration.request",
            producer=self.cfg.service_name,
            payload=payload.to_dict(),
        )
        self._publish(producer, self.cfg.topic_reg_request, envelope)
        logger.info(
            "Запрос регистрации отправлен (eventId=%s, accountId=%s)",
            envelope.eventId,
            self.cfg.account_id,
        )
        return envelope

    def _wait_for_assignment(self, consumer: Consumer, timeout: float = 30.0) -> None:
        deadline = time.monotonic() + timeout
        while not consumer.assignment() and not self._stop.is_set():
            if time.monotonic() > deadline:
                raise TimeoutError("Таймаут назначения партиций Kafka")
            consumer.poll(0.2)

    def _process_candle(
        self, event_id: str, candle: CandleClosedPayload, producer: Producer
    ) -> None:
        start_ns = time.perf_counter_ns()

        ticker = candle.instrument.ticker
        if ticker not in self._sma:
            self._sma[ticker] = SmaState(
                self.cfg.sma_fast_period, self.cfg.sma_slow_period
            )

        state = self._sma[ticker]
        state.push(float(candle.close))
        self._processed_candles += 1
        self._last_sequence_number = candle.sequenceNumber

        # Во время прогрева или нехватки данных — HOLD без расчёта
        if candle.isWarmup or not state.ready():
            action, confidence = "HOLD", 0.0
            reason = "warmup" if candle.isWarmup else "insufficient data"
        else:
            action, confidence, fast_sma, slow_sma = state.signal()
            reason = f"fast_sma={fast_sma:.4f} slow_sma={slow_sma:.4f}"

        processing_ms = (time.perf_counter_ns() - start_ns) // 1_000_000

        rec_payload = RecommendationPayload(
            recommendationId=str(uuid.uuid4()),
            sourceEventId=event_id,
            runId=candle.runId,
            secretKey=self.cfg.secret_key,
            strategyId=self.cfg.strategy_id,
            accountId=self.cfg.account_id,
            groupId=self.cfg.group_id,
            instrument=ticker,
            sequenceNumber=candle.sequenceNumber,
            signal=action,
            confidence=confidence,
            suggestedQuantity=1 if action != "HOLD" else 0,
            dataQuality="OK",
            reason=reason,
            processingTimeMs=processing_ms,
        )
        envelope = MessageEnvelope(
            eventType="strategy.recommendation.created",
            producer=self.cfg.service_name,
            payload=rec_payload.to_dict(),
            causationId=event_id,
        )
        self._publish(producer, self.cfg.topic_recommendation, envelope)
        self._published_recommendations += 1

        logger.info(
            "ticker=%s seq=%d action=%s confidence=%.1f ms=%d",
            ticker,
            candle.sequenceNumber,
            action,
            confidence,
            processing_ms,
        )

    # ------------------------------------------------------------------
    # Основной цикл
    # ------------------------------------------------------------------

    def _run_loop(self) -> None:
        while not self._stop.is_set():
            producer = None
            reg_consumer = None
            candle_consumer = None
            try:
                producer = self._make_producer()

                # --- Регистрация ---
                reg_consumer = self._make_consumer("reg")
                reg_consumer.subscribe([self.cfg.topic_reg_result])
                self._wait_for_assignment(reg_consumer)

                reg_envelope = self._send_registration(producer)

                registered = False
                deadline = time.monotonic() + 45
                while not self._stop.is_set():
                    if time.monotonic() > deadline:
                        raise TimeoutError("Нет ответа регистрации за 45 сек")
                    msg = reg_consumer.poll(0.3)
                    if msg is None:
                        continue
                    if msg.error():
                        raise RuntimeError(f"Ошибка чтения reg_result: {msg.error()}")

                    event = json.loads(msg.value())
                    is_mine = (
                        event.get("eventType") == "account.registration.result"
                        and (
                            event.get("causationId") == reg_envelope.eventId
                            or event.get("payload", {}).get("accountId") == self.cfg.account_id
                        )
                    )
                    if not is_mine:
                        continue

                    reg_consumer.commit(message=msg, asynchronous=False)
                    result = RegistrationResultPayload.from_dict(event.get("payload", {}))

                    if result.status == "REGISTERED":
                        logger.info("Регистрация подтверждена (reason=%s)", result.reason)
                        registered = True
                    else:
                        self.status = "DOWN"
                        self.error_details = f"Registration rejected: {result.reason}"
                        logger.error("Регистрация отклонена: %s", result.reason)
                    break

                if not registered:
                    self._stop.wait(30)
                    continue

                self.status = "UP"
                self.error_details = ""

                # --- Чтение свечей ---
                candle_consumer = self._make_consumer("candles")
                candle_consumer.subscribe([self.cfg.topic_candle_closed])
                self._wait_for_assignment(candle_consumer)
                logger.info("Подписка на свечи: %s", self.cfg.topic_candle_closed)

                next_heartbeat = 0.0
                while not self._stop.is_set():
                    if time.monotonic() >= next_heartbeat:
                        try:
                            self._send_heartbeat(producer)
                        except Exception as hb_err:
                            logger.warning("Heartbeat не отправлен: %s", hb_err)
                        next_heartbeat = time.monotonic() + 5.0

                    msg = candle_consumer.poll(0.3)
                    if msg is None:
                        continue
                    if msg.error():
                        raise RuntimeError(f"Ошибка чтения свечей: {msg.error()}")

                    event = json.loads(msg.value())
                    if event.get("eventType") == "market.candle.closed":
                        candle = CandleClosedPayload.from_dict(event["payload"])
                        self._process_candle(event["eventId"], candle, producer)

                    candle_consumer.commit(message=msg, asynchronous=False)

            except Exception as exc:
                self.status = "DOWN"
                self.error_details = type(exc).__name__
                logger.error(
                    "Сбой воркера [%s]: %s. Повтор через 5 сек.",
                    type(exc).__name__,
                    exc,
                )
            finally:
                if candle_consumer:
                    candle_consumer.close()
                if reg_consumer:
                    reg_consumer.close()
                if producer:
                    producer.flush(2)

            if not self._stop.is_set():
                self._stop.wait(5)
