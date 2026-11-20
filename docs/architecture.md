# Архитектурные схемы сервиса (C4 Model)

## 1. Контекстная схема (System Context)

Схема отражает место сервиса торговой стратегии в окружении платформы, пользователей и внешних систем, а также направление и назначение каждого взаимодействия.

```mermaid
flowchart TB
    Student["Разработчик / Оператор\n(Студент)"]
    Teacher["Владелец платформы\n(Преподаватель)"]

    subgraph Infra ["Внешняя инфраструктура"]
        Service["Сервис торговой стратегии\n(sma-crossover-strategy)"]
        Kafka[("Брокер сообщений\nApache Kafka\nomsu.7bits.it:9094")]
        Platform["Преподавательская\nплатформа"]
        MarketProvider["Внешний поставщик\nрыночных данных"]
    end

    Student -- "Настраивает .env,\nзапускает Docker,\nпроверяет GET /health и логи" --> Service
    Teacher -- "Управляет прогонами (replay),\nтемами и портфелями через UI" --> Platform
    MarketProvider -- "Поставляет исторические\nи текущие свечи" --> Platform

    Service -- "1. Публикует запрос регистрации\n(account.registration.request)" --> Kafka
    Service -- "2. Публикует пульс раз в 5 сек\n(account.service.heartbeat)" --> Kafka
    Service -- "3. Публикует торговые сигналы\n(strategy.recommendation.created)" --> Kafka

    Kafka -- "4. Доставляет результат регистрации\n(account.registration.result)" --> Service
    Kafka -- "5. Доставляет закрытые свечи\n(market.candle.closed)" --> Service

    Platform <-->|"Читает запросы/рекомендации,\nпубликует свечи и расчёты портфелей"| Kafka
```

---

## 2. Крупноузловая компонентная схема (Component Diagram)

Внутренняя структура сервиса с разделением зон ответственности и основных потоков данных.

```mermaid
flowchart TB
    subgraph External ["Внешняя среда"]
        KafkaBroker[("Apache Kafka\nomsu.7bits.it:9094")]
        Admin["Оператор / Мониторинг"]
    end

    subgraph App ["Контейнер сервиса: sma-crossover-strategy (Python / FastAPI)"]

        subgraph API ["Слой API"]
            HttpHealth["FastAPI Router\n(app/src/api/router.py)\nЗона ответственности:\nHTTP GET /health\nHTTP POST /signals"]
        end

        subgraph Infra ["Слой Infrastructure"]
            KafkaWorker["KafkaWorker\n(app/src/infrastructure/kafka_worker.py)\nЗона ответственности:\nSASL_SSL подключение,\nрегистрация, heartbeat,\nчтение свечей,\nпубликация рекомендаций,\nручной commit offset"]
        end

        subgraph Core ["Слой Core"]
            Config["AppConfig\n(app/src/core/config.py)\nЗона ответственности:\nЗагрузка и валидация\nпеременных окружения"]
        end

        subgraph Domain ["Слой Domain"]
            SmaLogic["SmaState /\ncalculate_signal_from_candles\n(app/src/domain/sma.py)\nЗона ответственности:\nРасчёт сигнала BUY/SELL/HOLD\nпо ценам закрытия свечей"]
            Entities["MessageEnvelope /\nCandleClosedPayload /\nRecommendationPayload\n(app/src/domain/entities.py)\nЗона ответственности:\nСхемы сообщений платформы"]
        end

    end

    Admin -- "GET /health" --> HttpHealth
    Admin -- "POST /signals" --> HttpHealth

    HttpHealth -- "Читает статус подключения" --> KafkaWorker
    HttpHealth -- "Вызывает расчёт сигнала" --> SmaLogic

    KafkaBroker <-->|"SASL_SSL:\nсвечи, регистрация,\nрекомендации, heartbeat"| KafkaWorker

    KafkaWorker -- "Передаёт CandleClosedPayload" --> SmaLogic
    KafkaWorker -- "Формирует / разбирает конверты" --> Entities
    KafkaWorker -- "Читает конфигурацию" --> Config
    HttpHealth -- "Читает конфигурацию" --> Config

    SmaLogic -- "Возвращает action, confidence,\nfast_sma, slow_sma" --> KafkaWorker
```
