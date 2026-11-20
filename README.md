# SMA Crossover Strategy Service

* **Автор:** Корзухин Михаил Алексеевич / МИБ-401
* **Назначение:** Микросервис стратегии SMA Crossover — формирует торговые сигналы на основе пересечения быстрой и медленной простых скользящих средних по ценам закрытия свечей. Подключается к преподавательской платформе через Apache Kafka.
* **Проектный документ:** [docs/project-description.md](docs/project-description.md)
* **Архитектурные схемы:** [docs/architecture.md](docs/architecture.md)

## Требования к окружению

- Docker 20.10+ для запуска через контейнер.
- Python 3.11.x для локального запуска без Docker.
- Порт `8000` должен быть свободен.

## Переменные окружения

Переменные окружения задаются в файле `.env`. Пример конфигурации — в `.env.example`.

Файл `.env` содержит локальные настройки и не добавляется в репозиторий. Файл `.env.example` хранится в репозитории как шаблон.

| Переменная | По умолчанию | Описание |
|---|---|---|
| `PORT` | `8000` | Порт HTTP-сервера |
| `SMA_FAST_PERIOD` | `10` | Период быстрой SMA |
| `SMA_SLOW_PERIOD` | `30` | Период медленной SMA; должен быть больше Fast |
| `KAFKA_BOOTSTRAP_SERVERS` | `omsu.7bits.it:9094` | Адрес Kafka-брокера |
| `KAFKA_USERNAME` | — | Key|
| `KAFKA_PASSWORD` | — | Secret |
| `KAFKA_GROUP_ID` | — | ID группы потребителя Kafka; у каждого студента свой |
| `ACCOUNT_ID` | — | Уникальный идентификатор аккаунта |
| `ACCOUNT_NAME` | — | Имя в интерфейсе платформы |
| `GROUP_ID` | `МИБ-401` | ID учебной группы |
| `STRATEGY_ID` | — | Идентификатор стратегии |
| `KAFKA_SECURITY_PROTOCOL` | `SASL_SSL` | Протокол подключения, зависит от платформы |
| `KAFKA_SASL_MECHANISM` | `SCRAM-SHA-256` | Механизм аутентификации, зависит от платформы |
| `KAFKA_AUTO_OFFSET_RESET` | `earliest` | Куда ставится offset, если нет сохранённого |
| `KAFKA_ENABLE_AUTO_COMMIT` | `false` | Подтверждение вручную после успешной обработки |

## Структура проекта

```
├── .env.example
├── .gitignore
├── Dockerfile
├── requirements.txt
├── README.md
├── app/
│   ├── main.py                          # Точка входа
│   └── src/
│       ├── api/
│       │   ├── router.py                # Эндпоинты /health и /signals
│       │   └── schemas.py               # Pydantic-схемы запроса/ответа
│       ├── core/
│       │   └── config.py                # Конфигурация из переменных окружения
│       ├── domain/
│       │   ├── entities.py              # Схемы сообщений платформы
│       │   └── sma.py                   # Алгоритм SMA Crossover
│       └── infrastructure/
│           └── kafka_worker.py          # Подключение к Kafka
├── docs/
│   └── project-description.md
└── tests/
    └── test_strategy.py
```

## Сборка и запуск в Docker

1. Создать файл `.env` в корне проекта, скопировав `.env.example`:

```bash
cp .env.example .env
```

2. Заполнить необходимые переменные окружения (особенно `KAFKA_USERNAME`, `KAFKA_PASSWORD`, `ACCOUNT_SECRET_KEY`).

3. Собрать образ и запустить контейнер:

```bash
docker build -t sma-crossover-strategy:latest .
docker run --rm -d -p 8000:8000 --env-file .env --name sma-crossover-strategy sma-crossover-strategy:latest
```

Проверка состояния:

```bash
curl http://localhost:8000/health
```

Документация API: `http://localhost:8000/docs`

Остановка контейнера:

```bash
docker stop sma-crossover-strategy
```

## Локальный запуск (Windows PowerShell)

```powershell
python -m venv .venv
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## Пример запроса к API

Для проверки на коротком примере запустите контейнер с уменьшенными периодами:

```bash
docker run --rm -d -p 8000:8000 --env-file .env -e SMA_FAST_PERIOD=2 -e SMA_SLOW_PERIOD=3 --name sma-crossover-strategy sma-crossover-strategy:latest
```

Отправьте запрос (4 свечи, ожидаемый сигнал — `BUY`):

```json
{
  "symbol": "TEST",
  "candles": [
    {"timestamp": "2026-01-01T00:00:00Z", "open": 5, "high": 5, "low": 5, "close": 5, "volume": 1},
    {"timestamp": "2026-01-01T00:01:00Z", "open": 4, "high": 4, "low": 4, "close": 4, "volume": 1},
    {"timestamp": "2026-01-01T00:02:00Z", "open": 3, "high": 3, "low": 3, "close": 3, "volume": 1},
    {"timestamp": "2026-01-01T00:03:00Z", "open": 6, "high": 6, "low": 6, "close": 6, "volume": 1}
  ]
}
```

Ожидаемый ответ (HTTP 200):

```json
{
  "signal_id": "<uuid>",
  "symbol": "TEST",
  "action": "BUY",
  "timestamp": "2026-01-01T00:03:00Z",
  "confidence": 1.0,
  "fast_sma": 4.5,
  "slow_sma": 4.0
}
```

При стандартных периодах (Fast=10, Slow=30) передавайте не менее 31 свечи.

## Тесты

```bash
docker run --rm sma-crossover-strategy:latest python -m unittest discover -s tests
```
