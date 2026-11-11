# SMA Crossover Strategy Service

* **Автор:** Корзухин Михаил Алексеевич / МИБ-401-О-02
* **Назначение:** Микросервис стратегии SMA Crossover — формирует торговые сигналы на основе пересечения быстрой и медленной простых скользящих средних по ценам закрытия свечей.
* **Проектный документ:** [docs/project-description.md](docs/project-description.md)

## Требования к окружению

- Docker 20.10+ для запуска через контейнер.
- Python 3.11.x для локального запуска без Docker.
- Порт `8000` должен быть свободен.

## Переменные окружения

| Переменная | По умолчанию | Описание |
|---|---:|---|
| `SERVICE_NAME` | `sma-crossover-strategy` | Имя сервиса |
| `APP_VERSION` | `1.0.0` | Версия сервиса |
| `PORT` | `8000` | Порт HTTP-сервера |
| `SMA_FAST_PERIOD` | `10` | Период быстрой SMA |
| `SMA_SLOW_PERIOD` | `30` | Период медленной SMA; должен быть больше Fast |

Пример без секретных значений — в `.env.example`. Секреты в Git не хранятся.

## Сборка и запуск в Docker

Все команды выполняются из папки `trading-strategy-service`.

```bash
docker build -t sma-crossover-strategy:latest .
docker run --rm -d -p 8000:8000 --name sma-crossover-strategy sma-crossover-strategy:latest
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
docker run --rm -d -p 8000:8000 -e SMA_FAST_PERIOD=2 -e SMA_SLOW_PERIOD=3 --name sma-crossover-strategy sma-crossover-strategy:latest
```

Отправьте запрос (4 свечи, ожидаемый сигнал — `BUY`):

```bash
curl -X POST http://localhost:8000/signals \
  -H "Content-Type: application/json" \
  -d '{
    "symbol": "TEST",
    "candles": [
      {"timestamp": "2026-01-01T00:00:00Z", "open": 5, "high": 5, "low": 5, "close": 5, "volume": 1},
      {"timestamp": "2026-01-01T00:01:00Z", "open": 4, "high": 4, "low": 4, "close": 4, "volume": 1},
      {"timestamp": "2026-01-01T00:02:00Z", "open": 3, "high": 3, "low": 3, "close": 3, "volume": 1},
      {"timestamp": "2026-01-01T00:03:00Z", "open": 6, "high": 6, "low": 6, "close": 6, "volume": 1}
    ]
  }'
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

При стандартных периодах (Fast=10, Slow=30) передавайте не менее 31 свечи. Документация API: `http://localhost:8000/docs`.

## Тесты

```bash
docker run --rm sma-crossover-strategy:latest python -m unittest discover -s tests
```

Подробные сценарии тестирования — в [docs/project-description.md](docs/project-description.md).
