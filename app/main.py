import json
import asyncio
import logging
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_SECRET_KEY = os.getenv("KAFKA_SECRET_KEY", "test-secret")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "sma-crossover-group")
STUDENT_ID = os.getenv("STUDENT_ID", "student-id")
STRATEGY_ID = os.getenv("STRATEGY_ID", "sma-crossover")


class RegistrationRequest(BaseModel):
    schemaVersion: int = 1
    secretKey: str
    studentId: str
    strategyId: str
    serviceId: str


class RegistrationResult(BaseModel):
    schemaVersion: int = 1
    isSuccessful: bool
    reason: str | None = None


kafka_status = "DOWN"
error_details = "Not connected"

async def kafka_worker():
    global kafka_status, error_details
    
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    consumer = AIOKafkaConsumer(
        "account.registration.result",
        "market.candle.closed",
        bootstrap_servers=KAFKA_BROKER,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset="latest"
    )

    try:
        await producer.start()
        await consumer.start()
        kafka_status = "UP"
        error_details = ""
        logger.info(f"Успешно подключились к брокеру {KAFKA_BROKER}")

        reg_msg = RegistrationRequest(
            secretKey=KAFKA_SECRET_KEY,
            studentId=STUDENT_ID,
            strategyId=STRATEGY_ID,
            serviceId=SERVICE_NAME
        ).model_dump()
        
        await producer.send_and_wait("account.registration.request", reg_msg)
        logger.info("Запрос на регистрацию отправлен.")

        async for msg in consumer:
            if msg.topic == "account.registration.result":
                result = RegistrationResult(**msg.value)
                if not result.isSuccessful:
                    logger.error(f"Отказ в регистрации. Причина: {result.reason}")
                    kafka_status = "DEGRADED"
                    error_details = f"Registration rejected: {result.reason}"
                else:
                    logger.info("Успешная регистрация на платформе. Ждем свечи...")
            
            elif msg.topic == "market.candle.closed":
                if kafka_status == "DEGRADED":
                    continue

                logger.info(f"Получена свеча: {msg.value}")

    except Exception as e:
        kafka_status = "DOWN"
        error_details = str(e)
        logger.error(f"Ошибка Kafka: {e}")
    finally:
        await consumer.stop()
        await producer.stop()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"Сервис {SERVICE_NAME} v{APP_VERSION} запускается...")
    # Запускаем Kafka в фоне
    task = asyncio.create_task(kafka_worker())
    yield
    task.cancel()
    logger.info(f"Сервис {SERVICE_NAME} остановлен.")


app = FastAPI(title=SERVICE_NAME, version=APP_VERSION, lifespan=lifespan)


@app.get("/health")
async def health_check() -> JSONResponse:
    overall_status = "UP" if kafka_status == "UP" else "DEGRADED"
    
    broker_info = {"status": kafka_status}
    if error_details:
        broker_info["details"] = error_details

    return JSONResponse(
        status_code=200,
        content={
            "status": overall_status,
            "service": SERVICE_NAME,
            "version": APP_VERSION,
            "components": {
                "messageBroker": broker_info
            },
        },
    )

@app.post("/signals", response_model=SignalResponse)
async def create_signal(request: SignalRequest) -> SignalResponse:
    try:
        return calculate_signal(request)
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error

if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=PORT)