import multiprocessing
import os
from processing_engine import logger

brokers = os.getenv('KAFKA_BROKERS', 'localhost:9092')

def start_health_vitals_prediction():
    pass

if __name__ == "__main__":
    logger.debug("Starting Consumers in Processing Engine")

    logger.debug("Starting Health Vitals Prediction Model")
    p1 = multiprocessing.Process(target=start_health_vitals_prediction)
    p1.start()

    p1.join()
