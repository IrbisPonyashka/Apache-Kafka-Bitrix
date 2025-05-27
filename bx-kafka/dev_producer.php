<?php

require 'vendor/autoload.php';

use RdKafka\Producer;
use RdKafka\Conf;

// Создаем конфигурацию
$conf = new Conf();

$conf->set('log_level', (string) LOG_DEBUG);

// Указываем адреса брокеров через конфигурацию
$conf->set('metadata.broker.list', '10.124.32.37:9092,10.124.32.38:9092,10.124.32.39:9092');

// Создаем продюсера
$producer = new Producer($conf);

$topic = $producer->newTopic("test");

$message = '{
    "id": "c4cc81a9-e8ca-49e8-9934-5cb2d4aab7eb",
    "name": "moderation.agent.updated",
    "timestamp": 1745929740407484,
    "data": "{\"id\":\"adwoiahwdiuhadihahwd\",\"companyName\":\"ООО КРОКоДИЛО БОМБАРДИЛО\",\"leadName\":\"ТУН ТУН ТУН САХУР\",\"phoneNumbers\":[],\"updatedAt\":1745929739}\n"
}';

$topic->produce(RD_KAFKA_PARTITION_UA, 0, $message);


// Ожидаем, пока все сообщения будут отправлены
$result = $producer->flush(10000);

// Проверяем результат отправки
if (RD_KAFKA_RESP_ERR_NO_ERROR !== $result) {
    echo "Failed to flush messages to Kafka\n";
} else {
    echo "Message sent successfully\n";
}
