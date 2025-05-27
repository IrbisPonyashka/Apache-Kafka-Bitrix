<?php

require 'vendor/autoload.php';

use RdKafka\Producer;
use RdKafka\Conf;

// Создаем конфигурацию
$conf = new Conf();

// Устанавливаем необходимые параметры конфигурации, если это требуется
// Например, можно установить уровень логирования:
$conf->set('log_level', (string) LOG_DEBUG);

// Указываем адреса брокеров через конфигурацию
$conf->set('metadata.broker.list', '10.124.32.37:9092,10.124.32.38:9092,10.124.32.39:9092');

// Создаем продюсера
$producer = new Producer($conf);

$topic = $producer->newTopic("test-from-uauto-agent-to-bitrix");
// $topic = $producer->newTopic("test-from-uauto-to-bitrix");
// $topic = $producer->newTopic("test-from-uauto-prescoring-to-bitrix");
// $topic = $producer->newTopic("from-uauto-prescoring-to-bitrix");

/* $lost_records_file = file_get_contents('lost_records.json');
$lost_records = json_decode($lost_records_file, 1);

foreach ($lost_records as $key => $record)
{
    $message = json_encode([
        "id" => generateUuidV4(),
        "name" => "moderation.created",
        "timestamp" => time(),
        "data" => json_encode($record)
    ]);
    // echo "<pre>\n"; print_r($message); echo "</pre>\n";
    $topic->produce(RD_KAFKA_PARTITION_UA, 0, $message);
} */
// die;

$message = '{
    "id": "c4cc81a9-e8ca-49e8-9934-5cb2d4aab7eb",
    "name": "moderation.agent.updated",
    "timestamp": 1745929740407484,
    "data": "{\"id\":\"adwoiahwdiuhadihahwd\",\"companyName\":\"ООО КРОКоДИЛО БОМБАРДИЛО\",\"leadName\":\"ТУН ТУН ТУН САХУР\",\"phoneNumbers\":[],\"updatedAt\":1745929739}\n"
}';

// $message = '{
//     "id": "a53d2ca4-3467-40dd-a5c9-78066b1cc8a6",
//     "name": "moderation.scoring.created",
//     "timestamp": 1745910164396241,
//     "data": "{\"id\":\"lnawdn328928d23d328d283jd382j\",\"userId\":\"lnawdn328928d23d328d283jd382j\",\"phoneNum\":\"99822332323\",\"name\":\"testsetset\",\"status\":\"APPROVED\",\"regionId\":\"644ba0905d0dd5f02ee6f4b5\",\"amount\":333220000,\"downPayment\":12000000,\"totalPeriodInMonths\":12,\"isArchive\":false,\"blockedUntil\":1745995707,\"createdAt\":1745910047,\"updatedAt\":1745910164,\"reasonCode\":\"INCORRECT_PERSONAL_DATA\",\"modelId\":\"65762c8f552859a70cafa3b2\",\"modelCode\":\"chazor\",\"brandCode\":\"byd\",\"intendedDownPayment\":12000000,\"monthlyRepaymentAmount\":9994016,\"purchaseAtPeriod\":\"TODAY\",\"districtCode\":\"132\",\"address\":\"B\",\"birthDate\":\"09.09.1999\",\"requestedAt\":1745910062,\"expiredAt\":1745996564,\"downPaymentAllowedFrom\":740000000,\"purchaseRegionIds\":[\"644ba0915d0dd5f02ee6f6c0\"],\"modelLabel\":\"BYD Chazor\",\"tries\":[{\"id\":\"6810792e4d19419b86fafd78\",\"status\":\"SOFT_REJECTED\",\"reason\":\"INCORRECT_PERSONAL_DATA\",\"requestedAt\":\"2025-04-29T07:01:02.366Z\",\"resultedAt\":\"2025-04-29T07:02:44.081161039Z\"}],\"maxPrice\":273245000}\n"
// }';

// $message = '{
//     "id":"67dc7e6f-baf6-438a-acd1-a66cce6123123123db8cb",
//     "name":"moderation.scoring.created",
//     "timestamp":1744790030463984,
//     "data":"{\"id\":\"67fe118e1cb8bf8e13b9246531313132\",\"userId\":\"658d8582da7164b8d08d04b1\",\"phoneNum\":\"998550011115\",\"name\":\"\",\"status\":\"SOFT_REJECTED\",\"regionId\":\"644ba0905d0dd5f02ee6f59e\",\"amount\":119787500,\"downPayment\":9583000,\"totalPeriodInMonths\":36,\"isArchive\":false,\"blockedUntil\":0,\"createdAt\":1744703886,\"updatedAt\":1744790030,\"reasonCode\":\"INCORRECT_PERSONAL_DATA\",\"modelId\":\"657176391d51da1fa959720c\",\"modelCode\":\"nexia3\",\"brandCode\":\"chevrolet\",\"intendedDownPayment\":8000000,\"monthlyRepaymentAmount\":4323058,\"purchaseAtPeriod\":\"THIS_WEEK\",\"districtCode\":\"024\",\"address\":\"ygfdghj\",\"birthDate\":\"09.09.2000\",\"requestedAt\":1744790021,\"expiredAt\":1744876430,\"downPaymentAllowedFrom\":740000000,\"purchaseRegionIds\":[\"644ba0935d0dd5f02ee6f815\"],\"modelLabel\":\"Chevrolet Nexia 3\",\"tries\":[{\"id\":\"67ff62052895d4f7240e7d59\",\"requestedAt\":\"2025-04-16T07:53:41.521Z\",\"resultedAt\":\"2025-04-16T07:53:50.152520292Z\"},{\"id\":\"67ff62052895d4f7240e7d59\",\"requestedAt\":\"2025-05-16T07:53:41.521Z\",\"resultedAt\":\"2025-05-16T07:53:50.152520292Z\"}]}\n"
// }';
$topic->produce(RD_KAFKA_PARTITION_UA, 0, $message);


// Ожидаем, пока все сообщения будут отправлены
$result = $producer->flush(10000);

// Проверяем результат отправки
if (RD_KAFKA_RESP_ERR_NO_ERROR !== $result) {
    echo "Failed to flush messages to Kafka\n";
} else {
    echo "Message sent successfully\n";
}


function generateUuidV4(): string
{
    $data = random_bytes(16);

    // Устанавливаем версию (0100xxxx = версия 4)
    $data[6] = chr((ord($data[6]) & 0x0f) | 0x40);
    // Устанавливаем variant (10xxxxxx)
    $data[8] = chr((ord($data[8]) & 0x3f) | 0x80);

    return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($data), 4));
}