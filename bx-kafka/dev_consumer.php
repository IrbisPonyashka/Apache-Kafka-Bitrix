<?php

require 'vendor/autoload.php';

use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use Irbisz\Crm\BxEntityManager;
use BxKafka\Consumer\ConsumerHandler;
use BxKafka\MessageHandler\AutoMessageHandler;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;

$kafkaConfig = require '/home/bitrix/dev/bx-kafka/config/kafka.php';

require '/home/bitrix/dev/bx-kafka/vendor/autoload.php';
require '/home/bitrix/dev/bx-kafka/src/Consumer/ConsumerHandler.php';
require '/home/bitrix/dev/bx-kafka/src/MessageHandler/MessageHandlerInterface.php';
require '/home/bitrix/www/bitrix/php_interface/Irbisz/classes/Kafka/BxEntityManager.php';
require '/home/bitrix/dev/bx-kafka/src/MessageHandler/PrescoringMessageHandler.php';

$conf = new Conf();

$conf->set('group.id',                  $kafkaConfig['group_id_prescoring'] );
$conf->set('metadata.broker.list',      implode(',', $kafkaConfig['brokers']) );
$conf->set('auto.offset.reset',         $kafkaConfig['offset_reset'] );
$conf->set('session.timeout.ms',        $kafkaConfig['session_timeout_ms'] );
$conf->set('max.poll.interval.ms',      $kafkaConfig['max_poll_interval_ms'] );

$consumer = new KafkaConsumer($conf);
$consumer->subscribe([$kafkaConfig['topic_prescoring']]);
// $consumer->subscribe(['test-from-uauto-to-bitrix']);

while (true) {
    $message = $consumer->consume(1000);
    switch ($message->err) {
        case RD_KAFKA_RESP_ERR_NO_ERROR:
            echo "Message received:\n";
            echo "Topic: " . $message->topic_name . "\n";
            echo "Partition: " . $message->partition . "\n";
            echo "Offset: " . $message->offset . "\n";
            echo "Payload: " . $message->payload . "\n";
            break;
        case RD_KAFKA_RESP_ERR__TIMED_OUT:
            echo "Waiting for messages...\n";
            break;
        case RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION:
            echo "Error: Unknown partition. Check if the topic exists.\n";
            break;
        default:
            echo "Error: " . $message->errstr() . "\n";
            break;
    }
}
