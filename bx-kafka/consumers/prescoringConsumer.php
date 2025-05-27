<?php

error_reporting(E_ALL);
ini_set('display_errors', 0);
ini_set('log_errors', 1);
// ini_set('error_log', '/home/bitrix/dev/bx-kafka/logs/php_errors.log');
ini_set('error_log', 'php://stderr');


use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use Irbisz\Crm\BxEntityManager;
use BxKafka\Consumer\ConsumerHandler;
use BxKafka\MessageHandler\PrescoringMessageHandler;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;

try {
    $kafkaConfig = require '/home/bitrix/dev/bx-kafka/config/kafka.php';

    require '/home/bitrix/dev/bx-kafka/vendor/autoload.php';
    require '/home/bitrix/dev/bx-kafka/src/Consumer/ConsumerHandler.php';
    require '/home/bitrix/dev/bx-kafka/src/MessageHandler/MessageHandlerInterface.php';
    require '/home/bitrix/www/bitrix/php_interface/Irbisz/classes/Kafka/BxEntityManager.php';
    require '/home/bitrix/dev/bx-kafka/src/MessageHandler/PrescoringMessageHandler.php';

    $logger = new Logger('prescoring_consumer');
    $logger->pushHandler(new StreamHandler('/home/bitrix/dev/bx-kafka/logs/prescoring/prescoring_consumer.log', Logger::DEBUG));

    $conf = new Conf();

    $conf->set('group.id',                  $kafkaConfig['group_id_prescoring'] );
    $conf->set('metadata.broker.list',      implode(',', $kafkaConfig['brokers']) );
    $conf->set('auto.offset.reset',         $kafkaConfig['offset_reset'] );
    $conf->set('session.timeout.ms',        $kafkaConfig['session_timeout_ms'] );
    $conf->set('max.poll.interval.ms',      $kafkaConfig['max_poll_interval_ms'] );

    $consumer = new KafkaConsumer($conf);
    $consumer->subscribe([$kafkaConfig['topic_prescoring']]);


    $bxEntityManager = new BxEntityManager();
    $messageHandler = new PrescoringMessageHandler($consumer, $bxEntityManager);
    $kafkaConsumerHandler = new ConsumerHandler($consumer, $messageHandler, $logger);
    
    $kafkaConsumerHandler->handleMessages();

} catch (Throwable $e) {
    error_log("Fatal error in prescoringConsumer: " . $e->getMessage());
    exit(1);
}