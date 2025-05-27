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
use BxKafka\MessageHandler\AutoMessageHandler;
use Monolog\Logger;
use Monolog\Handler\StreamHandler;

try {
    $kafkaConfig = require '/home/bitrix/dev/bx-kafka/config/kafka.php';

    require '/home/bitrix/dev/bx-kafka/vendor/autoload.php'; // Если используете Composer
    require '/home/bitrix/dev/bx-kafka/src/Consumer/ConsumerHandler.php';
    require '/home/bitrix/dev/bx-kafka/src/MessageHandler/MessageHandlerInterface.php';
    require '/home/bitrix/dev/bx-kafka/src/MessageHandler/AutoMessageHandler.php';
    require '/home/bitrix/www/bitrix/php_interface/Irbisz/classes/Kafka/BxEntityManager.php';

    $logger = new Logger('auto_consumer');
    $logger->pushHandler(new StreamHandler('/home/bitrix/dev/bx-kafka/logs/auto/auto_consumer.log', Logger::DEBUG));

    $conf = new Conf();
    
    $conf->set('group.id',                  $kafkaConfig['group_id_auto']);
    $conf->set('metadata.broker.list',      implode(',', $kafkaConfig['brokers']));
    $conf->set('auto.offset.reset',         $kafkaConfig['offset_reset']);
    $conf->set('session.timeout.ms',        $kafkaConfig['session_timeout_ms']);
    $conf->set('max.poll.interval.ms',      $kafkaConfig['max_poll_interval_ms']);

    $consumer = new KafkaConsumer($conf);
    $consumer->subscribe([$kafkaConfig['topic_auto']]);

    $bxEntityManager = new BxEntityManager();
    $autoMessageHandler = new AutoMessageHandler($consumer, $bxEntityManager);
    $kafkaConsumerHandler = new ConsumerHandler($consumer, $autoMessageHandler, $logger);

    $kafkaConsumerHandler->handleMessages();

} catch (Throwable $e) {
    error_log("Fatal error in autoConsumer: " . $e->getMessage());
    exit(1);
}