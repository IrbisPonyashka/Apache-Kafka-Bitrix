<?php

namespace BxKafka\MessageHandler;

use RdKafka\Message;
use Irbisz\Crm\BxEntityManager;
use RdKafka\KafkaConsumer;

class PrescoringMessageHandler implements MessageHandlerInterface
{
    private BxEntityManager $bxEntityManager;

    public function __construct(KafkaConsumer $consumer, object $bxEntityManager)
    {
        $this->consumer = $consumer;
        $this->bxEntityManager = $bxEntityManager;
    }

    public function handle(Message $message): void
    {
        $messagePayload = json_decode($message->payload, true);

        if (isset($messagePayload["data"])) {
            $data = json_decode($messagePayload["data"], 1);
            
            self::Logger($data, "_message_data_", "_message_handler");
            
            $result = $this->bxEntityManager->prescoring_init($data);
            // $result["data"] = $data;
            
            if ($result["status"] == "success") {
                $this->consumer->commit($message);
                self::Logger($result, "_message_success_", "_message_result");
            } else {
                self::Logger($result, "_message_result_error", "_message_result_error");
            }
        } else {
            self::Logger("Invalid message payload", "__another_message__", "_message");
        }
    }

    private static function Logger($data, string $comment, string $fileName): void
    {
        $logDir = '/home/bitrix/dev/bx-kafka/logs/prescoring/';
        $logPath = $logDir . $fileName . '.log';

        if (!file_exists($logDir)) {
            mkdir($logDir, 0775, true);
        }

        $logMessage = '[' . date('Y-m-d H:i:s') . "] {$comment}: " . print_r($data, true) . PHP_EOL;
        file_put_contents($logPath, $logMessage, FILE_APPEND);
    }

}