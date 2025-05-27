<?php

namespace BxKafka\Consumer;

use RdKafka\KafkaConsumer;
use RdKafka\Message;
use Psr\Log\LoggerInterface; // Рекомендуется использовать PSR-3 Logger

class ConsumerHandler
{
    private KafkaConsumer $consumer;
    private object $messageHandler; // Интерфейс MessageHandlerInterface
    private LoggerInterface $logger; // Инъекция логгера

    public function __construct(KafkaConsumer $consumer, object $messageHandler, LoggerInterface $logger)
    {
        $this->consumer = $consumer;
        if (!($messageHandler instanceof \BxKafka\MessageHandler\MessageHandlerInterface)) {
            throw new \InvalidArgumentException(
                sprintf('Message handler must implement %s', \BxKafka\MessageHandler\MessageHandlerInterface::class)
            );
        }
        
        $this->messageHandler = $messageHandler;
        $this->logger = $logger;
    }

    public function handleMessages(): void
    {
        while (true) {
            $message = $this->consumer->consume(1000);
            $this->processMessage($message);
        }
    }

    private function processMessage(Message $message): void
    {
        switch ($message->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                $this->logger->debug('Received message', [
                    'topic' => $message->topic_name,
                    'partition' => $message->partition,
                    'offset' => $message->offset,
                    'payload' => $message->payload,
                ]);

                try {
                    $this->messageHandler->handle($message);
                } catch (\Throwable $e) {
                    $this->logger->error('Error processing message', [
                        'error' => $e->getMessage(),
                        'trace' => $e->getTraceAsString(),
                    ]);
                    exit(1);
                }
                break;
            case RD_KAFKA_RESP_ERR__TIMED_OUT:
                break;
            case RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION:
                $errorMessage = "Error: Unknown partition. Check if the topic exists.";
                $this->logger->critical($errorMessage);
                // Решение о дальнейших действиях (например, остановка consumer)
                exit(1);
                break;
            default:
                $errorMessage = "Kafka Error: " . $message->errstr();
                $this->logger->error($errorMessage);
                exit(1);
        }
    }
}