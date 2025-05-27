<?php

namespace BxKafka\MessageHandler;

use RdKafka\Message;

interface MessageHandlerInterface
{
    // public function __construct(object $entityManager);
    public function handle(Message $message): void;
}