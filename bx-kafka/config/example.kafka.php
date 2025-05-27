<?php

return [
    'brokers' => ["00.111.22.33:4444", "00.111.22.33:4444", "00.111.22.33:4444"],
    'offset_reset' => 'earliest',
    'session_timeout_ms' => '30000',
    'max_poll_interval_ms' => '600000',
    /* auto */
    'topic_auto' => 'test-from-service-to-bx',
    'group_id_auto' => 'test-bitrix-service-consumer-group',
    /* prescoring */
    'topic_prescoring' => 'test-from-service-apple-to-bitrix',
    'group_id_prescoring' => 'test-bitrix-apple-consumer-group',
    /* agent */
    'topic_agent' => 'test-from-service-human-to-bx',
    'group_id_agent' => 'test-bx-human-consumer-group',
];