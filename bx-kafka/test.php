<?php

use \Bitrix\Crm;
use \Bitrix\Crm\CompanyTable;
use \Bitrix\Crm\DealTable;
use \Bitrix\Crm\Merger\EntityMerger;
use \Bitrix\Crm\Integrity\DuplicateCriterion;
use \Bitrix\Main\Loader;

$_SERVER["DOCUMENT_ROOT"] = "/home/bitrix/www";

define("NO_KEEP_STATISTIC", true);
define("NOT_CHECK_PERMISSIONS", true);
define('CHK_EVENT', true);

require ($_SERVER["DOCUMENT_ROOT"] . "/bitrix/modules/main/include/prolog_before.php");

require '/home/bitrix/www/bitrix/php_interface/Irbisz/classes/Kafka/BxEntityManager.php';

use Irbisz\Crm\BxEntityManager;

$bxEntityManager = new BxEntityManager();

$messagePayload = [
    "data" => '{"id":"3741d0ce-2524-4c9b-ba3d-5dcb65257322","name":"moderation.created","timestamp":1738678299332318,"data":"{\"id\":\"67346f7b46960b913f8150d5\",\"version\":\"67346f7b46960b913f8150d5_1738678298\",\"costUsd\":1713,\"currency\":\"UZS\",\"cost\":23132333,\"description\":\"\",\"brand\":\"bmw\",\"model\":\"x1\",\"madeYear\":2017,\"mileage\":3423,\"photo\":[\"https://avto.dev.uzumauto.uz/images/content/orig/67346f7abe6e0d31c3f6b4a2.jpeg\", \"https://avto.dev.uzumauto.uz/images/content/orig/67346f7abe6e0d31c3f6b4a2.jpeg\"],\"status\":\"PRE_MODERATE\",\"ownerId\":\"64f58854da7164b8d077faac\",\"ownerPhone\":\"998550067890\",\"ownerType\":\"AGENT\",\"isGas\":false,\"isSunroof\":false,\"driveType\":\"4wd\",\"enginePower\":190,\"engineType\":\"diesel\",\"engineVolume\":2.5,\"transmission\":\"automatic\"}\n"}'
];

$messagePayload["data"] = json_decode($messagePayload["data"], 1);

$result = $bxEntityManager->avto_init( json_decode($messagePayload["data"]["data"], 1) );

print_r($result);