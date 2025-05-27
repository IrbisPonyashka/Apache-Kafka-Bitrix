<?php

namespace Irbisz\Crm;

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

// Проверяем, что модуль CRM подключен
if (!Loader::includeModule('crm')) {
    $response = new \Bitrix\Main\Engine\Response\Json(
        ['status' => 'error', 'message' => 'Модуль CRM не найден.']
    );
    $response->send();
    die();
}
if (!Loader::includeModule('bizproc')) {
    $response = new \Bitrix\Main\Engine\Response\Json(
        ['status' => 'error', 'message' => 'Модуль bizproc не найден.']
    );
    $response->send();
    die();
}

class BxEntityManager
{
    /**
     * Инициализация для топика по Лидгену
     */
    public function prescoring_init(array $data): array
    {
        $deal_action_res = [
            'status' => null,
            'dealId' => null,
            'message' => null
        ];

        $action = "add";
        // Поиск сделки по user_ID(uzum_bank_kafka)
        $currentDeal = self::findDealsByUzumUserId( $data["userId"] );
        $currentDeal != null ? $action = "update" : null;

        // подготовка данных
        $dealFields = self::preparePrescoringDealFeilds($data, $action, $currentDeal);

        // обновления
        if($action == "update" && $currentDeal != null && !empty($currentDeal["ID"])) {
            $deal_action_res = self::updateDeal((int)$currentDeal["ID"], $dealFields);
        // новая сделка
        }else{
            $deal_action_res = self::addDeal($dealFields);
        }

        return $deal_action_res;
    }

    /**
     * Инициализация для топика по Авто
     */
    public function avto_init(array $data): array
    {
        $action = "add";
        // Поиск сделки по id машины(авто)
        $currentDeal = self::findDealsByAvtoId( $data["id"] );
        $currentDeal != null ? $action = "update" : null;

        // поиск контакта по ownerId
        if( $data["ownerId"] ){
            $data["contactId"] =  self::findContactsByAvtoId( $data["ownerId"] );
        }
        // подготовка данных
        $dealFields = self::prepareAvtoDealFeilds($data, $action, $currentDeal);

        // return $dealFields;
        // обновления
        if($action == "update" && $currentDeal != null && !empty($currentDeal["ID"])) {
            $deal_action_res = self::updateDeal((int)$currentDeal["ID"], $dealFields);
            // новая сделка
        }else{
            $deal_action_res = self::addDeal($dealFields);
        }

        return $deal_action_res;
    }

    /**
     * Инициализация для топика по Комиссионерам
     */
    public function agent_init(array $data): array
    {
        $contact_action_res = [
            'status' => null,
            'contactId' => null,
            'message' => null
        ];

        $action = "add";
        // агент (комиссионер)
        $contactId = self::findContactsByAvtoId( $data["id"] );
        $contactId != null ? $action = "update" : null;

        // подготовка данных
        $contactFields = self::prepareAgentContactFeilds($data, $action);

        // return $contactFields;
        // обновления
        if($action == "update" && $contactId != null ) {
            $contact_action_res = self::updateContact((int)$contactId, $contactFields);
            // новая сделка
        }else{
            $contact_action_res = self::addContact($contactFields);
        }

        return $contact_action_res;

        return [
            "status" => "принят",
            "data" => $data
        ];
    }

    /**
     * avto_id (uzum_bank_kafka)
     * @return deal array
     */
    protected static function findDealsByAvtoId(string $avto_id): array | null
    {
        $filter = [ "UF_CRM_1708675963304" => $avto_id ];
        $select = ["ID", "TITLE", "STAGE_ID", "CATEGORY_ID", "UF_CRM_1708675963304", "UF_CRM_1708675997605"];

        $findedDealsByAvtoId = self::getDealList($filter, $select);
        $currentDeal = null;
        if( !empty($findedDealsByAvtoId) && count($findedDealsByAvtoId) > 0){
            $currentDeal = $findedDealsByAvtoId[0];
        }

        return $currentDeal;
    }

    /**
     * user_id (uzum_bank_kafka)
     * @return deal array
     */
    protected static function findDealsByUzumUserId(string $user_id): array | null
    {
        $filter = [ "UF_CRM_1732101166976" => $user_id ];
        $select = ["ID", "TITLE", "STAGE_ID", "CATEGORY_ID", "UF_CRM_1732101166976", "UF_CRM_1708675963304"];

        $findedDealsByAvtoId = self::getDealList($filter, $select);
        $currentDeal = null;
        if( !empty($findedDealsByAvtoId) && count($findedDealsByAvtoId) > 0){
            $currentDeal = $findedDealsByAvtoId[0];
        }

        return $currentDeal;
    }

    protected static function findContactsByAvtoId(string $owner_id): string | null
    {
        $contactId = null;
        $filter = [
            "UF_CRM_1708692967696" => $owner_id,
            "CHECK_PERMISSIONS" => "N",
        ];
        $select = [ "ID", "UF_CRM_1708692967696" ];
        $contactListByAvtoId = self::getContactList($filter, $select);
        if(!empty($contactListByAvtoId) && count($contactListByAvtoId) > 0)
        {
            $contactId = $contactListByAvtoId[0]["ID"];
        }

        return $contactId;
    }

    /**
     * приготовление полей для ЛИДГЕНА(PRESCORING)
     */
    protected static function preparePrescoringDealFeilds(array $data, string $action, array | null $currentDeal = null): array
    {
        $raw = [
            "fields" => [
                "TITLE"                     =>      $data["name"],
                "CATEGORY_ID"               =>      "73",
                "UF_CRM_1731518916"         =>      $data["id"],
                "UF_CRM_1731518948"         =>      $data["regionId"],
                "UF_CRM_1733306116484"      =>      $data["reasonCode"] ?? "", // reasonCode
                "UF_CRM_1731518527"         =>      $data["amount"] ? (string) $data["amount"] . "|UZS" : "0",
                "UF_CRM_1731518775"         =>      $data["downPayment"] ? (string) $data["downPayment"] . "|UZS" : "0",
                "UF_CRM_1731518729"         =>      $data["totalPeriodInMonths"],
                "UF_CRM_1731518828"         =>      $data["createdAt"] ? date('d.m.Y H:i:s', (int) $data["createdAt"]) : "", // 1729158843
                "UF_CRM_1731518878"         =>      $data["updatedAt"] ? date('d.m.Y H:i:s', (int) $data["updatedAt"]) : "", // 1729158843
                "UF_CRM_1732101166976"      =>      $data["userId"] ? $data["userId"] : "",
                "UF_CRM_1686143533955"      =>      $data["phoneNum"] ? [$data["phoneNum"]] : "",
                "UF_CRM_1732101082644"      =>      (string) $data["isArchive"] ? (string) $data["isArchive"] : "",
                "UF_CRM_1732101115359"      =>      $data["blockedUntil"] ? date('d.m.y H:m:s',(int) $data["blockedUntil"]) : "",
                "UF_CRM_1737381834361"      =>      "",
            ]
        ];

        // Логирования(со стороны uzum.auto) [tries]
        $logFieldAr = [];
        if( !empty($data["tries"]) && count($data["tries"]) > 0 )
        {
            foreach ($data["tries"] as $key => $try)
            {
                $requestedAt = new \DateTime($try["requestedAt"], new \DateTimeZone('UTC'));
                $requestedAt->setTimezone(new \DateTimeZone('Asia/Tashkent'));

                $resultedAt = new \DateTime($try["resultedAt"], new \DateTimeZone('UTC'));
                $resultedAt->setTimezone(new \DateTimeZone('Asia/Tashkent'));

                $SCORING_SENT = $requestedAt->format('d.m.Y H:i:s');
                $SCORING_RECEIVED = $resultedAt->format('d.m.Y H:i:s');

                $logFieldAr[] = "$SCORING_SENT - SCORING_SENT";
                $logFieldAr[] = "$SCORING_RECEIVED - SCORING_RECEIVED";
            }

            $raw["fields"]["UF_CRM_1737381834361"] = $logFieldAr;
        }

        // reasoneCode
        if($data["reasonCode"]){
            switch ($data["reasonCode"]) {
                case "HAS_DEBT":
                    $raw["fields"]["UF_CRM_1733306116484"] = "Есть задолженность";
                    break;
                case "HAS_OVERDUE_DEBT":
                    $raw["fields"]["UF_CRM_1733306116484"] = "Есть просроченный платеж";
                    break;
                case "INCORRECT_PERSONAL_DATA":
                    $raw["fields"]["UF_CRM_1733306116484"] = "Ошибка заполнения";
                    break;
                case "INCORRECT_REG_DATE":
                    $raw["fields"]["UF_CRM_1733306116484"] = "Некорректные даты в документах";
                    break;
                case "BANK_DECLINED":
                    $raw["fields"]["UF_CRM_1733306116484"] = "Несоответствие политике банка";
                    break;
            }
        }

        if (
            !$currentDeal
            || in_array($currentDeal["STAGE_ID"], ["C73:UC_OT28S2", "C73:PREPARATION", "C73:PREPAYMENT_INVOIC"])
        ) {
            switch ($data["status"]) {
                case 'HARD_REJECTED':
                    $raw["fields"]["STAGE_ID"] = "C73:LOSE";
                    break;
                case 'APPROVED':
                    $raw["fields"]["STAGE_ID"] = "C73:PREPAYMENT_INVOIC";
                    break;
                case 'SOFT_REJECTED':
                    $raw["fields"]["STAGE_ID"] = "C73:PREPARATION";
                    break;
                case 'IN_PROCESS':
                    $raw["fields"]["STAGE_ID"] = "C73:PREPAYMENT_INVOIC";
                    break;
                case 'CREATED':
                    $raw["fields"]["STAGE_ID"] = "C73:NEW";
                    break;
            }
        }

        return $raw;
    }

    /**
     * приготовление полей для АВТО(AUTO)
     */
    protected static function prepareAvtoDealFeilds(array $data, string $action, array | null $currentDeal = null): array
    {

        $raw = [
            "fields" => [
                "TITLE"                   =>   "Заявка с сайта uzum.auto",
                "STAGE_ID"                =>   "C69:NEW",
                "CATEGORY_ID"             =>   "69", // воронка "uzum auto модерация"
                "CONTACT_ID"              =>   $data["contactId"] ?? null, // контакт=комсионер
                "UF_CRM_1708675963304"    =>   $data["id"], // ID
                "UF_CRM_1708675997605"    =>   $data["version"], // версия PROD
                "UF_CRM_1618904191"       =>   $data["brand"], // марка PROD
                "UF_CRM_1701930819061"    =>   $data["model"], // модель
                "UF_CRM_1701931065488"    =>   "" , // гос номер
                "UF_CRM_1701930883403"    =>   $data["madeYear"] , // год выпуска
                "UF_CRM_1701930915987"    =>   $data["isGas"] ? "Есть" : "Отсутствует" , // наличие газового оборудования
                "UF_CRM_1701931045128"    =>   $data["isSunroof"] ? "Есть" : "Отсутствует" , // наличие люка
                "UF_CRM_1701930941412"    =>   $data["driveType"] , // привод
                "UF_CRM_1701930898227"    =>   $data["engineType"] , // тип топлива
                "UF_CRM_1701930929594"    =>   $data["transmission"] , // коробка передач
                "UF_CRM_1701930967570"    =>   $data["engineVolume"] , // объем двигателя, л
                "UF_CRM_1701930988762"    =>   $data["enginePower"] , // мощность
                "UF_CRM_1701931004481"    =>   $data["mileage"], // пробег, км
                "UF_CRM_1701931019657"    =>   "", // состояние кузова
                "UF_CRM_1701931032417"    =>   "", // цвет
                "UF_CRM_1701930863171"    =>   "", // доп. характеристика
                "UF_CRM_1701931098840"    =>   $data["description"], //описание
                "UF_CRM_1708676378871"    =>   $data["ownerId"], // id коммисинера
                // "UF_CRM_1697711268976"    =>   $data["cost"] && $data["currency"] ? "$data[cost]|$data[currency]" : null, // цена
                // {"fileData"              =>[`${msgFields.model}.jpeg`, file]}

            ]
        ];

        // сумма|валюта
        if( !empty($data["cost"]) && !empty($data["currency"])){
            $raw["fields"]["UF_CRM_1697711268976"] = "$data[cost]|$data[currency]";
        }
        // агент
        if( !empty($data["ownerType"])){
            $raw["fields"]["UF_CRM_AUTO_OWNER_TYPE"] = $data["ownerType"] == "AGENT" ? 10212 : 10213;
        }
        // картинки
        if( !empty($data["photo"]) && count($data["photo"]) > 0 )
        {
            // обработка фоток, конвертация в base64
            $images = self::prepareBxImagesField($data["photo"], $data["model"]);
            // $raw["fields"]["UF_CRM_1701931113016"] = $images;
            $raw["fields"]["UF_CRM_AVTO_IMAGES"] = $images;
        }

        if($action === "add")
        {
            $raw["fields"]["STAGE_ID"] = "C69:NEW";
            $raw["fields"]["CATEGORY_ID"] = "69";
            $raw["fields"]["UF_CRM_1695817928491"] = $data["ownerPhone"];
        }
        elseif($action === "update" && $currentDeal)
        {
            if( ($data["status"] == "ACTIVE" || $data["status"] == "PRE_MODERATE") && $currentDeal && !empty($currentDeal["UF_CRM_1708675997605"]) )
            {
                /* необходимо сравнить версии */
                $msgVersion = $data["version"]; // версия тикета из сообщения
                $msgVersion = explode("_", $msgVersion)[1];

                $currentMsgVersion = $currentDeal["UF_CRM_1708675997605"]; // текущая версия тикета в CRM
                $currentMsgVersion = explode("_", $currentMsgVersion)[1];

                /* И Если текущая версия ниже новой, то меняем стадию */
                if($msgVersion > $currentMsgVersion){
                    $raw["fields"]["STAGE_ID"] = "C69:NEW";
                };
            }
        }

        return $raw;
    }

    /**
     * приготовление полей для Комиссионеров(AGENT)
     */
    protected static function prepareAgentContactFeilds(array $data, string $action, array | null $currentContact = null): array
    {
        $raw = [
            "fields" => [
                "UF_CRM_1708692912392"  =>  $data["companyName"],
                "NAME"                  =>  $data["leadName"],
                "LAST_NAME"             =>  $data["TITLE"],
                "OPENED"                =>  "Y",
                "ASSIGNED_BY_ID"        =>  1,
                "TYPE_ID"               =>  "CLIENT",
                "SOURCE_ID"             =>  "WEB",
            ]
        ];

        // ID комиссионера
        $action == "add" ? $raw["fields"]["UF_CRM_1708692967696"]  =  $data["id"] : null;

        $phones = [];
        // номера агента
        if($data["phoneNumbers"] && count($data["phoneNumbers"]) > 0){
            foreach ($data["phoneNumbers"] as $key => $value) {
                $phones[] = [
                    "VALUE" => $value,
                    "VALUE_TYPE" => "WORK"
                ];
            }
        }

        // Дата последнего обновления (avto)
        $dateFormat = (new \DateTime())->setTimestamp($data["updatedAt"])->setTimezone(new \DateTimeZone('UTC'));
        $formattedDate = $dateFormat->format('Y-m-d\TH:i:s.v\Z');

        $raw["fields"]["UF_CRM_1708692996422"] = $formattedDate;
        $raw["fields"]["FM"]["PHONE"] = $phones;

        return $raw;
    }

    public static function getDealList(array $filter = [], array $select = ["*", "UF_*"], array $order = array('ID' => 'ASC') ): array
    {
        return DealTable::getList(array(
            "select" => $select,
            "filter" => $filter,
            "order" => $order
        ))->fetchAll();
    }

    public static function getDeal(array $data): array
    {

        Loader::includeModule("crm");

        // Получение списка сделок через ORM
        $deals = DealTable::getList([
            "filter" => ["STAGE_ID" => "NEW"],
            "select" => ["ID", "TITLE"]
        ]);

        while ($deal = $deals->fetch()) {
            echo "Сделка: " . $deal["TITLE"] . "\n";
        }
        return [];
    }

    public static function addDeal(array $data): array
    {
        if ($data) {
            try {
                $options = [ 'CURRENT_USER' => 1 ]; //из под админа
                $deal = new \CCrmDeal(false);
                $dealId = $deal->Add($data["fields"], true, $options);

                if ($dealId > 0) {
                    $result = [
                        'status' => 'success',
                        'dealId' => $dealId,
                        'message' => 'Элемент успешно создан.'
                    ];
                    $errors = [];
                    \CCrmBizProcHelper::AutoStartWorkflows(
                        \CCrmOwnerType::Deal, // \CCrmOwnerType::Lead, ...
                        $dealId,
                        \CCrmBizProcEventType::Create,
                        $errors,
                        []
                    );
                } else {
                    global $APPLICATION;
                    $errorText = '';

                    // Получаем ошибку из $APPLICATION
                    if ($exception = $APPLICATION->GetException()) {
                        $errorText = $exception->GetString();
                    } else {
                        // Если нет исключений, возможно, ошибка вернётся в $deal->LAST_ERROR
                        $errorText = $deal->LAST_ERROR;
                    }
                    $result = [
                        'status' => 'error',
                        'errors' => "Ошибка создание сделки",
                        'message' => $errorText,
                        'data' => [
                            "deal_id" => $dealId,
                            "fields" => $data
                        ]
                    ];
                }

                return $result;

            } catch (Exception $e) {
                return [
                    'status' => 'error',
                    'message' => $e->getMessage()
                ];
            }
        } else {
            return [
                'status' => 'error',
                'message' => 'Некорректные данные для создания элемента.'
            ];
        }
    }

    public static function updateDeal( int $id, array $data): array
    {
        if ($id && $data) {

            try {
                $options = [ 'CURRENT_USER' => 1 ]; // из под админа
                $deal = new \CCrmDeal(false);
                $dealUpdRes = $deal->Update($id, $data["fields"], true, true, $options);

                if ($dealUpdRes > 0) {
                    $result = [
                        'status' => 'success',
                        'dealId' => $id,
                        'message' => 'Элемент успешно обновлен.'
                    ];
                    $errors = [];
                    \CCrmBizProcHelper::AutoStartWorkflows(
                        \CCrmOwnerType::Deal, // \CCrmOwnerType::Lead, ...
                        $id,
                        \CCrmBizProcEventType::Edit,
                        $errors,
                        []
                    );
                } else {
                    global $APPLICATION;
                    $errorText = '';

                    // Получаем ошибку из $APPLICATION
                    if ($exception = $APPLICATION->GetException()) {
                        $errorText = $exception->GetString();
                    } else {
                        // Если нет исключений, возможно, ошибка вернётся в $deal->LAST_ERROR
                        $errorText = $deal->LAST_ERROR;
                    }
                    $result = [
                        'status' => 'error',
                        'errors' => "Ошибка обновления сделки",
                        'message' => $errorText,
                        'data' => [
                            "deal_id" => $id,
                            "fields" => $data
                        ]
                    ];
                }

                return $result;

            } catch (Exception $e) {
                return [
                    'status' => 'error',
                    'message' => $e->getMessage()
                ];
            }
        } else {
            return [
                'status' => 'error',
                'message' => 'Некорректные данные для создания элемента.'
            ];
        }
    }

    public static function addContact(array $data): array
    {
        if ($data) {
            try {
                $options = [ 'CURRENT_USER' => 1 ]; //из под админа
                $contact = new \CCrmContact(false);
                $contactId = $contact->Add($data["fields"], true, $options);

                if ($contactId > 0) {
                    $result = [
                        'status' => 'success',
                        'contactId' => $contactId,
                        'message' => 'Элемент успешно создан.'
                    ];
                    /*
                        $errors = [];
                        \CCrmBizProcHelper::AutoStartWorkflows(
                            \CCrmOwnerType::Deal, // \CCrmOwnerType::Lead, ...
                            $dealId,
                            \CCrmBizProcEventType::Create,
                            $errors,
                            []
                        );
                    */
                } else {
                    global $APPLICATION;
                    $errorText = '';

                    // Получаем ошибку из $APPLICATION
                    if ($exception = $APPLICATION->GetException()) {
                        $errorText = $exception->GetString();
                    } else {
                        // Если нет исключений, возможно, ошибка вернётся в $contact->LAST_ERROR
                        $errorText = $contact->LAST_ERROR;
                    }
                    $result = [
                        'status' => 'error',
                        'errors' => "Ошибка создание сделки",
                        'message' => $errorText,
                        'data' => [
                            "contact_id" => $contactId,
                            "fields" => $data
                        ]
                    ];
                }

                return $result;

            } catch (Exception $e) {
                return [
                    'status' => 'error',
                    'message' => $e->getMessage()
                ];
            }
        } else {
            return [
                'status' => 'error',
                'message' => 'Некорректные данные для создания элемента.'
            ];
        }
    }

    public static function updateContact( int $id, array $data): array
    {
        if ($id && $data) {

            try {
                $options = [ 'CURRENT_USER' => 1 ]; // из под админа
                $contact = new \CCrmContact(false);
                $contactUpdRes = $contact->Update($id, $data["fields"], true, true, $options);

                if ($contactUpdRes > 0) {
                    $result = [
                        'status' => 'success',
                        'contactId' => $id,
                        'message' => 'Элемент успешно обновлен.'
                    ];
                    /*
                        $errors = [];
                        \CCrmBizProcHelper::AutoStartWorkflows(
                            \CCrmOwnerType::contact, // \CCrmOwnerType::Lead, ...
                            $id,
                            \CCrmBizProcEventType::Edit,
                            $errors,
                            []
                        );
                    */
                } else {
                    global $APPLICATION;
                    $errorText = '';

                    // Получаем ошибку из $APPLICATION
                    if ($exception = $APPLICATION->GetException()) {
                        $errorText = $exception->GetString();
                    } else {
                        // Если нет исключений, возможно, ошибка вернётся в $contact->LAST_ERROR
                        $errorText = $contact->LAST_ERROR;
                    }
                    $result = [
                        'status' => 'error',
                        'errors' => "Ошибка обновления сделки",
                        'message' => $errorText,
                        'data' => [
                            "contact_id" => $id,
                            "fields" => $data
                        ]
                    ];
                }

                return $result;

            } catch (Exception $e) {
                return [
                    'status' => 'error',
                    'message' => $e->getMessage()
                ];
            }
        } else {
            return [
                'status' => 'error',
                'message' => 'Некорректные данные для создания элемента.'
            ];
        }
    }

    public static function getContactList(array $filter = [], array $select = ["*", "UF_*"], array $order = array('ID' => 'ASC') ): array
    {
        $contacts = [];
        $contactResult = \CCrmContact::GetListEx( $order, $filter, false, false, $select );
        while( $contact = $contactResult->fetch() )
        {
            $contacts[] = $contact;
        }
        return $contacts;
    }


    public static function prepareBxImagesField(array $images, string $model = "car"): array
    {
        $imgsField = [];

        foreach ($images as $key => $imgUri)
        {

            $fileArray = \CFile::MakeFileArray($imgUri);

            if ($fileArray && is_array($fileArray)) {
                $imgsField[] = $fileArray;
            }
            // self::getConvertedImage($imgUri);
            /* 
                $fileBase64 = self::convertImageToBase64($imgUri);
                if($fileBase64)
                {
                    $fileType = pathinfo($imgUri, PATHINFO_EXTENSION);
                    
                    $imgsField[] = [
                        "fileData" => [$model . '.' . $fileType, $fileBase64]
                    ];
                }
            */

        }
        return $imgsField;
    }

    public static function convertImageToBase64(string $image_url): string
    {
        $fileBase64 = "";

        // Получаем содержимое файла
        $imageData = file_get_contents($image_url);
        if ($imageData !== false) {

            // Кодируем в base64
            $fileBase64 = base64_encode($imageData);
        }

        return $fileBase64;
    }

    private function callCurl(array $data): bool
    {
        // Реализация вызова API Битрикса
        // Возвращаем true, если успешно, и false, если ошибка
        return true;
    }
}