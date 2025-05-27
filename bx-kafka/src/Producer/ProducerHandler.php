<?php

namespace BxKafka\Producer;

use RdKafka\Conf;
use RdKafka\Producer;

class ProducerHandler
{
    private $broker_list = '10.111.22.33:4444,10.111.22.00:4444,10.111.22.11:4444';

    public function __construct()
    {
    }

    public function addProducer(): Producer
    {
        // return [];
        $conf = new Conf();
        
        $conf->set('log_level', (string) LOG_DEBUG);
        
        $conf->set('metadata.broker.list', $this->broker_list);
        
        return new Producer($conf);
    }

    public function sendMessage($topic_name, $message)
    {
        try {
            $producer = $this->addProducer();

            $topic = $producer->newTopic($topic_name);
            
            /** 
                * [
                    * "id"=> $deal["auto_id"], // $deal["UF_CRM_1702561941"][0],
                    * "name"=> $deal["auto_version"], // $deal["UF_CRM_1702561969"][0],
                    * "timestamp"=> time(), // UNIX
                    * "data"=>json_encode(
                    *     [
                    *         "id"=> $deal["auto_id"], //$deal["UF_CRM_1702561941"][0],
                    *         "version"=> $deal["auto_version"], // $deal["UF_CRM_1702561969"][0],
                    *         "resolution"=> $deal["auto_resolution"] ,//$deal["STAGE_ID"],
                    *         "comment"=> $deal["auto_comment"], // $deal["UF_CRM_1697717537317"],
                    *         "block_statuses"=> $deal["block_statuses"] ?? [] ,  // $deal["UF_CRM_1697717537317"],
                    *         "createdAt" => time(),
                    *     ]
                    * )
                * ]
            */
            
            $topic->produce(RD_KAFKA_PARTITION_UA, 0, $message);
            
            $result = $producer->flush(10000);
            
            // Logger($result, $comment, $fileName) = array();
            self::Logger($result, "result", "producerHandlerMsg");
            if (RD_KAFKA_RESP_ERR_NO_ERROR !== $result) {
                $return["success"] = false;
                $return["error"] = $result;
                $return["message"] = "Failed to flush messages to Kafka";
            } else {
                $return["success"] = true;
                $return["message"] = "Message sent successfully";
            }

            return $return;

        } catch (\Throwable $th) {
            self::Logger($th, "cath_error", "producerHandlerError");
            
            return [
                "success" => false,
                "error" => $th,
                "message" => "Failed to flush messages to Kafka",
            ];
        }
    }
    
}