// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Derived model for topic of a Kafka external catalog.
*/
export interface KafkaTopicSchema extends model.Schema {
    /**
    * The number of partitions in the Kafka topic.
    */
    'partitions'?: string;
    /**
    * The replication factor.
    */
    'replicationFactor'?: string;
    /**
    * Bootstrap servers for the Kafka topic.
    */
    'bootstrapServers'?: string;
    /**
    * The details of the partitions in Kafka topic.
    */
    'partitionDetails': string;

   "entityType": string;
}

export namespace KafkaTopicSchema {





    export function getJsonObj(obj: KafkaTopicSchema, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Schema.getJsonObj(obj) as KafkaTopicSchema, ...{
            




        }};

        
        
        return jsonObj;
    }
    export const entityType = 'KAFKA_TOPIC';
    export function getDeserializedJsonObj(obj: KafkaTopicSchema, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Schema.getDeserializedJsonObj(obj) as KafkaTopicSchema, ...{
            




         }};

        
        
        return jsonObj;
    }
}
