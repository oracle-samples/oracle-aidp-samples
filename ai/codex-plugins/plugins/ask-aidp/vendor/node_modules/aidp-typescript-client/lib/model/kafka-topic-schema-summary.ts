// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Derived model for topic of a Kafka external catalog.
*/
export interface KafkaTopicSchemaSummary extends model.SchemaSummary {
    /**
    * The number of partitions in the Kafka topic.
    */
    'partitions'?: string;
    /**
    * The replication factor.
    */
    'replicationFactor'?: string;

   "entityType": string;
}

export namespace KafkaTopicSchemaSummary {



    export function getJsonObj(obj: KafkaTopicSchemaSummary, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.SchemaSummary.getJsonObj(obj) as KafkaTopicSchemaSummary, ...{
            


        }};

        
        
        return jsonObj;
    }
    export const entityType = 'KAFKA_TOPIC';
    export function getDeserializedJsonObj(obj: KafkaTopicSchemaSummary, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.SchemaSummary.getDeserializedJsonObj(obj) as KafkaTopicSchemaSummary, ...{
            


         }};

        
        
        return jsonObj;
    }
}
