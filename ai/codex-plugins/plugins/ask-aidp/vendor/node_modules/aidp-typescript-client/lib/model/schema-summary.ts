// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Summary information about a schema.
*/
export interface SchemaSummary {
    /**
    * The fully qualified name of the schema in the format <catalog_name>.<schema_name>.
    */
    'key': string;
    /**
    * Schema name.
    */
    'displayName': string;
    /**
    * Schema description.
    */
    'description'?: string;
    /**
    * The date and time the schema was created.
    */
    'timeCreated'?: Date;
    /**
    * The date and time the schema was updated.
    */
    'timeUpdated'?: Date;
    /**
    * ID of the user who created the schema.
    */
    'createdBy'?: string;
    /**
    * ID of the user who last updated the schema.
    */
    'updatedBy'?: string;
    /**
    * The current state of the schema.
    */
    'lifecycleState'?: model.SchemaLifecycleState;
    /**
    * Deprecated field. Map of key-value pairs. This object will be only provided when the parent catalog is external.
    */
    'details'?: { [key: string]: string; };
    /**
    * The status for last refresh performed on schema.
    */
    'lastRefreshStatus'?: model.CrawlerLastRefreshStatus;
    /**
    * The timestamp for last refresh performed on schema.
    */
    'timeLastRefresh'?: Date;

   "entityType": string;
}

export namespace SchemaSummary {












    export function getJsonObj(obj: SchemaSummary): object {
        const jsonObj = {...obj, ...{
            











        }};

        
        
        if (obj && "entityType" in obj && obj.entityType) {
            switch (obj.entityType) {
                case "ALH":
                    return model.AlhSchemaSummary.getJsonObj(<model.AlhSchemaSummary>(<object>jsonObj), true);
                case "EXADATA":
                    return model.ExadataSchemaSummary.getJsonObj(<model.ExadataSchemaSummary>(<object>jsonObj), true);
                case "ORACLE":
                    return model.OracleSchemaSummary.getJsonObj(<model.OracleSchemaSummary>(<object>jsonObj), true);
                case "ORACLE_ANALYTICS":
                    return model.OacSchemaSummary.getJsonObj(<model.OacSchemaSummary>(<object>jsonObj), true);
                case "ATP":
                    return model.AtpSchemaSummary.getJsonObj(<model.AtpSchemaSummary>(<object>jsonObj), true);
                case "KAFKA_TOPIC":
                    return model.KafkaTopicSchemaSummary.getJsonObj(<model.KafkaTopicSchemaSummary>(<object>jsonObj), true);
                case "ADW":
                    return model.AdwSchemaSummary.getJsonObj(<model.AdwSchemaSummary>(<object>jsonObj), true);
                case "STANDARD":
                    return model.StandardSchemaSummary.getJsonObj(<model.StandardSchemaSummary>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.entityType}`)

        }
        }
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: SchemaSummary): object {
        const jsonObj = {...obj, ...{
            











         }};

        
        
        if (obj && "entityType" in obj && obj.entityType) {
            switch (obj.entityType) {
                case "ALH":
                    return model.AlhSchemaSummary.getDeserializedJsonObj(<model.AlhSchemaSummary>(<object>jsonObj), true);
                case "EXADATA":
                    return model.ExadataSchemaSummary.getDeserializedJsonObj(<model.ExadataSchemaSummary>(<object>jsonObj), true);
                case "ORACLE":
                    return model.OracleSchemaSummary.getDeserializedJsonObj(<model.OracleSchemaSummary>(<object>jsonObj), true);
                case "ORACLE_ANALYTICS":
                    return model.OacSchemaSummary.getDeserializedJsonObj(<model.OacSchemaSummary>(<object>jsonObj), true);
                case "ATP":
                    return model.AtpSchemaSummary.getDeserializedJsonObj(<model.AtpSchemaSummary>(<object>jsonObj), true);
                case "KAFKA_TOPIC":
                    return model.KafkaTopicSchemaSummary.getDeserializedJsonObj(<model.KafkaTopicSchemaSummary>(<object>jsonObj), true);
                case "ADW":
                    return model.AdwSchemaSummary.getDeserializedJsonObj(<model.AdwSchemaSummary>(<object>jsonObj), true);
                case "STANDARD":
                    return model.StandardSchemaSummary.getDeserializedJsonObj(<model.StandardSchemaSummary>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.entityType}`)
        }
        }
        return jsonObj;
    }
}
