// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Schema in data catalogs are constructs to organize data. Schema can contain tables, which contain structured data,
* and volumes, which contain unstructured data. A default schema is created in all standard catalogs created in the
* Master Catalog. To use any of the API operations, you must be authorized in an IAM policy. If you're not authorized, talk to
* an administrator. If you're an administrator who needs to write policies to give users access, see
* <a href=\"https://docs.oracle.com/en/cloud/paas/ai-data-platform/aidug/iam-policies-oracle-ai-data-platform.html\" target=\"_blank\" rel=\"noopener noreferrer\">IAM Policies for Oracle AI Data Platform Workbench</a>.
* 
*/
export interface Schema {
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
    * Key-value pair representing a defined tag key and value.
    */
    'properties'?: { [key: string]: string; };
    /**
    * The name of the catalog to which this schema belongs.
    */
    'catalogName'?: string;
    /**
    * The current state of the schema.
    */
    'lifecycleState'?: model.SchemaLifecycleState;
    /**
    * A message describing the current state in more detail. For example, it can be used to provide actionable information for a resource in Failed state.
    */
    'lifecycleStateDetails'?: string;
    /**
    * Deprecated field. Map of key-value pairs. This object will be only provided when the parent catalog is external.
    */
    'details'?: { [key: string]: string; };

   "entityType": string;
}

export namespace Schema {













    export function getJsonObj(obj: Schema): object {
        const jsonObj = {...obj, ...{
            












        }};

        
        
        if (obj && "entityType" in obj && obj.entityType) {
            switch (obj.entityType) {
                case "ORACLE":
                    return model.OracleSchema.getJsonObj(<model.OracleSchema>(<object>jsonObj), true);
                case "ALH":
                    return model.AlhSchema.getJsonObj(<model.AlhSchema>(<object>jsonObj), true);
                case "ADW":
                    return model.AdwSchema.getJsonObj(<model.AdwSchema>(<object>jsonObj), true);
                case "KAFKA_TOPIC":
                    return model.KafkaTopicSchema.getJsonObj(<model.KafkaTopicSchema>(<object>jsonObj), true);
                case "ATP":
                    return model.AtpSchema.getJsonObj(<model.AtpSchema>(<object>jsonObj), true);
                case "STANDARD":
                    return model.StandardSchema.getJsonObj(<model.StandardSchema>(<object>jsonObj), true);
                case "ORACLE_ANALYTICS":
                    return model.OacSchema.getJsonObj(<model.OacSchema>(<object>jsonObj), true);
                case "EXADATA":
                    return model.ExadataSchema.getJsonObj(<model.ExadataSchema>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.entityType}`)

        }
        }
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: Schema): object {
        const jsonObj = {...obj, ...{
            












         }};

        
        
        if (obj && "entityType" in obj && obj.entityType) {
            switch (obj.entityType) {
                case "ORACLE":
                    return model.OracleSchema.getDeserializedJsonObj(<model.OracleSchema>(<object>jsonObj), true);
                case "ALH":
                    return model.AlhSchema.getDeserializedJsonObj(<model.AlhSchema>(<object>jsonObj), true);
                case "ADW":
                    return model.AdwSchema.getDeserializedJsonObj(<model.AdwSchema>(<object>jsonObj), true);
                case "KAFKA_TOPIC":
                    return model.KafkaTopicSchema.getDeserializedJsonObj(<model.KafkaTopicSchema>(<object>jsonObj), true);
                case "ATP":
                    return model.AtpSchema.getDeserializedJsonObj(<model.AtpSchema>(<object>jsonObj), true);
                case "STANDARD":
                    return model.StandardSchema.getDeserializedJsonObj(<model.StandardSchema>(<object>jsonObj), true);
                case "ORACLE_ANALYTICS":
                    return model.OacSchema.getDeserializedJsonObj(<model.OacSchema>(<object>jsonObj), true);
                case "EXADATA":
                    return model.ExadataSchema.getDeserializedJsonObj(<model.ExadataSchema>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.entityType}`)
        }
        }
        return jsonObj;
    }
}
