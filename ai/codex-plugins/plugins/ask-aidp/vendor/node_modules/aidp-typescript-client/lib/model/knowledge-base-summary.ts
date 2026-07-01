// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Summary of the KnowledgeBase.
*/
export interface KnowledgeBaseSummary {
    /**
    * Immutable Unique identifier generated at creation
    */
    'key': string;
    /**
    * KnowledgeBase Identifier, can be renamed
    */
    'displayName': string;
    /**
    * the catalog hosting the KnowledgeBase
    */
    'catalogKey'?: string;
    /**
    * the schema inside the catalog hosting the KnowledgeBase
    */
    'schemaKey'?: string;
    /**
    * The description of KnowledgeBase.
    */
    'description'?: string;
    /**
    * The time at which KnowledgeBase was created. An RFC3339 formatted datetime string
    */
    'timeCreated': Date;
    /**
    * Identifier for KnowledgeBase creator
    */
    'createdBy'?: string;
    /**
    * The time at which KnowledgeBase was updated. An RFC3339 formatted datetime string
    */
    'timeUpdated'?: Date;
    /**
    * Identifier for principal who updated the KnowledgeBase
    */
    'updatedBy'?: string;
    /**
    * Count of items/documents processed by KB for which embeddings are present Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'currentProcessedItems'?: number;
    /**
    * The current state of the KnowledgeBase.
    */
    'lifecycleState': model.KnowledgeBaseLifecycleState;
    /**
    * System tags for this resource. Each key is predefined and scoped to a namespace.
* <p>
Example: {@code {\"orcl-cloud\": {\"free-tier-retained\": \"true\"}}}
* 
    */
    'systemTags'?: { [key: string]: { [key: string]: any; }; };

}

export namespace KnowledgeBaseSummary {













    export function getJsonObj(obj: KnowledgeBaseSummary): object {
        const jsonObj = {...obj, ...{
            












        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: KnowledgeBaseSummary): object {
        const jsonObj = {...obj, ...{
            












         }};

        
        
        return jsonObj;
    }
}
