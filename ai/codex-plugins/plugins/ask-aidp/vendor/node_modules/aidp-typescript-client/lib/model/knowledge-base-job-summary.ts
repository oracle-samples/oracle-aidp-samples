// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Description of KB Job Definition.
*/
export interface KnowledgeBaseJobSummary {
    /**
    * The Unique identifier for this KnowledgeBase Job resource
    */
    'key': string;
    /**
    * A user-friendly name. Does not have to be unique, and it's changeable. Avoid entering confidential information.
    */
    'displayName': string;
    /**
    * A user-friendly description about this KnowledgeBase Job resource
    */
    'description'?: string;
    /**
    * The name of the catalog containing the KnowledgeBase.
    */
    'catalogKey': string;
    /**
    * The name of the schema containing the KnowledgeBase.
    */
    'schemaKey': string;
    /**
    * The name of the KnowledgeBase
    */
    'knowledgeBaseKey': string;
    /**
    * type of knowledgeBase Job Definition
    */
    'type': model.KnowledgeBaseJobType;
    /**
    * Name of the source, \"*\" for Default job
    */
    'sources'?: string;
    /**
    * Id of the source, should be provided for all jobs except DEFAULT JOB (which is supposed to run at all sources of KB)
    */
    'sourceKey'?: string;
    /**
    * Timestamp when knowledgeBase job was created
    */
    'timeCreated': Date;
    /**
    * Timestamp when knowledgeBase job was updated
    */
    'timeUpdated'?: Date;
    /**
    * Identifier for knowledgeBase job creator
    */
    'createdBy': string;
    /**
    * Identifier for principal who updated the knowledgeBase job
    */
    'updatedBy'?: string;
    /**
    * Lifecycle of knowledgeBase Job.
    */
    'lifecycleState'?: model.KnowledgeBaseJobLifecycleState;
    /**
    * Additional information about the current state of KB job
    */
    'lifecycleStateDetails'?: string;

}

export namespace KnowledgeBaseJobSummary {
















    export function getJsonObj(obj: KnowledgeBaseJobSummary): object {
        const jsonObj = {...obj, ...{
            















        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: KnowledgeBaseJobSummary): object {
        const jsonObj = {...obj, ...{
            















         }};

        
        
        return jsonObj;
    }
}
