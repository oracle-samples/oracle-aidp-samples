// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Description of KB Job Definition.
*/
export interface KnowledgeBaseJob {
    /**
    * The Unique identifier for this KnowledgeBase Job resource
    */
    'key': string;
    /**
    * The Unique identifier for this KnowledgeBase Job resource's mapped WF job
    */
    'workflowJobKey'?: string;
    /**
    * A user-friendly name. Does not have to be unique, and it's changeable. Avoid entering confidential information.
    */
    'displayName': string;
    /**
    * A user-friendly description about this KnowledgeBase Job resource
    */
    'description'?: string;
    /**
    * The name of the KnowledgeBase
    */
    'knowledgeBaseKey': string;
    /**
    * The name of the catalog containing the KnowledgeBase.
    */
    'catalogKey': string;
    /**
    * The name of the schema containing the KnowledgeBase.
    */
    'schemaKey': string;
    /**
    * type of knowledgeBase Job Definition
    */
    'type': model.KnowledgeBaseJobType;
    /**
    * type of KB Job Goal
    */
    'goal'?: model.KnowledgeBaseJobGoalType;
    /**
    * Name of the source, \"*\" for Default job
    */
    'sources'?: string;
    /**
    * Id of the source, should be provided for all jobs except DEFAULT JOB (which is supposed to run at all sources of KB)
    */
    'sourceKey'?: string;
    /**
    * If the job type is SCHEDULED, this field is used to provide schedule information in cron style. For example, \"0 0 * * *\" means 12:00 AM daily
    */
    'schedule'?: string;
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

export namespace KnowledgeBaseJob {



















    export function getJsonObj(obj: KnowledgeBaseJob): object {
        const jsonObj = {...obj, ...{
            


















        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: KnowledgeBaseJob): object {
        const jsonObj = {...obj, ...{
            


















         }};

        
        
        return jsonObj;
    }
}
