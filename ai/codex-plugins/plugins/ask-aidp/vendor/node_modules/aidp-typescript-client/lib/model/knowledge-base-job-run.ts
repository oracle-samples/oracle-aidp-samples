// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Description of KnowledgeBase Job Run.
*/
export interface KnowledgeBaseJobRun {
    /**
    * The Unique identifier for this KnowledgeBase Job run
    */
    'key': string;
    /**
    * The name of the KnowledgeBase Job definition
    */
    'knowledgeBaseJobKey': string;
    /**
    * A user-friendly description about this KnowledgeBase Job run
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
    * Timestamp when KnowledgeBase job run was created
    */
    'timeCreated': Date;
    /**
    * Timestamp when KnowledgeBase job run was updated
    */
    'timeUpdated'?: Date;
    /**
    * Timestamp when KnowledgeBase job run was updated
    */
    'timeFinished'?: Date;
    /**
    * Identifier for KnowledgeBase job run creator
    */
    'createdBy': string;
    /**
    * Identifier for principal who updated the KnowledgeBase
    */
    'updatedBy'?: string;
    /**
    * Lifecycle of KnowledgeBase Job Run.
    */
    'lifecycleState'?: model.KnowledgeBaseJobRunLifecycleState;
    /**
    * Additional information about the current state of KnowledgeBase job run
    */
    'lifecycleStateDetails'?: string;
    'runData'?: model.KnowledgeBaseJobRunData;

}

export namespace KnowledgeBaseJobRun {















    export function getJsonObj(obj: KnowledgeBaseJobRun): object {
        const jsonObj = {...obj, ...{
            













                'runData': obj.runData ?
                
                
                model.KnowledgeBaseJobRunData.getJsonObj(obj.runData) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: KnowledgeBaseJobRun): object {
        const jsonObj = {...obj, ...{
            













                    'runData': obj.runData ?
                
                
                model.KnowledgeBaseJobRunData.getDeserializedJsonObj(obj.runData) : undefined,
         }};

        
        
        return jsonObj;
    }
}
