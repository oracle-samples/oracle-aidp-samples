// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Description of KB Job Run Statistics.
*/
export interface KnowledgeBaseJobRunData {
    /**
    * Number of records/files added as part of this job run Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'recordsAdded'?: number;
    /**
    * Number of records/files deleted as part of this job run Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'recordsDeleted'?: number;
    /**
    * Number of records/files updated as part of this job run Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'recordsUpdated'?: number;
    /**
    * Size of all the records/files processed as part of this job run Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'recordsAddedUpdatedSize'?: number;
    /**
    * Hint for how job run is getting started.
    */
    'triggerType'?: model.KnowledgeBaseJobRunTriggerType;

}

export namespace KnowledgeBaseJobRunData {






    export function getJsonObj(obj: KnowledgeBaseJobRunData): object {
        const jsonObj = {...obj, ...{
            





        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: KnowledgeBaseJobRunData): object {
        const jsonObj = {...obj, ...{
            





         }};

        
        
        return jsonObj;
    }
}
