// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Request body for creating a new job run for a job in a KnowledgeBase.
*/
export interface CreateKnowledgeBaseJobRunDetails {
    /**
    * The description of KB Job
    */
    'description'?: string;

}

export namespace CreateKnowledgeBaseJobRunDetails {


    export function getJsonObj(obj: CreateKnowledgeBaseJobRunDetails): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CreateKnowledgeBaseJobRunDetails): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
