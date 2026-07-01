// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The information about a source to be delete from a KnowledgeBase
*/
export interface UpdateKnowledgeBaseDeleteSourceDetails {
    /**
    * name for source
    */
    'name': string;
    /**
    * The type of source
    */
    'type': model.KnowledgeBaseSourceType;

}

export namespace UpdateKnowledgeBaseDeleteSourceDetails {



    export function getJsonObj(obj: UpdateKnowledgeBaseDeleteSourceDetails): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateKnowledgeBaseDeleteSourceDetails): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
