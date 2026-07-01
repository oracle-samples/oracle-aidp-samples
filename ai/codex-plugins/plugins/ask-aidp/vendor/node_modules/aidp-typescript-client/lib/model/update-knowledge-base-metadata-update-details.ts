// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The information about a source metad
*/
export interface UpdateKnowledgeBaseMetadataUpdateDetails {
    /**
    * new name for source
    */
    'name'?: string;
    /**
    * new description for source
    */
    'description'?: string;

}

export namespace UpdateKnowledgeBaseMetadataUpdateDetails {



    export function getJsonObj(obj: UpdateKnowledgeBaseMetadataUpdateDetails): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateKnowledgeBaseMetadataUpdateDetails): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
