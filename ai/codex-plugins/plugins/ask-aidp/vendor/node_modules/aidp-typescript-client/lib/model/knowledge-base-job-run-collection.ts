// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Collection of KnowledgeBase Job Run definitions.
*/
export interface KnowledgeBaseJobRunCollection {
    /**
    * Array of KnowledgeBase Runs.
    */
    'items': Array<model.KnowledgeBaseJobRunSummary>;

}

export namespace KnowledgeBaseJobRunCollection {


    export function getJsonObj(obj: KnowledgeBaseJobRunCollection): object {
        const jsonObj = {...obj, ...{
            
                'items': obj.items ?
                
                obj.items.map((item)=>{return model.KnowledgeBaseJobRunSummary.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: KnowledgeBaseJobRunCollection): object {
        const jsonObj = {...obj, ...{
            
                    'items': obj.items ?
                
                obj.items.map((item)=>{return model.KnowledgeBaseJobRunSummary.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
