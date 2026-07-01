// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Results of a knowledgebase search. Contains both KnowledgeBaseSummary items and other data.
*/
export interface KnowledgeBaseCollection {
    /**
    * List of knowledgeBase Summaries.
    */
    'items': Array<model.KnowledgeBaseSummary>;
    /**
    * token for next opc page.
    */
    'nextStartPage'?: string;

}

export namespace KnowledgeBaseCollection {



    export function getJsonObj(obj: KnowledgeBaseCollection): object {
        const jsonObj = {...obj, ...{
            
                'items': obj.items ?
                
                obj.items.map((item)=>{return model.KnowledgeBaseSummary.getJsonObj(item)})
                
                 : undefined,

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: KnowledgeBaseCollection): object {
        const jsonObj = {...obj, ...{
            
                    'items': obj.items ?
                
                obj.items.map((item)=>{return model.KnowledgeBaseSummary.getDeserializedJsonObj(item)})
                
                 : undefined,

         }};

        
        
        return jsonObj;
    }
}
