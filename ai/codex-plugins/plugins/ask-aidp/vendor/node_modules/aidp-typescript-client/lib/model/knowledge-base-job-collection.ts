// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Results of a knowledgebaseJOb search. Contains both KnowledgeBaseJobSummary items and other data.
*/
export interface KnowledgeBaseJobCollection {
    /**
    * List of knowledgeBaseJob Summaries.
    */
    'items': Array<model.KnowledgeBaseJobSummary>;
    /**
    * token for next opc page.
    */
    'nextStartPage'?: string;

}

export namespace KnowledgeBaseJobCollection {



    export function getJsonObj(obj: KnowledgeBaseJobCollection): object {
        const jsonObj = {...obj, ...{
            
                'items': obj.items ?
                
                obj.items.map((item)=>{return model.KnowledgeBaseJobSummary.getJsonObj(item)})
                
                 : undefined,

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: KnowledgeBaseJobCollection): object {
        const jsonObj = {...obj, ...{
            
                    'items': obj.items ?
                
                obj.items.map((item)=>{return model.KnowledgeBaseJobSummary.getDeserializedJsonObj(item)})
                
                 : undefined,

         }};

        
        
        return jsonObj;
    }
}
