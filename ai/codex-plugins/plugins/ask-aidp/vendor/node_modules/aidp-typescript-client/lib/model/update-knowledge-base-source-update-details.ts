// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The information to be updated for KnowledgeBase source.
*/
export interface UpdateKnowledgeBaseSourceUpdateDetails {
    /**
    * Batch Payload for updating KnowledgeBase sources. Items marked for deletion are processed before items marked for addition
    */
    'sources'?: Array<model.UpdateKnowledgeBaseSourceUpdateDetailsItem>;

}

export namespace UpdateKnowledgeBaseSourceUpdateDetails {


    export function getJsonObj(obj: UpdateKnowledgeBaseSourceUpdateDetails): object {
        const jsonObj = {...obj, ...{
            
                'sources': obj.sources ?
                
                obj.sources.map((item)=>{return model.UpdateKnowledgeBaseSourceUpdateDetailsItem.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateKnowledgeBaseSourceUpdateDetails): object {
        const jsonObj = {...obj, ...{
            
                    'sources': obj.sources ?
                
                obj.sources.map((item)=>{return model.UpdateKnowledgeBaseSourceUpdateDetailsItem.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
