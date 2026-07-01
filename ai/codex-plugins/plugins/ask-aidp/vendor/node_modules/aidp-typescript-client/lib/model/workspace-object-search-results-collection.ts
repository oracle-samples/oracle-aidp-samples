// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* List of Workspace objects
*/
export interface WorkspaceObjectSearchResultsCollection {
    /**
    * List of Workspace objects.
    */
    'items': Array<model.WorkspaceObjectSearchSummary>;

}

export namespace WorkspaceObjectSearchResultsCollection {


    export function getJsonObj(obj: WorkspaceObjectSearchResultsCollection): object {
        const jsonObj = {...obj, ...{
            
                'items': obj.items ?
                
                obj.items.map((item)=>{return model.WorkspaceObjectSearchSummary.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: WorkspaceObjectSearchResultsCollection): object {
        const jsonObj = {...obj, ...{
            
                    'items': obj.items ?
                
                obj.items.map((item)=>{return model.WorkspaceObjectSearchSummary.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
