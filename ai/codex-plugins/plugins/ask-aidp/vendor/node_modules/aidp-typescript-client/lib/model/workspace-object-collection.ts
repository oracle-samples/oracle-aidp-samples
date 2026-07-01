// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Results of a workspace object list.
*/
export interface WorkspaceObjectCollection {
    /**
    * List of workspace objects.
    */
    'items': Array<model.WorkspaceObjectSummary>;

}

export namespace WorkspaceObjectCollection {


    export function getJsonObj(obj: WorkspaceObjectCollection): object {
        const jsonObj = {...obj, ...{
            
                'items': obj.items ?
                
                obj.items.map((item)=>{return model.WorkspaceObjectSummary.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: WorkspaceObjectCollection): object {
        const jsonObj = {...obj, ...{
            
                    'items': obj.items ?
                
                obj.items.map((item)=>{return model.WorkspaceObjectSummary.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
