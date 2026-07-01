// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* List of tool permissions.
*/
export interface ToolPermissionCollection {
    /**
    * List of tool permissions.
    */
    'items': Array<model.ToolPermissionSummary>;

}

export namespace ToolPermissionCollection {


    export function getJsonObj(obj: ToolPermissionCollection): object {
        const jsonObj = {...obj, ...{
            
                'items': obj.items ?
                
                obj.items.map((item)=>{return model.ToolPermissionSummary.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ToolPermissionCollection): object {
        const jsonObj = {...obj, ...{
            
                    'items': obj.items ?
                
                obj.items.map((item)=>{return model.ToolPermissionSummary.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
