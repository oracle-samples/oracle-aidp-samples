// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* List of share permissions.
*/
export interface SharePermissionCollection {
    /**
    * List of share permissions.
    */
    'items': Array<model.SharePermissionSummary>;

}

export namespace SharePermissionCollection {


    export function getJsonObj(obj: SharePermissionCollection): object {
        const jsonObj = {...obj, ...{
            
                'items': obj.items ?
                
                obj.items.map((item)=>{return model.SharePermissionSummary.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: SharePermissionCollection): object {
        const jsonObj = {...obj, ...{
            
                    'items': obj.items ?
                
                obj.items.map((item)=>{return model.SharePermissionSummary.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
