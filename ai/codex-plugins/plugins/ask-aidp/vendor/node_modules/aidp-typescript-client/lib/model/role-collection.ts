// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The results of a role search. It contains RoleSummary objects.
*/
export interface RoleCollection {
    /**
    * The role summaries returned.
    */
    'items': Array<model.RoleSummary>;

}

export namespace RoleCollection {


    export function getJsonObj(obj: RoleCollection): object {
        const jsonObj = {...obj, ...{
            
                'items': obj.items ?
                
                obj.items.map((item)=>{return model.RoleSummary.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RoleCollection): object {
        const jsonObj = {...obj, ...{
            
                    'items': obj.items ?
                
                obj.items.map((item)=>{return model.RoleSummary.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
