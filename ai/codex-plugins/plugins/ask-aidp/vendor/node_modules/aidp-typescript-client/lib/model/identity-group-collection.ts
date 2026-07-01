// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A collection of tenancy groups.
*/
export interface IdentityGroupCollection {
    /**
    * List of tenancy groups.
    */
    'items': Array<model.IdentityGroupSummary>;

}

export namespace IdentityGroupCollection {


    export function getJsonObj(obj: IdentityGroupCollection): object {
        const jsonObj = {...obj, ...{
            
                'items': obj.items ?
                
                obj.items.map((item)=>{return model.IdentityGroupSummary.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: IdentityGroupCollection): object {
        const jsonObj = {...obj, ...{
            
                    'items': obj.items ?
                
                obj.items.map((item)=>{return model.IdentityGroupSummary.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
