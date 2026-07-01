// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A collection of Identity domains.
*/
export interface IdentityDomainCollection {
    /**
    * List of Identity domains.
    */
    'items': Array<model.IdentityDomainSummary>;

}

export namespace IdentityDomainCollection {


    export function getJsonObj(obj: IdentityDomainCollection): object {
        const jsonObj = {...obj, ...{
            
                'items': obj.items ?
                
                obj.items.map((item)=>{return model.IdentityDomainSummary.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: IdentityDomainCollection): object {
        const jsonObj = {...obj, ...{
            
                    'items': obj.items ?
                
                obj.items.map((item)=>{return model.IdentityDomainSummary.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
