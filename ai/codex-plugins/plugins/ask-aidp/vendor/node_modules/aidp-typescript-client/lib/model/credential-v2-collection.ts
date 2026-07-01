// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Result of CredentialV2Collection.
*/
export interface CredentialV2Collection {
    /**
    * List of CredentialV2 summaries.
    */
    'items': Array<model.CredentialV2Summary>;

}

export namespace CredentialV2Collection {


    export function getJsonObj(obj: CredentialV2Collection): object {
        const jsonObj = {...obj, ...{
            
                'items': obj.items ?
                
                obj.items.map((item)=>{return model.CredentialV2Summary.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CredentialV2Collection): object {
        const jsonObj = {...obj, ...{
            
                    'items': obj.items ?
                
                obj.items.map((item)=>{return model.CredentialV2Summary.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
