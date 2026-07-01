// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Results of an endpoint search. Contains EndpointSummary items and other information such as metadata.
*/
export interface DacEndpointCollection {
    /**
    * List of endpoints.
    */
    'items': Array<model.DacEndpointSummary>;

}

export namespace DacEndpointCollection {


    export function getJsonObj(obj: DacEndpointCollection): object {
        const jsonObj = {...obj, ...{
            
                'items': obj.items ?
                
                obj.items.map((item)=>{return model.DacEndpointSummary.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: DacEndpointCollection): object {
        const jsonObj = {...obj, ...{
            
                    'items': obj.items ?
                
                obj.items.map((item)=>{return model.DacEndpointSummary.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
