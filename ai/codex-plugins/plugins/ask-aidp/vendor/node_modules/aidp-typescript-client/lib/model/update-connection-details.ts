// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details of a connection associated with a catalog.
*/
export interface UpdateConnectionDetails {
    /**
    * Connection properties.
    */
    'connectionProperties': { [key: string]: string; };

}

export namespace UpdateConnectionDetails {


    export function getJsonObj(obj: UpdateConnectionDetails): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateConnectionDetails): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
