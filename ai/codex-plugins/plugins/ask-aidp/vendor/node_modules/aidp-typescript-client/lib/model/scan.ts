// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Single Client Access Name (SCAN) is the object with a fully-qualified domain name and a port number.
*/
export interface Scan {
    /**
    * A fully-qualified domain name (FQDN).
    */
    'fqdn'?: string;
    /**
    * Port number of the FQDN.
    */
    'port'?: string;

}

export namespace Scan {



    export function getJsonObj(obj: Scan): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: Scan): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
