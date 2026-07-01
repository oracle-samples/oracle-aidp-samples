// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Represents a key-value pair for a secret, consisting of a secret key and its corresponding value.
*/
export interface SecretPair {
    /**
    * The secret key. The minimum length is 1 character and the maximum is 255 characters.
    */
    'secretKey': string;
    /**
    * The secret value. The minimum length is 1 character and the maximum is 1 MB.
    */
    'secretValue': string;

}

export namespace SecretPair {



    export function getJsonObj(obj: SecretPair): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: SecretPair): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
