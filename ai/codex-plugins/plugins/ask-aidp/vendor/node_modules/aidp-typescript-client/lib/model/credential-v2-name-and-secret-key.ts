// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Represents the name of a credential and an optional secret key name used when
* retrieving details via GetDataLakeCredentialByName.
* 
*/
export interface CredentialV2NameAndSecretKey {
    /**
    * The display name of the credential. Must start with a letter and contain only letters, numbers, or underscores.
    */
    'displayName': string;
    /**
    * The secret key name to filter SecretToken credentials.
    */
    'secretKey'?: string;

}

export namespace CredentialV2NameAndSecretKey {



    export function getJsonObj(obj: CredentialV2NameAndSecretKey): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CredentialV2NameAndSecretKey): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
