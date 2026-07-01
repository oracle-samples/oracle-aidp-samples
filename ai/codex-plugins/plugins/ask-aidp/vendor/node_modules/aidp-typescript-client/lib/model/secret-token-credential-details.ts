// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Credential details for storing secret tokens or API keys. This extends the base CredentialDetails
* object, with the credential type set as SECRET_TOKEN.
* 
*/
export interface SecretTokenCredentialDetails extends model.CredentialDetails {
    /**
    * A list of secret key-value pairs used as secret tokens or API keys.
    */
    'secretTokenPair': Array<model.SecretPair>;

   "credentialType": string;
}

export namespace SecretTokenCredentialDetails {


    export function getJsonObj(obj: SecretTokenCredentialDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.CredentialDetails.getJsonObj(obj) as SecretTokenCredentialDetails, ...{
            
                'secretTokenPair': obj.secretTokenPair ?
                
                obj.secretTokenPair.map((item)=>{return model.SecretPair.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    export const credentialType = 'SECRET_TOKEN';
    export function getDeserializedJsonObj(obj: SecretTokenCredentialDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.CredentialDetails.getDeserializedJsonObj(obj) as SecretTokenCredentialDetails, ...{
            
                    'secretTokenPair': obj.secretTokenPair ?
                
                obj.secretTokenPair.map((item)=>{return model.SecretPair.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
