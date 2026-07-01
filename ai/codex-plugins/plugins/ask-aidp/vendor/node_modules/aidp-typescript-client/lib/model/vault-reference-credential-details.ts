// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Credential details for referencing existing secrets managed outside AI Data Platform. This extends
* the base CredentialDetails object, with the credential type set as VAULT_REFERENCE.
* 
*/
export interface VaultReferenceCredentialDetails extends model.CredentialDetails {
    /**
    * The OCID of the external secret to reference.
    */
    'secretId': string;

   "credentialType": string;
}

export namespace VaultReferenceCredentialDetails {


    export function getJsonObj(obj: VaultReferenceCredentialDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.CredentialDetails.getJsonObj(obj) as VaultReferenceCredentialDetails, ...{
            

        }};

        
        
        return jsonObj;
    }
    export const credentialType = 'VAULT_REFERENCE';
    export function getDeserializedJsonObj(obj: VaultReferenceCredentialDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.CredentialDetails.getDeserializedJsonObj(obj) as VaultReferenceCredentialDetails, ...{
            

         }};

        
        
        return jsonObj;
    }
}
