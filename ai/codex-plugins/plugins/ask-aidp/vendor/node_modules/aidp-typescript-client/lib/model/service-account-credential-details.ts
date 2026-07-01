// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Credential details for representing service account object. This extends
* the base CredentialDetails object, with the credential type set as SERVICE_ACCOUNT.
* 
*/
export interface ServiceAccountCredentialDetails extends model.CredentialDetails {
    /**
    * The OCID of the user for the service account.
    */
    'userId': string;
    /**
    * The fingerprint of the service account's API key.
    */
    'fingerprint': string;
    /**
    * The OCID of the tenancy for the service account.
    */
    'tenancy': string;
    /**
    * The region for the service account (e.g., us-ashburn-1).
    */
    'region': string;
    /**
    * Whether the credentials are read-only.
    */
    'isReadOnly': boolean;
    /**
    * The private key associated with the service account.
    */
    'privateKey': string;

   "credentialType": string;
}

export namespace ServiceAccountCredentialDetails {







    export function getJsonObj(obj: ServiceAccountCredentialDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.CredentialDetails.getJsonObj(obj) as ServiceAccountCredentialDetails, ...{
            






        }};

        
        
        return jsonObj;
    }
    export const credentialType = 'SERVICE_ACCOUNT';
    export function getDeserializedJsonObj(obj: ServiceAccountCredentialDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.CredentialDetails.getDeserializedJsonObj(obj) as ServiceAccountCredentialDetails, ...{
            






         }};

        
        
        return jsonObj;
    }
}
