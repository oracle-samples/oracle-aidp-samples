// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The base object containing credential information, extended by specific credential types 
* such as SecretTokenCredentialDetails or VaultReferenceCredentialDetails. The type of credential
* is identified by the {@code credentialType} property.
* 
*/
export interface CredentialDetails {

   "credentialType": string;
}

export namespace CredentialDetails {

    export function getJsonObj(obj: CredentialDetails): object {
        const jsonObj = {...obj, ...{
            
        }};

        
        
        if (obj && "credentialType" in obj && obj.credentialType) {
            switch (obj.credentialType) {
                case "SERVICE_ACCOUNT":
                    return model.ServiceAccountCredentialDetails.getJsonObj(<model.ServiceAccountCredentialDetails>(<object>jsonObj), true);
                case "SECRET_TOKEN":
                    return model.SecretTokenCredentialDetails.getJsonObj(<model.SecretTokenCredentialDetails>(<object>jsonObj), true);
                case "VAULT_REFERENCE":
                    return model.VaultReferenceCredentialDetails.getJsonObj(<model.VaultReferenceCredentialDetails>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.credentialType}`)

        }
        }
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CredentialDetails): object {
        const jsonObj = {...obj, ...{
            
         }};

        
        
        if (obj && "credentialType" in obj && obj.credentialType) {
            switch (obj.credentialType) {
                case "SERVICE_ACCOUNT":
                    return model.ServiceAccountCredentialDetails.getDeserializedJsonObj(<model.ServiceAccountCredentialDetails>(<object>jsonObj), true);
                case "SECRET_TOKEN":
                    return model.SecretTokenCredentialDetails.getDeserializedJsonObj(<model.SecretTokenCredentialDetails>(<object>jsonObj), true);
                case "VAULT_REFERENCE":
                    return model.VaultReferenceCredentialDetails.getDeserializedJsonObj(<model.VaultReferenceCredentialDetails>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.credentialType}`)
        }
        }
        return jsonObj;
    }
}
