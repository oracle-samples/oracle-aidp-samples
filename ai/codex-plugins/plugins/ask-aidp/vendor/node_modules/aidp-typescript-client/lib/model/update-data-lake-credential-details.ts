// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The set of details required to update an existing credential object.
*/
export interface UpdateDataLakeCredentialDetails {
    /**
    * A user-friendly name for the credential object. This value does not have to be unique and can be changed. Must start with a letter and contain only letters, numbers, or underscores. Avoid entering confidential information.
    */
    'displayName'?: string;
    /**
    * A brief summary of the credential object and its purpose.
    */
    'credentialDescription'?: string;
    /**
    * The type of credential stored. Allowed values are defined in CredentialType.
    */
    'type'?: model.CredentialType;
    'credentialDetails'?: model.ServiceAccountCredentialDetails| model.SecretTokenCredentialDetails| model.VaultReferenceCredentialDetails;

}

export namespace UpdateDataLakeCredentialDetails {





    export function getJsonObj(obj: UpdateDataLakeCredentialDetails): object {
        const jsonObj = {...obj, ...{
            



                'credentialDetails': obj.credentialDetails ?
                
                
                model.CredentialDetails.getJsonObj(obj.credentialDetails) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateDataLakeCredentialDetails): object {
        const jsonObj = {...obj, ...{
            



                    'credentialDetails': obj.credentialDetails ?
                
                
                model.CredentialDetails.getDeserializedJsonObj(obj.credentialDetails) : undefined,
         }};

        
        
        return jsonObj;
    }
}
