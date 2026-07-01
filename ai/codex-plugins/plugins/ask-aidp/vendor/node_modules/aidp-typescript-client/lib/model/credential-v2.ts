// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Represents a credential object, which holds and manages credential resources.
*/
export interface CredentialV2 {
    /**
    * The unique identifier for the credential object.
    */
    'key': string;
    /**
    * A user-friendly name for the credential object. This value does not have to be unique and can be changed. Must start with a letter and contain only letters, numbers, or underscores. Avoid entering confidential information.
    */
    'displayName': string;
    /**
    * The type of credential stored. Allowed values are defined in CredentialType.
    */
    'type'?: model.CredentialType;
    'credentialDetails'?: model.ServiceAccountCredentialDetails| model.SecretTokenCredentialDetails| model.VaultReferenceCredentialDetails;
    /**
    * A brief summary of the credential object and its purpose.
    */
    'credentialDescription'?: string;
    /**
    * The current state of the credential object. Allowed values are defined in CredentialLifeCycleState.
    */
    'lifecycleState'?: model.CredentialV2LifeCycleState;
    /**
    * Additional details or reasons regarding the current lifecycle state. Often used to provide actionable information (e.g., for resources in a Failed state).
    */
    'lifecycleStateDetails'?: string;
    /**
    * The date and time when the credential object was created, in RFC 3339 timestamp format.
    */
    'timeCreated'?: Date;
    /**
    * The date and time when the credential object was most recently updated, in RFC 3339 timestamp format.
    */
    'timeUpdated'?: Date;
    /**
    * The unique identifier of the user who created the credential object.
    */
    'createdBy'?: string;
    /**
    * The unique identifier of the user who last updated the credential object.
    */
    'updatedBy'?: string;

}

export namespace CredentialV2 {












    export function getJsonObj(obj: CredentialV2): object {
        const jsonObj = {...obj, ...{
            



                'credentialDetails': obj.credentialDetails ?
                
                
                model.CredentialDetails.getJsonObj(obj.credentialDetails) : undefined,







        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CredentialV2): object {
        const jsonObj = {...obj, ...{
            



                    'credentialDetails': obj.credentialDetails ?
                
                
                model.CredentialDetails.getDeserializedJsonObj(obj.credentialDetails) : undefined,







         }};

        
        
        return jsonObj;
    }
}
