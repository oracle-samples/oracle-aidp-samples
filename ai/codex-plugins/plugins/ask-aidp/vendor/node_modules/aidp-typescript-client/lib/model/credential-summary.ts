// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Summary view of a credential for public API consumers.
*/
export interface CredentialSummary {
    /**
    * The unique identifier for the credential object.
    */
    'key': string;
    /**
    * A user-friendly name for the credential object. This value does not have to be unique and can be changed. Must start with a letter and contain only letters, numbers, or underscores. Avoid entering confidential information.
    */
    'displayName': string;
    /**
    * A brief summary of the credential object and its purpose.
    */
    'description'?: string;
    /**
    * The type of credential stored. Allowed values are defined in CredentialType.
    */
    'credentialType': model.CredentialType;
    /**
    * The date and time when the credential object was created, in RFC 3339 timestamp format.
    */
    'timeCreated'?: Date;
    /**
    * The unique identifier of the user who created the credential object.
    */
    'createdBy'?: string;
    /**
    * The date and time when the credential object was most recently updated, in RFC 3339 timestamp format.
    */
    'timeUpdated'?: Date;
    /**
    * The unique identifier of the user who last updated the credential object.
    */
    'updatedBy'?: string;
    /**
    * The current state of the credential object. Allowed values are defined in CredentialLifecycleState.
    */
    'lifeCycleState'?: model.CredentialLifecycleState;

}

export namespace CredentialSummary {










    export function getJsonObj(obj: CredentialSummary): object {
        const jsonObj = {...obj, ...{
            









        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CredentialSummary): object {
        const jsonObj = {...obj, ...{
            









         }};

        
        
        return jsonObj;
    }
}
