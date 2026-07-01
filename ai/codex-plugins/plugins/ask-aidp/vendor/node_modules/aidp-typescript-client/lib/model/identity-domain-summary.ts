// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A domain in the tenancy.
*/
export interface IdentityDomainSummary {
    /**
    * The ID of the domain.
    */
    'domainId'?: string;
    /**
    * The name of the domain.
    */
    'domainName'?: string;
    /**
    * The current state of the domain in its lifecycle.
    */
    'lifecycleState'?: IdentityDomainSummary.LifecycleState;
    /**
    * The primary region-specific URL for accessing the domain.
    */
    'homeRegionUrl'?: string;

}

export namespace IdentityDomainSummary {



    export enum LifecycleState {
    
    Creating = "CREATING",
    Active = "ACTIVE",
    Deleting = "DELETING",
    Inactive = "INACTIVE"

}



    export function getJsonObj(obj: IdentityDomainSummary): object {
        const jsonObj = {...obj, ...{
            




        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: IdentityDomainSummary): object {
        const jsonObj = {...obj, ...{
            




         }};

        
        
        return jsonObj;
    }
}
