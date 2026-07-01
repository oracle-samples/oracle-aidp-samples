// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Reference to a deployed resource.
*/
export interface BundleDeployedResource {
    /**
    * Type of resource
    */
    'type': BundleDeployedResource.Type;
    /**
    * Workspace unique key for the resource
    */
    'key'?: string;
    /**
    * Resource name
    */
    'name': string;

}

export namespace BundleDeployedResource {

    export enum Type {
    
    Job = "JOB",
    Agentflow = "AGENTFLOW",
    /**
    * This value is used if a service returns a value for this enum that is not recognized by this
    * version of the SDK.
    */
    UnknownValue = "UNKNOWN_VALUE"
}




    export function getJsonObj(obj: BundleDeployedResource): object {
        const jsonObj = {...obj, ...{
            



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: BundleDeployedResource): object {
        const jsonObj = {...obj, ...{
            



         }};

        
        
        return jsonObj;
    }
}
