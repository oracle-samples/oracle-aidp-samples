// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Descriptor for a workspace resource (Job/Agentflow) to include in a bundle.
*/
export interface BundledResource {
    /**
    * Type of resource (job or agentflow).
    */
    'resourceType': BundledResource.ResourceType;
    /**
    * Workspace unique key for the resource.
    */
    'resourceKey': string;

}

export namespace BundledResource {

    export enum ResourceType {
    
    Job = "JOB",
    Agentflow = "AGENTFLOW"

}



    export function getJsonObj(obj: BundledResource): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: BundledResource): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
