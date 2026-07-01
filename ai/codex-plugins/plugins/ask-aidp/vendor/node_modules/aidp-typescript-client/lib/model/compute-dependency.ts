// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Compute dependency. Represents a compute cluster resource.
*/
export interface ComputeDependency extends model.Dependency {
    /**
    * Unique identifier for compute resource.
    */
    'key': string;

   "type": string;
}

export namespace ComputeDependency {


    export function getJsonObj(obj: ComputeDependency, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Dependency.getJsonObj(obj) as ComputeDependency, ...{
            

        }};

        
        
        return jsonObj;
    }
    export const type = 'COMPUTE';
    export function getDeserializedJsonObj(obj: ComputeDependency, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Dependency.getDeserializedJsonObj(obj) as ComputeDependency, ...{
            

         }};

        
        
        return jsonObj;
    }
}
