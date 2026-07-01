// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Aggregated override candidate for a compute dependency.
*/
export interface ComputeOverrideItem {
    /**
    * Compute dependency name.
    */
    'name': string;
    /**
    * Variable name used for this override candidate.
    */
    'variableName': string;
    /**
    * Canonical dependency token using {@code .key}.
    */
    'defaultValue': string;
    /**
    * Current override value resolved from {@code .aidp/overrides.yaml} if present.
    */
    'overrideValue': string;
    /**
    * Top-level jobs that reference this compute dependency.
    */
    'jobs'?: Array<string>;

}

export namespace ComputeOverrideItem {






    export function getJsonObj(obj: ComputeOverrideItem): object {
        const jsonObj = {...obj, ...{
            





        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ComputeOverrideItem): object {
        const jsonObj = {...obj, ...{
            





         }};

        
        
        return jsonObj;
    }
}
