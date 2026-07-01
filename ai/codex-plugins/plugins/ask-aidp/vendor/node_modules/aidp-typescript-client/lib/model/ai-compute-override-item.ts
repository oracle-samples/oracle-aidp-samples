// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Aggregated override candidate for an aicompute dependency.
*/
export interface AiComputeOverrideItem {
    /**
    * Aicompute dependency name.
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
    * Top-level agentflows that reference this aicompute dependency.
    */
    'agentflows'?: Array<string>;

}

export namespace AiComputeOverrideItem {






    export function getJsonObj(obj: AiComputeOverrideItem): object {
        const jsonObj = {...obj, ...{
            





        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AiComputeOverrideItem): object {
        const jsonObj = {...obj, ...{
            





         }};

        
        
        return jsonObj;
    }
}
