// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A single feature summary and its value.
*/
export interface FeatureStatusSummary {
    /**
    * The name of the queried feature.
    */
    'featureName': string;
    /**
    * The status value of the feature.
    */
    'value': string;
    /**
    * Error message, if error with featureName like unsupported.
    */
    'error'?: string;

}

export namespace FeatureStatusSummary {




    export function getJsonObj(obj: FeatureStatusSummary): object {
        const jsonObj = {...obj, ...{
            



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: FeatureStatusSummary): object {
        const jsonObj = {...obj, ...{
            



         }};

        
        
        return jsonObj;
    }
}
