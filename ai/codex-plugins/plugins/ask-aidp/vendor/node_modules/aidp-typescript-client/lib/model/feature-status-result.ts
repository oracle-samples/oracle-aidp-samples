// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Model representing the response for checking the status of a single feature.
*/
export interface FeatureStatusResult {
    'feature': model.Feature;

}

export namespace FeatureStatusResult {


    export function getJsonObj(obj: FeatureStatusResult): object {
        const jsonObj = {...obj, ...{
            
                'feature': obj.feature ?
                
                
                model.Feature.getJsonObj(obj.feature) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: FeatureStatusResult): object {
        const jsonObj = {...obj, ...{
            
                    'feature': obj.feature ?
                
                
                model.Feature.getDeserializedJsonObj(obj.feature) : undefined,
         }};

        
        
        return jsonObj;
    }
}
