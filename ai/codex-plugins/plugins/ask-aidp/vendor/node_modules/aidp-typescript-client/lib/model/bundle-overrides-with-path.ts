// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Response payload for creating or updating bundle overrides.
*/
export interface BundleOverridesWithPath {
    /**
    * Target folder for the new bundle.
    */
    'path': string;
    'overrides': model.BundleOverrides;

}

export namespace BundleOverridesWithPath {



    export function getJsonObj(obj: BundleOverridesWithPath): object {
        const jsonObj = {...obj, ...{
            

                'overrides': obj.overrides ?
                
                
                model.BundleOverrides.getJsonObj(obj.overrides) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: BundleOverridesWithPath): object {
        const jsonObj = {...obj, ...{
            

                    'overrides': obj.overrides ?
                
                
                model.BundleOverrides.getDeserializedJsonObj(obj.overrides) : undefined,
         }};

        
        
        return jsonObj;
    }
}
