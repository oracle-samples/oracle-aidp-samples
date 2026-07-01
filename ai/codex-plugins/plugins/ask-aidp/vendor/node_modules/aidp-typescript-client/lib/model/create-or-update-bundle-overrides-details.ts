// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Request payload for creating or updating bundle overrides.
*/
export interface CreateOrUpdateBundleOverridesDetails {
    /**
    * Target folder for the new bundle.
    */
    'path': string;
    'overrides': model.BundleOverrides;

}

export namespace CreateOrUpdateBundleOverridesDetails {



    export function getJsonObj(obj: CreateOrUpdateBundleOverridesDetails): object {
        const jsonObj = {...obj, ...{
            

                'overrides': obj.overrides ?
                
                
                model.BundleOverrides.getJsonObj(obj.overrides) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CreateOrUpdateBundleOverridesDetails): object {
        const jsonObj = {...obj, ...{
            

                    'overrides': obj.overrides ?
                
                
                model.BundleOverrides.getDeserializedJsonObj(obj.overrides) : undefined,
         }};

        
        
        return jsonObj;
    }
}
