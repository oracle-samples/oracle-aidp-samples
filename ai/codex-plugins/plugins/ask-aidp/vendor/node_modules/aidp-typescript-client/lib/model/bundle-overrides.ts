// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Override candidates grouped by dependency for compute and aicompute.
*/
export interface BundleOverrides {
    /**
    * Compute override candidates grouped by dependency.
    */
    'compute': Array<model.ComputeOverrideItem>;
    /**
    * Aicompute override candidates grouped by dependency.
    */
    'aicompute': Array<model.AiComputeOverrideItem>;

}

export namespace BundleOverrides {



    export function getJsonObj(obj: BundleOverrides): object {
        const jsonObj = {...obj, ...{
            
                'compute': obj.compute ?
                
                obj.compute.map((item)=>{return model.ComputeOverrideItem.getJsonObj(item)})
                
                 : undefined,
                'aicompute': obj.aicompute ?
                
                obj.aicompute.map((item)=>{return model.AiComputeOverrideItem.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: BundleOverrides): object {
        const jsonObj = {...obj, ...{
            
                    'compute': obj.compute ?
                
                obj.compute.map((item)=>{return model.ComputeOverrideItem.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'aicompute': obj.aicompute ?
                
                obj.aicompute.map((item)=>{return model.AiComputeOverrideItem.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
