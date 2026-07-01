// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Request body to create a new bundle.
*/
export interface CreateBundleDetails {
    /**
    * Name for the new bundle.
    */
    'name': string;
    /**
    * Description for the bundle.
    */
    'description'?: string;
    /**
    * Target folder for the new bundle.
    */
    'path': string;
    /**
    * List of workspace resource descriptors (jobs, agentflows) to be included.
* Each has minimally resourceType and resourceKey.
* 
    */
    'bundledResources': Array<model.BundledResource>;

}

export namespace CreateBundleDetails {





    export function getJsonObj(obj: CreateBundleDetails): object {
        const jsonObj = {...obj, ...{
            



                'bundledResources': obj.bundledResources ?
                
                obj.bundledResources.map((item)=>{return model.BundledResource.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CreateBundleDetails): object {
        const jsonObj = {...obj, ...{
            



                    'bundledResources': obj.bundledResources ?
                
                obj.bundledResources.map((item)=>{return model.BundledResource.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
