// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Cluster library details to update.
*/
export interface PatchClusterLibraryDetails {
    /**
    * List of library changes to make.
    */
    'items': Array<model.ClusterLibraryDetails>;

}

export namespace PatchClusterLibraryDetails {


    export function getJsonObj(obj: PatchClusterLibraryDetails): object {
        const jsonObj = {...obj, ...{
            
                'items': obj.items ?
                
                obj.items.map((item)=>{return model.ClusterLibraryDetails.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: PatchClusterLibraryDetails): object {
        const jsonObj = {...obj, ...{
            
                    'items': obj.items ?
                
                obj.items.map((item)=>{return model.ClusterLibraryDetails.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
