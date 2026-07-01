// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* List of cluster libraries.
*/
export interface ClusterLibraryCollection {
    /**
    * List of cluster libraries.
    */
    'items': Array<model.ClusterLibrarySummary>;

}

export namespace ClusterLibraryCollection {


    export function getJsonObj(obj: ClusterLibraryCollection): object {
        const jsonObj = {...obj, ...{
            
                'items': obj.items ?
                
                obj.items.map((item)=>{return model.ClusterLibrarySummary.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ClusterLibraryCollection): object {
        const jsonObj = {...obj, ...{
            
                    'items': obj.items ?
                
                obj.items.map((item)=>{return model.ClusterLibrarySummary.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
