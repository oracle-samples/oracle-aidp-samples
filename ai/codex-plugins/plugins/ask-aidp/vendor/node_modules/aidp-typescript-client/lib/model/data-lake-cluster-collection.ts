// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Results of a cluster list within the AI Data Platform Workbench.
*/
export interface DataLakeClusterCollection {
    /**
    * List of clusters within the AI Data Platform Workbench.
    */
    'items': Array<model.DataLakeClusterSummary>;

}

export namespace DataLakeClusterCollection {


    export function getJsonObj(obj: DataLakeClusterCollection): object {
        const jsonObj = {...obj, ...{
            
                'items': obj.items ?
                
                obj.items.map((item)=>{return model.DataLakeClusterSummary.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: DataLakeClusterCollection): object {
        const jsonObj = {...obj, ...{
            
                    'items': obj.items ?
                
                obj.items.map((item)=>{return model.DataLakeClusterSummary.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
