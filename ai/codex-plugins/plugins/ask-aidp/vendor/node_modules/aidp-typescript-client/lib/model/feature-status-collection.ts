// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Model representing the response for checking the statuses of features.
*/
export interface FeatureStatusCollection {
    /**
    * List of features.
    */
    'items': Array<model.FeatureStatusSummary>;

}

export namespace FeatureStatusCollection {


    export function getJsonObj(obj: FeatureStatusCollection): object {
        const jsonObj = {...obj, ...{
            
                'items': obj.items ?
                
                obj.items.map((item)=>{return model.FeatureStatusSummary.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: FeatureStatusCollection): object {
        const jsonObj = {...obj, ...{
            
                    'items': obj.items ?
                
                obj.items.map((item)=>{return model.FeatureStatusSummary.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
