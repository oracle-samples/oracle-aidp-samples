// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Results of a listing Delta Shares assets. Contains summary information of shares assets.
*/
export interface ShareDataAssetCollection {
    /**
    * List of Shares assets.
    */
    'items': Array<model.ShareDataAssetSummary>;

}

export namespace ShareDataAssetCollection {


    export function getJsonObj(obj: ShareDataAssetCollection): object {
        const jsonObj = {...obj, ...{
            
                'items': obj.items ?
                
                obj.items.map((item)=>{return model.ShareDataAssetSummary.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ShareDataAssetCollection): object {
        const jsonObj = {...obj, ...{
            
                    'items': obj.items ?
                
                obj.items.map((item)=>{return model.ShareDataAssetSummary.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
