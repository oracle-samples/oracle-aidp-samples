// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Collection of commits for a branch (optionally filtered by folder).
*/
export interface HistoryCollection {
    /**
    * List of commit summaries.
    */
    'items': Array<model.HistorySummary>;
    /**
    * The Git repository URL corresponding to the branch.
    */
    'gitUrl'?: string;

}

export namespace HistoryCollection {



    export function getJsonObj(obj: HistoryCollection): object {
        const jsonObj = {...obj, ...{
            
                'items': obj.items ?
                
                obj.items.map((item)=>{return model.HistorySummary.getJsonObj(item)})
                
                 : undefined,

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: HistoryCollection): object {
        const jsonObj = {...obj, ...{
            
                    'items': obj.items ?
                
                obj.items.map((item)=>{return model.HistorySummary.getDeserializedJsonObj(item)})
                
                 : undefined,

         }};

        
        
        return jsonObj;
    }
}
