// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Collection of Git diff summaries for files under a folder context.
*/
export interface GitDiffSummaryCollection {
    /**
    * List of Git diff summaries.
    */
    'items': Array<model.GitDiffSummary>;

}

export namespace GitDiffSummaryCollection {


    export function getJsonObj(obj: GitDiffSummaryCollection): object {
        const jsonObj = {...obj, ...{
            
                'items': obj.items ?
                
                obj.items.map((item)=>{return model.GitDiffSummary.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: GitDiffSummaryCollection): object {
        const jsonObj = {...obj, ...{
            
                    'items': obj.items ?
                
                obj.items.map((item)=>{return model.GitDiffSummary.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
