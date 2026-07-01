// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Result of listing experiment runs.
*/
export interface ExperimentRunCollection {
    /**
    * Runs that match the search criteria.
    */
    'runs': Array<model.ExperimentRun>;
    /**
    * Token that can be used to retrieve the next page of runs. An empty token means that no more runs are available for retrieval.
    */
    'nextPageToken'?: string;

}

export namespace ExperimentRunCollection {



    export function getJsonObj(obj: ExperimentRunCollection): object {
        const jsonObj = {...obj, ...{
            
                'runs': obj.runs ?
                
                obj.runs.map((item)=>{return model.ExperimentRun.getJsonObj(item)})
                
                 : undefined,
                'next_page_token': obj.nextPageToken,

        }};

        delete (jsonObj as Partial<ExperimentRunCollection>).nextPageToken;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ExperimentRunCollection): object {
        const jsonObj = {...obj, ...{
            
                    'runs': obj.runs ?
                
                obj.runs.map((item)=>{return model.ExperimentRun.getDeserializedJsonObj(item)})
                
                 : undefined,
                'nextPageToken': (obj as any)["next_page_token"],

         }};

        delete (jsonObj as any)["next_page_token"];
        
        return jsonObj;
    }
}
