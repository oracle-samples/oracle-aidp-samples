// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Result of listing experiments.
*/
export interface ExperimentCollection {
    /**
    * Experiments that match the search criteria.
    */
    'experiments': Array<model.Experiment>;
    /**
    * Token that can be used to retrieve the next page of experiments. An empty token means that no more experiments are available for retrieval.
    */
    'nextPageToken'?: string;

}

export namespace ExperimentCollection {



    export function getJsonObj(obj: ExperimentCollection): object {
        const jsonObj = {...obj, ...{
            
                'experiments': obj.experiments ?
                
                obj.experiments.map((item)=>{return model.Experiment.getJsonObj(item)})
                
                 : undefined,
                'next_page_token': obj.nextPageToken,

        }};

        delete (jsonObj as Partial<ExperimentCollection>).nextPageToken;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ExperimentCollection): object {
        const jsonObj = {...obj, ...{
            
                    'experiments': obj.experiments ?
                
                obj.experiments.map((item)=>{return model.Experiment.getDeserializedJsonObj(item)})
                
                 : undefined,
                'nextPageToken': (obj as any)["next_page_token"],

         }};

        delete (jsonObj as any)["next_page_token"];
        
        return jsonObj;
    }
}
