// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* DatasetInput
*/
export interface DatasetInput {
    'dataset': model.Dataset;
    /**
    * Tags for the dataset input.
    */
    'tags'?: Array<model.InputTag>;

}

export namespace DatasetInput {



    export function getJsonObj(obj: DatasetInput): object {
        const jsonObj = {...obj, ...{
            
                'dataset': obj.dataset ?
                
                
                model.Dataset.getJsonObj(obj.dataset) : undefined,
                'tags': obj.tags ?
                
                obj.tags.map((item)=>{return model.InputTag.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: DatasetInput): object {
        const jsonObj = {...obj, ...{
            
                    'dataset': obj.dataset ?
                
                
                model.Dataset.getDeserializedJsonObj(obj.dataset) : undefined,
                    'tags': obj.tags ?
                
                obj.tags.map((item)=>{return model.InputTag.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
