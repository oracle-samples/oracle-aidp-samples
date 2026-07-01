// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The summary of the Model.
*/
export interface ModelSummary {
    /**
    * A unique Id for the Model summary, that is immutable on creation.
    */
    'id': string;
    /**
    * A unique key for the Model summary, that is immutable on creation.
    */
    'modelName': string;
    /**
    * The Model summary name, it can be changed.
    */
    'displayName'?: string;
    /**
    * version that is available for that Model.
    */
    'modelVersion': string;
    /**
    * vendor name for that Model.
    */
    'vendor'?: string;
    /**
    * Possible modelTypes of Models object
    */
    'modelType': ModelSummary.ModelType;

}

export namespace ModelSummary {






    export enum ModelType {
    
    GenAi = "GEN_AI"

}


    export function getJsonObj(obj: ModelSummary): object {
        const jsonObj = {...obj, ...{
            






        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ModelSummary): object {
        const jsonObj = {...obj, ...{
            






         }};

        
        
        return jsonObj;
    }
}
