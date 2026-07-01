// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Dataset. Represents a reference to data used for training, testing, or evaluation during the model development process.
*/
export interface Dataset {
    /**
    * Name of the dataset.
    */
    'name': string;
    /**
    * Digest (hash) of the dataset.
    */
    'digest': string;
    /**
    * Type of the dataset source.
    */
    'sourceType': string;
    /**
    * URI or path to the dataset source.
    */
    'source': string;
    /**
    * Schema of the dataset.
    */
    'schema'?: string;
    /**
    * Profile of the dataset.
    */
    'profile'?: string;

}

export namespace Dataset {







    export function getJsonObj(obj: Dataset): object {
        const jsonObj = {...obj, ...{
            


                'source_type': obj.sourceType,




        }};

        delete (jsonObj as Partial<Dataset>).sourceType;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: Dataset): object {
        const jsonObj = {...obj, ...{
            


                'sourceType': (obj as any)["source_type"],




         }};

        delete (jsonObj as any)["source_type"];
        
        return jsonObj;
    }
}
