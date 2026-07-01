// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The data to update a schema.
*/
export interface UpdateSchemaDetails {
    /**
    * Schema name.
    */
    'displayName'?: string;
    /**
    * Schema description.
    */
    'description'?: string;
    /**
    * Key-value pair representing a defined tag key and value.
    */
    'properties'?: { [key: string]: string; };

}

export namespace UpdateSchemaDetails {




    export function getJsonObj(obj: UpdateSchemaDetails): object {
        const jsonObj = {...obj, ...{
            



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateSchemaDetails): object {
        const jsonObj = {...obj, ...{
            



         }};

        
        
        return jsonObj;
    }
}
