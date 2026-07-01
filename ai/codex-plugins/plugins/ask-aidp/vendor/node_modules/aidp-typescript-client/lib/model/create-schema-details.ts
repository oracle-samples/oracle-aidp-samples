// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The data to create a schema.
*/
export interface CreateSchemaDetails {
    /**
    * Schema name.
    */
    'displayName': string;
    /**
    * Schema description.
    */
    'description'?: string;
    /**
    * Key-value pair representing a defined tag key and value.
    */
    'properties'?: { [key: string]: string; };
    /**
    * The name of the catalog to which this schema belongs.
    */
    'catalogName': string;

}

export namespace CreateSchemaDetails {





    export function getJsonObj(obj: CreateSchemaDetails): object {
        const jsonObj = {...obj, ...{
            




        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CreateSchemaDetails): object {
        const jsonObj = {...obj, ...{
            




         }};

        
        
        return jsonObj;
    }
}
