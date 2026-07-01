// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Schema details of log fields.
*/
export interface FieldInfo {
    /**
    * The name of the field.
    */
    'fieldName': string;
    /**
    * The type of the field.
    */
    'fieldType': FieldInfo.FieldType;

}

export namespace FieldInfo {


    export enum FieldType {
    
    String = "STRING",
    Number = "NUMBER",
    Boolean = "BOOLEAN",
    Array = "ARRAY",
    /**
    * This value is used if a service returns a value for this enum that is not recognized by this
    * version of the SDK.
    */
    UnknownValue = "UNKNOWN_VALUE"
}


    export function getJsonObj(obj: FieldInfo): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: FieldInfo): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
