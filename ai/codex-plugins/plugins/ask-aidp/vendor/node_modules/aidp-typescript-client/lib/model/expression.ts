// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Map of expressions with key as unique string and value as single expression.
*/
export interface Expression {
    /**
    * A unique character to map the expression. This character is used to refer the expression string in condition field of the task. May only contain alphanumeric characters.
    */
    'key': string;
    /**
    * Expression string.
    */
    'value': string;

}

export namespace Expression {



    export function getJsonObj(obj: Expression): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: Expression): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
