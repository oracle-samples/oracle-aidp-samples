// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The input schema definition of a NL to SQL Tool
*/
export interface NlToSqlToolInputSchema {
    /**
    * The user question to answer using relevant documents
    */
    'query': string;

}

export namespace NlToSqlToolInputSchema {


    export function getJsonObj(obj: NlToSqlToolInputSchema): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: NlToSqlToolInputSchema): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
