// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Tool configurations are set by the agent developer when they create the tool. | The agent does not see those configurations and can NOT modify their values
*/
export interface NlToSqlToolConfiguration {
    /**
    * The Catalog to use for SQL query execution
    */
    'catalogKey': string;
    /**
    * The Schema to use for SQL query execution
    */
    'schemaKey': string;
    /**
    * The fully qualified table names to use in SQL query generation
    */
    'tables': Array<string>;
    /**
    * The fully qualified column names to use in SQL query generation
    */
    'columns': Array<string>;
    /**
    * Optional few-shot examples (NL \u2192 SQL pairs) for better generation.
    */
    'inContextLearning'?: string;
    /**
    * Additional instructions that is injected in the system prompt
    */
    'additionalInstructions'?: string;

}

export namespace NlToSqlToolConfiguration {







    export function getJsonObj(obj: NlToSqlToolConfiguration): object {
        const jsonObj = {...obj, ...{
            






        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: NlToSqlToolConfiguration): object {
        const jsonObj = {...obj, ...{
            






         }};

        
        
        return jsonObj;
    }
}
