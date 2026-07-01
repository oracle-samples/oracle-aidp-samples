// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Tool configurations are set by the agent developer when they create the tool. | The agent does not see those configurations and can NOT modify their values
*/
export interface SqlToolConfiguration {
    /**
    * The Catalog to use for SQL query execution
    */
    'catalogKey'?: string;
    /**
    * The Schema to use for SQL query execution
    */
    'schemaKey'?: string;
    /**
    * The SQL query to execute
    */
    'query'?: string;
    /**
    * Whether to enforce the row limit when running the SQL tool
    */
    'isRowLimitEnabled'?: boolean;
    /**
    * Maximum number of result rows returned per SQL tool execution to prevent oversized result sets Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'maxRows'?: number;

}

export namespace SqlToolConfiguration {






    export function getJsonObj(obj: SqlToolConfiguration): object {
        const jsonObj = {...obj, ...{
            





        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: SqlToolConfiguration): object {
        const jsonObj = {...obj, ...{
            





         }};

        
        
        return jsonObj;
    }
}
