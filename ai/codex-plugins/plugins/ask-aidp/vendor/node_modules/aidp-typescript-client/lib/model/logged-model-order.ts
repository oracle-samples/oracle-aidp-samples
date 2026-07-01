// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* LoggedModelOrder.
*/
export interface LoggedModelOrder {
    /**
    * Field name. Allowed values are creation_time.
    */
    'fieldName': string;
    /**
    * Whether the order is ascending.
    */
    'ascending'?: boolean;

}

export namespace LoggedModelOrder {



    export function getJsonObj(obj: LoggedModelOrder): object {
        const jsonObj = {...obj, ...{
            
                'field_name': obj.fieldName,


        }};

        delete (jsonObj as Partial<LoggedModelOrder>).fieldName;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: LoggedModelOrder): object {
        const jsonObj = {...obj, ...{
            
                'fieldName': (obj as any)["field_name"],


         }};

        delete (jsonObj as any)["field_name"];
        
        return jsonObj;
    }
}
