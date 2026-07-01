// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The airflow cell ID and the URL.
*/
export interface AirflowCellConfig {
    /**
    * The airflow cell ID.
    */
    'cellId'?: string;
    /**
    * The airflow cell URL.
    */
    'cellUrl'?: string;

}

export namespace AirflowCellConfig {



    export function getJsonObj(obj: AirflowCellConfig): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AirflowCellConfig): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
