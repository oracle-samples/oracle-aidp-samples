// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Payload required to export task run output content to a file.
*/
export interface ExportTaskRunOutputDetails {
    /**
    * The format in which task run output should be exported, supported formats are HTML and ipynb only.
    */
    'format'?: ExportTaskRunOutputDetails.Format;

}

export namespace ExportTaskRunOutputDetails {

    export enum Format {
    
    Html = "HTML",
    Ipynb = "IPYNB"

}


    export function getJsonObj(obj: ExportTaskRunOutputDetails): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ExportTaskRunOutputDetails): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
