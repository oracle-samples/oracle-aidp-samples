// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Payload required to export contents of a file.
*/
export interface ExportContentsDetails {
    /**
    * The format in which contents should be exported, supported formats are HTML and ipynb only.
    */
    'format'?: ExportContentsDetails.Format;

}

export namespace ExportContentsDetails {

    export enum Format {
    
    Ipynb = "ipynb",
    Html = "html"

}


    export function getJsonObj(obj: ExportContentsDetails): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ExportContentsDetails): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
