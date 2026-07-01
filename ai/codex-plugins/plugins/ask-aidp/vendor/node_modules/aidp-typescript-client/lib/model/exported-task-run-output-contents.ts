// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* This model represents a file with contents, format, and other details.
* 
*/
export interface ExportedTaskRunOutputContents {
    /**
    * Name of file, equivalent to the last part of the path.
    */
    'name': string;
    /**
    * Full path of the file.
    */
    'path': string;
    /**
    * For ipynb format, this contains a string representing the .ipynb file.
* For HTML format, this contains a string representing the HTML page. It has base64-encoded data for images.
* 
    */
    'content': any;
    /**
    * Format of content as requested by the client. HTML format by default.
* 
    */
    'format': ExportedTaskRunOutputContents.Format;

}

export namespace ExportedTaskRunOutputContents {




    export enum Format {
    
    Html = "HTML",
    Ipynb = "IPYNB",
    /**
    * This value is used if a service returns a value for this enum that is not recognized by this
    * version of the SDK.
    */
    UnknownValue = "UNKNOWN_VALUE"
}


    export function getJsonObj(obj: ExportedTaskRunOutputContents): object {
        const jsonObj = {...obj, ...{
            




        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ExportedTaskRunOutputContents): object {
        const jsonObj = {...obj, ...{
            




         }};

        
        
        return jsonObj;
    }
}
