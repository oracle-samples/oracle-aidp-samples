// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* This model represents a file with contents, format and other details.
* 
*/
export interface ExportedContents {
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
* For html format, this contains a string representing the HTML page, it has base64 encoded data for images.
* 
    */
    'content': any;
    /**
    * Format of content as requested by the client. By default, ipynb format.
* 
    */
    'format': ExportedContents.Format;

}

export namespace ExportedContents {




    export enum Format {
    
    Html = "html",
    Ipynb = "ipynb",
    /**
    * This value is used if a service returns a value for this enum that is not recognized by this
    * version of the SDK.
    */
    UnknownValue = "UNKNOWN_VALUE"
}


    export function getJsonObj(obj: ExportedContents): object {
        const jsonObj = {...obj, ...{
            




        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ExportedContents): object {
        const jsonObj = {...obj, ...{
            




         }};

        
        
        return jsonObj;
    }
}
