// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details for a generated URI for uploading a temporary file in a schema.
*/
export interface GenerateTempFileUploadTargetResponseDetails {
    /**
    * The generated target URI to upload the file, it must be prefixed by object stroage URL.
    */
    'tempFileUploadTarget': string;
    /**
    * The unique key for this request.
    */
    'uploadKey': string;
    /**
    * The exact URI path of the object storage.
    */
    'ociFilePath': string;

}

export namespace GenerateTempFileUploadTargetResponseDetails {




    export function getJsonObj(obj: GenerateTempFileUploadTargetResponseDetails): object {
        const jsonObj = {...obj, ...{
            



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: GenerateTempFileUploadTargetResponseDetails): object {
        const jsonObj = {...obj, ...{
            



         }};

        
        
        return jsonObj;
    }
}
