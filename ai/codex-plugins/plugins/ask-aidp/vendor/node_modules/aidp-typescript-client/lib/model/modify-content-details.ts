// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Path of file to copy. A POST to /api/contents/path creates a New untitled, empty file or directory. A POST to /api/contents/path with body {'copy_from': '/path/to/OtherNotebook.ipynb'} creates a new copy of OtherNotebook in path
*/
export interface ModifyContentDetails {
    /**
    * New path for file or directory
    */
    'path'?: string;

}

export namespace ModifyContentDetails {


    export function getJsonObj(obj: ModifyContentDetails): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ModifyContentDetails): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
