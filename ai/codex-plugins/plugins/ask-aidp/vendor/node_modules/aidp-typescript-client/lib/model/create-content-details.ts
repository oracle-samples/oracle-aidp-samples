// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Path of file to copy. A POST to /api/contents/path creates a New untitled, empty file or directory. A POST to /api/contents/path with body {'copy_from': '/path/to/OtherNotebook.ipynb'} creates a new copy of OtherNotebook in path.
*/
export interface CreateContentDetails {
    /**
    * Copy from Path. For example, /path/to/OtherNotebook.ipynb.
    */
    'copyFrom'?: string;
    /**
    * File format extension
    */
    'ext'?: string;
    /**
    * Type of Content model. Either notebook, file, or directory.
    */
    'type'?: string;

}

export namespace CreateContentDetails {




    export function getJsonObj(obj: CreateContentDetails): object {
        const jsonObj = {...obj, ...{
            
                'copy_from': obj.copyFrom,



        }};

        delete (jsonObj as Partial<CreateContentDetails>).copyFrom;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CreateContentDetails): object {
        const jsonObj = {...obj, ...{
            
                'copyFrom': (obj as any)["copy_from"],



         }};

        delete (jsonObj as any)["copy_from"];
        
        return jsonObj;
    }
}
