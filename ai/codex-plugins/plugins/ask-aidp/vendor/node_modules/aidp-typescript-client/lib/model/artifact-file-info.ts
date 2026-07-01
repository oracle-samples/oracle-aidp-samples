// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* File info of artifact.
*/
export interface ArtifactFileInfo {
    /**
    * Path relative to the root artifact directory run.
    */
    'path'?: string;
    /**
    * Whether the path is a directory.
    */
    'isDir'?: boolean;
    /**
    * Size in bytes. Unset for directories. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'fileSize'?: number;

}

export namespace ArtifactFileInfo {




    export function getJsonObj(obj: ArtifactFileInfo): object {
        const jsonObj = {...obj, ...{
            

                'is_dir': obj.isDir,

                'file_size': obj.fileSize,

        }};

        delete (jsonObj as Partial<ArtifactFileInfo>).isDir;delete (jsonObj as Partial<ArtifactFileInfo>).fileSize;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ArtifactFileInfo): object {
        const jsonObj = {...obj, ...{
            

                'isDir': (obj as any)["is_dir"],

                'fileSize': (obj as any)["file_size"],

         }};

        delete (jsonObj as any)["is_dir"];delete (jsonObj as any)["file_size"];
        
        return jsonObj;
    }
}
