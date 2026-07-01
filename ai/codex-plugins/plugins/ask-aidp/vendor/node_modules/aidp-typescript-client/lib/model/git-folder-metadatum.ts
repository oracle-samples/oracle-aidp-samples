// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details about a Git repository.
*/
export interface GitFolderMetadatum {
    /**
    * Unique repo key if folder/file path is associated with a Git folder.
    */
    'repoKey': string;
    /**
    * If the passed folder/file path is associated with a Git folder. (Active/Inactive)
    */
    'isAssociated': boolean;

}

export namespace GitFolderMetadatum {



    export function getJsonObj(obj: GitFolderMetadatum): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: GitFolderMetadatum): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
