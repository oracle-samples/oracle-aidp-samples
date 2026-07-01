// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* File dependency. Represents a file or library.
*/
export interface FileDependency extends model.Dependency {
    /**
    * File path or library name.
    */
    'path'?: string;

   "type": string;
}

export namespace FileDependency {


    export function getJsonObj(obj: FileDependency, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Dependency.getJsonObj(obj) as FileDependency, ...{
            

        }};

        
        
        return jsonObj;
    }
    export const type = 'FILE';
    export function getDeserializedJsonObj(obj: FileDependency, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Dependency.getDeserializedJsonObj(obj) as FileDependency, ...{
            

         }};

        
        
        return jsonObj;
    }
}
