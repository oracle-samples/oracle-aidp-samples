// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A Suggest object in a Data Lake
* 
*/
export interface ObjectSuggestSummary {
    /**
    * A user-friendly name.
    */
    'displayName': string;
    /**
    * The type of the Object
    */
    'type': string;
    /**
    * Path of object.
    */
    'path': string;
    /**
    * Key of object.
    */
    'key'?: string;
    /**
    * Key of the workspace.
    */
    'workspaceKey'?: string;
    /**
    * Key of the catalog.
    */
    'catalogKey'?: string;
    /**
    * Key of the schema.
    */
    'schemaKey'?: string;
    /**
    * Name of the workspace
    */
    'workspaceName'?: string;

}

export namespace ObjectSuggestSummary {









    export function getJsonObj(obj: ObjectSuggestSummary): object {
        const jsonObj = {...obj, ...{
            








        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ObjectSuggestSummary): object {
        const jsonObj = {...obj, ...{
            








         }};

        
        
        return jsonObj;
    }
}
