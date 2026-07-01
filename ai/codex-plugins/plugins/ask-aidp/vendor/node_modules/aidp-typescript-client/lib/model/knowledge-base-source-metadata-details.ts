// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The information about a source to be added to a KnowledgeBase
*/
export interface KnowledgeBaseSourceMetadataDetails {
    /**
    * key for the source
    */
    'key': string;
    /**
    * name for the source
    */
    'name': string;
    /**
    * New description of KnowledgeBase
    */
    'description'?: string;
    /**
    * The type of source
    */
    'type': model.KnowledgeBaseSourceType;
    /**
    * location on volume or name of the table
    */
    'location': string;
    /**
    * The id of the workspace associated with the source.
    */
    'workspaceKey'?: string;
    /**
    * The id of the cluster associated with the source.
    */
    'clusterKey'?: string;
    /**
    * Chunk size at source level settings Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'chunkSize'?: number;
    /**
    * Chunk Overlap at source level settings Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'chunkOverlap'?: number;
    /**
    * Applicable for Native KnowledgeBase where source type is KnowledgeBase
    */
    'sourceFilePattern'?: string;
    /**
    * The time at which KnowledgeBase was created. An RFC3339 formatted datetime string
    */
    'timeCreated'?: Date;
    /**
    * Identifier for KnowledgeBase creator
    */
    'createdBy'?: string;

}

export namespace KnowledgeBaseSourceMetadataDetails {













    export function getJsonObj(obj: KnowledgeBaseSourceMetadataDetails): object {
        const jsonObj = {...obj, ...{
            












        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: KnowledgeBaseSourceMetadataDetails): object {
        const jsonObj = {...obj, ...{
            












         }};

        
        
        return jsonObj;
    }
}
