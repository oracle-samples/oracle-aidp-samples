// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The information about a source to be added to a KnowledgeBase
*/
export interface UpdateKnowledgeBaseAddSourceDetails {
    /**
    * name for source
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
    * Optional boolean flag to indicate if ingestion job should run inline.
    */
    'shouldRunIngestionJobInline'?: boolean;
    /**
    * The id of the workspace associated with the source.
    */
    'workspaceKey'?: string;
    /**
    * The id of the cluster associated with the source.
    */
    'clusterKey'?: string;
    /**
    * location on volume or name of the table
    */
    'location': string;
    /**
    * Chunk size at KnowledgeBase level which can be overridden by source level settings Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'chunkSize'?: number;
    /**
    * Chunk Overlap at KnowledgeBase level which can be overridden by source level settings Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'chunkOverlap'?: number;
    /**
    * Applicable for Native KnowledgeBase where source type is KnowledgeBase
    */
    'sourceFilePattern'?: string;

}

export namespace UpdateKnowledgeBaseAddSourceDetails {











    export function getJsonObj(obj: UpdateKnowledgeBaseAddSourceDetails): object {
        const jsonObj = {...obj, ...{
            










        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateKnowledgeBaseAddSourceDetails): object {
        const jsonObj = {...obj, ...{
            










         }};

        
        
        return jsonObj;
    }
}
