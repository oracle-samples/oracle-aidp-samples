// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The configuration details, whether to add the content moderation feature to the model. Content moderation removes toxic and biased content from responses.
*/
export interface ContentModerationConfig {
    /**
    * Enum for the modes of operation for inference protection.
    */
    'mode'?: ContentModerationConfig.Mode;
    /**
    * The OCID of the model used for the feature.
    */
    'modelId'?: string;
    /**
    * Whether to enable the content moderation feature.
    */
    'isEnabled': boolean;

}

export namespace ContentModerationConfig {

    export enum Mode {
    
    Inform = "INFORM",
    Block = "BLOCK"

}




    export function getJsonObj(obj: ContentModerationConfig): object {
        const jsonObj = {...obj, ...{
            



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ContentModerationConfig): object {
        const jsonObj = {...obj, ...{
            



         }};

        
        
        return jsonObj;
    }
}
