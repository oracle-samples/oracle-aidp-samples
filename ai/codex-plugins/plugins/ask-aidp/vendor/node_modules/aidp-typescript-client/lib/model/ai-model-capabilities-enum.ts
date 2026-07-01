// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Possible capabilities of AiModels object
**/
export enum AiModelCapabilitiesEnum {
    FineTune = "FINE_TUNE",
    Chat = "CHAT",
    TextEmbeddings = "TEXT_EMBEDDINGS"
    
}

export namespace AiModelCapabilitiesEnum {
    export function getJsonObj(obj: AiModelCapabilitiesEnum): AiModelCapabilitiesEnum {
        return obj;
    }
    export function getDeserializedJsonObj(obj: AiModelCapabilitiesEnum): AiModelCapabilitiesEnum {
        return obj;
    }
}

