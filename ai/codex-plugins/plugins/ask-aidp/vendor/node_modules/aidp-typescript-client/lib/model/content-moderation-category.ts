// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Content moderation categories
**/
export enum ContentModerationCategory {
    HateSpeech = "HATE_SPEECH",
    Harassment = "HARASSMENT",
    Violence = "VIOLENCE",
    Sexual = "SEXUAL",
    Derogatory = "DEROGATORY",
    Toxic = "TOXIC"
    
}

export namespace ContentModerationCategory {
    export function getJsonObj(obj: ContentModerationCategory): ContentModerationCategory {
        return obj;
    }
    export function getDeserializedJsonObj(obj: ContentModerationCategory): ContentModerationCategory {
        return obj;
    }
}

