// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Capabilities included in publish request.
*/
export interface AgentCardCapabilitiesDetail {
    /**
    * Indicates whether streaming responses are supported.
    */
    'isStreaming'?: boolean;

}

export namespace AgentCardCapabilitiesDetail {


    export function getJsonObj(obj: AgentCardCapabilitiesDetail): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AgentCardCapabilitiesDetail): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
