// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* agent flow to communicate with MCP using the defined auth mode
*/
export interface Auth {

   "authType": string;
}

export namespace Auth {

    export function getJsonObj(obj: Auth): object {
        const jsonObj = {...obj, ...{
            
        }};

        
        
        if (obj && "authType" in obj && obj.authType) {
            switch (obj.authType) {
                case "OCI_RESOURCE_PRINCIPAL":
                    return model.OciResourcePrincipalAuth.getJsonObj(<model.OciResourcePrincipalAuth>(<object>jsonObj), true);
                case "BEARER_TOKEN":
                    return model.BearerTokenAuth.getJsonObj(<model.BearerTokenAuth>(<object>jsonObj), true);
                case "NO_AUTH":
                    return model.NoAuth.getJsonObj(<model.NoAuth>(<object>jsonObj), true);
                case "OAUTH":
                    return model.OAuth.getJsonObj(<model.OAuth>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.authType}`)

        }
        }
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: Auth): object {
        const jsonObj = {...obj, ...{
            
         }};

        
        
        if (obj && "authType" in obj && obj.authType) {
            switch (obj.authType) {
                case "OCI_RESOURCE_PRINCIPAL":
                    return model.OciResourcePrincipalAuth.getDeserializedJsonObj(<model.OciResourcePrincipalAuth>(<object>jsonObj), true);
                case "BEARER_TOKEN":
                    return model.BearerTokenAuth.getDeserializedJsonObj(<model.BearerTokenAuth>(<object>jsonObj), true);
                case "NO_AUTH":
                    return model.NoAuth.getDeserializedJsonObj(<model.NoAuth>(<object>jsonObj), true);
                case "OAUTH":
                    return model.OAuth.getDeserializedJsonObj(<model.OAuth>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.authType}`)
        }
        }
        return jsonObj;
    }
}
