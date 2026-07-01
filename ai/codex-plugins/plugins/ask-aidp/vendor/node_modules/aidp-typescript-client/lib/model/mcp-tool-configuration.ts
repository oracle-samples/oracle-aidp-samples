// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Tool configurations are set by the agent developer when they create the tool. | The agent does not see those configurations and can NOT modify their values
*/
export interface McpToolConfiguration {
    /**
    * The endpoint of the mcp to connect to
    */
    'endpoint': string;
    'auth'?: model.OciResourcePrincipalAuth| model.BearerTokenAuth| model.NoAuth| model.OAuth;
    /**
    * The list of allowed tools on an MCP server.
    */
    'allowedTools'?: Array<model.AllowedToolDetails>;
    /**
    * Map of header key value pairs.
    */
    'customHeaders'?: { [key: string]: string; };

}

export namespace McpToolConfiguration {





    export function getJsonObj(obj: McpToolConfiguration): object {
        const jsonObj = {...obj, ...{
            

                'auth': obj.auth ?
                
                
                model.Auth.getJsonObj(obj.auth) : undefined,
                'allowedTools': obj.allowedTools ?
                
                obj.allowedTools.map((item)=>{return model.AllowedToolDetails.getJsonObj(item)})
                
                 : undefined,

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: McpToolConfiguration): object {
        const jsonObj = {...obj, ...{
            

                    'auth': obj.auth ?
                
                
                model.Auth.getDeserializedJsonObj(obj.auth) : undefined,
                    'allowedTools': obj.allowedTools ?
                
                obj.allowedTools.map((item)=>{return model.AllowedToolDetails.getDeserializedJsonObj(item)})
                
                 : undefined,

         }};

        
        
        return jsonObj;
    }
}
