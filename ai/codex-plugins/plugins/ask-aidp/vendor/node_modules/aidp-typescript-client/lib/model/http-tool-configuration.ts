// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Configuration for HTTP Tool requests
*/
export interface HttpToolConfiguration {
    /**
    * HTTP method (GET, POST, PUT, DELETE, PATCH)
    */
    'method'?: model.HttpMethod;
    /**
    * Target URL with optional {{variable}} templates
    */
    'url'?: string;
    /**
    * Optional custom headers
    */
    'headers'?: { [key: string]: string; };
    /**
    * Optional query parameters
    */
    'params'?: { [key: string]: string; };
    /**
    * Optional request body (for POST, PUT, PATCH)
    */
    'body'?: { [key: string]: any; };
    /**
    * Request timeout in seconds Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'timeout'?: number;
    'auth'?: model.OciResourcePrincipalAuth| model.BearerTokenAuth| model.NoAuth| model.OAuth;
    /**
    * Response optimization settings
    */
    'responseOptimization'?: { [key: string]: any; };

}

export namespace HttpToolConfiguration {









    export function getJsonObj(obj: HttpToolConfiguration): object {
        const jsonObj = {...obj, ...{
            






                'auth': obj.auth ?
                
                
                model.Auth.getJsonObj(obj.auth) : undefined,

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: HttpToolConfiguration): object {
        const jsonObj = {...obj, ...{
            






                    'auth': obj.auth ?
                
                
                model.Auth.getDeserializedJsonObj(obj.auth) : undefined,

         }};

        
        
        return jsonObj;
    }
}
