// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Configuration for a custom tool package uploaded to the workspace volume.
*/
export interface CustomToolConfiguration {
    /**
    * Human-readable name for this tool package
    */
    'displayName'?: string;
    /**
    * Description of the tool package
    */
    'description'?: string;
    /**
    * Tool package version
    */
    'version'?: string;
    /**
    * Workspace volume path to the uploaded ZIP file
    */
    'packagePath'?: string;
    /**
    * SHA256 hash of requirements.txt for dependency caching
    */
    'requirementsHash'?: string;
    'auth'?: model.OciResourcePrincipalAuth| model.BearerTokenAuth| model.NoAuth| model.OAuth;
    /**
    * Tool provider identifier
    */
    'toolProvider'?: string;
    /**
    * List of tool class entries in this package (multi-tool support)
    */
    'tools'?: Array<model.CustomToolEntry>;

}

export namespace CustomToolConfiguration {









    export function getJsonObj(obj: CustomToolConfiguration): object {
        const jsonObj = {...obj, ...{
            





                'auth': obj.auth ?
                
                
                model.Auth.getJsonObj(obj.auth) : undefined,

                'tools': obj.tools ?
                
                obj.tools.map((item)=>{return model.CustomToolEntry.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CustomToolConfiguration): object {
        const jsonObj = {...obj, ...{
            





                    'auth': obj.auth ?
                
                
                model.Auth.getDeserializedJsonObj(obj.auth) : undefined,

                    'tools': obj.tools ?
                
                obj.tools.map((item)=>{return model.CustomToolEntry.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
