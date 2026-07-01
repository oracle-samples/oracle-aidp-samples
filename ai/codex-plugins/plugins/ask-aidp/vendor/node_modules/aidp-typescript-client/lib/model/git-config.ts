// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Git configuration used when source is GIT_PROVIDER.
*/
export interface GitConfig {
    /**
    * Git provider.
    */
    'provider'?: GitConfig.Provider;
    /**
    * Git credential to access the repository.
    */
    'credential'?: string;
    /**
    * Git repository URL.
    */
    'repositoryUrl'?: string;
    /**
    * Git branch path.
    */
    'branch'?: string;

}

export namespace GitConfig {

    export enum Provider {
    
    Github = "GITHUB",
    Bitbucket = "BITBUCKET",
    Gitlab = "GITLAB",
    OciDevops = "OCI_DEVOPS",
    /**
    * This value is used if a service returns a value for this enum that is not recognized by this
    * version of the SDK.
    */
    UnknownValue = "UNKNOWN_VALUE"
}





    export function getJsonObj(obj: GitConfig): object {
        const jsonObj = {...obj, ...{
            




        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: GitConfig): object {
        const jsonObj = {...obj, ...{
            




         }};

        
        
        return jsonObj;
    }
}
