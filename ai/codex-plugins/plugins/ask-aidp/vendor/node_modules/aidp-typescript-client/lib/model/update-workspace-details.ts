// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The data to update a workspace.
*/
export interface UpdateWorkspaceDetails {
    /**
    * A user-friendly name that has to be unique in a AI Data Platform Workbench instance.
    */
    'displayName'?: string;
    /**
    * Description of the workspace.
    */
    'description'?: string;
    /**
    * The key of the catalog to be used as the default catalog for this workspace.
* A default catalog in the workspace will allow users to use that
* catalog without the need to refer it in the notebook. For example, if default catalog is iCat1, and it has
* schema1 and table1, you can refer to the table in a notebook using: schema1.table1.
* 
    */
    'defaultCatalogKey'?: string;
    'networkConfigurationDetails'?: model.WorkspaceNetworkConfigurationDetails;

}

export namespace UpdateWorkspaceDetails {





    export function getJsonObj(obj: UpdateWorkspaceDetails): object {
        const jsonObj = {...obj, ...{
            



                'networkConfigurationDetails': obj.networkConfigurationDetails ?
                
                
                model.WorkspaceNetworkConfigurationDetails.getJsonObj(obj.networkConfigurationDetails) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateWorkspaceDetails): object {
        const jsonObj = {...obj, ...{
            



                    'networkConfigurationDetails': obj.networkConfigurationDetails ?
                
                
                model.WorkspaceNetworkConfigurationDetails.getDeserializedJsonObj(obj.networkConfigurationDetails) : undefined,
         }};

        
        
        return jsonObj;
    }
}
