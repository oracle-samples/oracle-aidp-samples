// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The data to update a view.
*/
export interface UpdateViewDetails {
    /**
    * The mode of update for a view
    */
    'updateMode': UpdateViewDetails.UpdateMode;
    'viewUpdateDetails'?: model.ViewUpdateDetails;

}

export namespace UpdateViewDetails {

    export enum UpdateMode {
    
    RenameView = "RENAME_VIEW",
    AddProperties = "ADD_PROPERTIES",
    DropProperties = "DROP_PROPERTIES",
    UpdateViewDescription = "UPDATE_VIEW_DESCRIPTION",
    UpdateViewQuery = "UPDATE_VIEW_QUERY",
    UpdateColumnDescription = "UPDATE_COLUMN_DESCRIPTION"

}



    export function getJsonObj(obj: UpdateViewDetails): object {
        const jsonObj = {...obj, ...{
            

                'viewUpdateDetails': obj.viewUpdateDetails ?
                
                
                model.ViewUpdateDetails.getJsonObj(obj.viewUpdateDetails) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateViewDetails): object {
        const jsonObj = {...obj, ...{
            

                    'viewUpdateDetails': obj.viewUpdateDetails ?
                
                
                model.ViewUpdateDetails.getDeserializedJsonObj(obj.viewUpdateDetails) : undefined,
         }};

        
        
        return jsonObj;
    }
}
