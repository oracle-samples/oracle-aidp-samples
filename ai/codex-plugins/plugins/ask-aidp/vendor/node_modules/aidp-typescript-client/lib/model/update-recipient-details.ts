// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The data to update a recipient.
*/
export interface UpdateRecipientDetails {
    /**
    * A user-friendly name. Has to be unique within the AI Data Platform Workbench instance.
    */
    'displayName'?: string;
    /**
    * Short description of the Recipient
    */
    'description'?: string;
    /**
    * Key-value pair representing a defined tag key and value.
* Example: {@code { \"CostCenter\": \"42\" }}
* 
    */
    'properties'?: { [key: string]: string; };

}

export namespace UpdateRecipientDetails {




    export function getJsonObj(obj: UpdateRecipientDetails): object {
        const jsonObj = {...obj, ...{
            



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateRecipientDetails): object {
        const jsonObj = {...obj, ...{
            



         }};

        
        
        return jsonObj;
    }
}
