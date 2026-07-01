// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Manage audit log details.
*/
export interface ManageAuditLogsDetails {
    /**
    * Action to enable or disable the logs.
    */
    'action'?: model.Action;
    /**
    * Retention period of the audit logs. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'retentionPeriod'?: number;

}

export namespace ManageAuditLogsDetails {



    export function getJsonObj(obj: ManageAuditLogsDetails): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ManageAuditLogsDetails): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
