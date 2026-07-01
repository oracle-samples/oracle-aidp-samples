// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Metadata of the dataLake
*/
export interface DataLakeMetadata {
    /**
    * Log object Id for DataLake logs
    */
    'auditLogId'?: string;
    /**
    * log groupId for the DataLake
    */
    'logGroupId'?: string;
    /**
    * is Audit enabled for the DataLake
    */
    'isAuditEnabled'?: boolean;
    /**
    * DataLake Retention period for audit logs Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'auditRetentionPeriod'?: number;

}

export namespace DataLakeMetadata {





    export function getJsonObj(obj: DataLakeMetadata): object {
        const jsonObj = {...obj, ...{
            




        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: DataLakeMetadata): object {
        const jsonObj = {...obj, ...{
            




         }};

        
        
        return jsonObj;
    }
}
