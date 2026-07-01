// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Summary information about a Bucket.
*/
export interface BucketSummary {
    /**
    * The namespace for the specified Oracle Object storage resource. You can find the namespace under Object Storage Settings in the Console.
    */
    'namespace'?: string;
    /**
    * Bucket name
    */
    'name': string;
    /**
    * The [OCID]({{DOC_SERVER_URL}}/iaas/Content/General/Concepts/identifiers.htm) of the compartment in which to list resources.
    */
    'compartmentId'?: string;
    /**
    * The ID of the user who created the schema
* 
    */
    'createdBy'?: string;
    /**
    * The date and time the Data Lake Schema was created, in the format defined by [RFC 3339](https://tools.ietf.org/html/rfc3339).
* Example: {@code 2016-08-25T21:10:29.600Z}
* 
    */
    'timeCreated': Date;

}

export namespace BucketSummary {






    export function getJsonObj(obj: BucketSummary): object {
        const jsonObj = {...obj, ...{
            





        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: BucketSummary): object {
        const jsonObj = {...obj, ...{
            





         }};

        
        
        return jsonObj;
    }
}
