// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The data to create a volume.
*/
export interface CreateVolumeDetails {
    /**
    * A user-friendly name. Has to be unique within the schema and is changeable.
    */
    'displayName': string;
    /**
    * The name of the catalog to which this volume belongs.
    */
    'catalogName': string;
    /**
    * The name of the schema to which this volume belongs.
    */
    'schemaName': string;
    /**
    * Short description of the volume
    */
    'description'?: string;
    /**
    * The type of volume.
    */
    'volumeType'?: CreateVolumeDetails.VolumeType;
    /**
    * The storage location of the external volume. Only applicable for external volumes.
    */
    'storageLocation'?: string;

}

export namespace CreateVolumeDetails {





    export enum VolumeType {
    
    Managed = "MANAGED",
    External = "EXTERNAL"

}



    export function getJsonObj(obj: CreateVolumeDetails): object {
        const jsonObj = {...obj, ...{
            






        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CreateVolumeDetails): object {
        const jsonObj = {...obj, ...{
            






         }};

        
        
        return jsonObj;
    }
}
