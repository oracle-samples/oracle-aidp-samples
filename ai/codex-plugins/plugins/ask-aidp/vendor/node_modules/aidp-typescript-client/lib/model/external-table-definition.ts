// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details about the new external table.
*/
export interface ExternalTableDefinition {
    /**
    * External table location type, either object store location or mount location.
    */
    'externalTableLocationType'?: model.ExternalTableLocationType;
    /**
    * The file location from which table properties are loaded.
    */
    'objectStorageLocationPath'?: string;
    /**
    * External table data format.
    */
    'externalTableDataFormat': model.DataFormat;
    'txtFileDefinition'?: model.TxtFileDefinition;

}

export namespace ExternalTableDefinition {





    export function getJsonObj(obj: ExternalTableDefinition): object {
        const jsonObj = {...obj, ...{
            



                'txtFileDefinition': obj.txtFileDefinition ?
                
                
                model.TxtFileDefinition.getJsonObj(obj.txtFileDefinition) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ExternalTableDefinition): object {
        const jsonObj = {...obj, ...{
            



                    'txtFileDefinition': obj.txtFileDefinition ?
                
                
                model.TxtFileDefinition.getDeserializedJsonObj(obj.txtFileDefinition) : undefined,
         }};

        
        
        return jsonObj;
    }
}
