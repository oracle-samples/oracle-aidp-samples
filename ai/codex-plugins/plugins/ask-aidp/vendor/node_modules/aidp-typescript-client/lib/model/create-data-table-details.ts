// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details to create a managed table with data.
*/
export interface CreateDataTableDetails {
    /**
    * Table name.
    */
    'displayName': string;
    /**
    * Table description.
    */
    'description'?: string;
    /**
    * The name of the catalog to which this table belongs.
    */
    'catalogKey': string;
    /**
    * The name of the schema to which this table belongs.
    */
    'schemaKey': string;
    /**
    * Columns for table.
    */
    'tableFields': Array<model.TableFieldDetails>;
    /**
    * Columns to be used in partition for table.
    */
    'partitionKeys'?: Array<model.TableFieldDetails>;
    /**
    * Table properties.
    */
    'tableProperties'?: Array<model.TableProperty>;
    'managedTableDefinition': model.ManagedTableDefinition;
    /**
    * The list of the columns from which data needs to be copied.
    */
    'selectedColumns': Array<string>;
    /**
    * Format of the sample file from which data needs to be copied.
    */
    'fileFormat': model.DataFormat;
    /**
    * The file location from which table details will be used.
    */
    'objectStorageLocationPath': string;

}

export namespace CreateDataTableDetails {












    export function getJsonObj(obj: CreateDataTableDetails): object {
        const jsonObj = {...obj, ...{
            




                'tableFields': obj.tableFields ?
                
                obj.tableFields.map((item)=>{return model.TableFieldDetails.getJsonObj(item)})
                
                 : undefined,
                'partitionKeys': obj.partitionKeys ?
                
                obj.partitionKeys.map((item)=>{return model.TableFieldDetails.getJsonObj(item)})
                
                 : undefined,
                'tableProperties': obj.tableProperties ?
                
                obj.tableProperties.map((item)=>{return model.TableProperty.getJsonObj(item)})
                
                 : undefined,
                'managedTableDefinition': obj.managedTableDefinition ?
                
                
                model.ManagedTableDefinition.getJsonObj(obj.managedTableDefinition) : undefined,



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CreateDataTableDetails): object {
        const jsonObj = {...obj, ...{
            




                    'tableFields': obj.tableFields ?
                
                obj.tableFields.map((item)=>{return model.TableFieldDetails.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'partitionKeys': obj.partitionKeys ?
                
                obj.partitionKeys.map((item)=>{return model.TableFieldDetails.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'tableProperties': obj.tableProperties ?
                
                obj.tableProperties.map((item)=>{return model.TableProperty.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'managedTableDefinition': obj.managedTableDefinition ?
                
                
                model.ManagedTableDefinition.getDeserializedJsonObj(obj.managedTableDefinition) : undefined,



         }};

        
        
        return jsonObj;
    }
}
