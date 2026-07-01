// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The data to create a table.
*/
export interface CreateTableDetails {
    /**
    * Table name.
    */
    'displayName': string;
    /**
    * The name of the catalog to which this table belongs.
    */
    'catalogKey': string;
    /**
    * The name of the schema to which this table belongs.
    */
    'schemaKey': string;
    /**
    * Table description.
    */
    'description'?: string;
    /**
    * Type of table. Managed, external or mount table.
    */
    'tableType': model.TableType;
    'managedTableDefinition'?: model.ManagedTableDefinition;
    'externalTableDefinition'?: model.ExternalTableDefinition;
    /**
    * Columns for table.
    */
    'tableFields'?: Array<model.TableFieldDetails>;
    /**
    * Columns to be used in partition for table.
    */
    'partitionKeys'?: Array<model.TableFieldDetails>;
    /**
    * Table properties.
    */
    'tableProperties'?: Array<model.TableProperty>;

}

export namespace CreateTableDetails {











    export function getJsonObj(obj: CreateTableDetails): object {
        const jsonObj = {...obj, ...{
            





                'managedTableDefinition': obj.managedTableDefinition ?
                
                
                model.ManagedTableDefinition.getJsonObj(obj.managedTableDefinition) : undefined,
                'externalTableDefinition': obj.externalTableDefinition ?
                
                
                model.ExternalTableDefinition.getJsonObj(obj.externalTableDefinition) : undefined,
                'tableFields': obj.tableFields ?
                
                obj.tableFields.map((item)=>{return model.TableFieldDetails.getJsonObj(item)})
                
                 : undefined,
                'partitionKeys': obj.partitionKeys ?
                
                obj.partitionKeys.map((item)=>{return model.TableFieldDetails.getJsonObj(item)})
                
                 : undefined,
                'tableProperties': obj.tableProperties ?
                
                obj.tableProperties.map((item)=>{return model.TableProperty.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CreateTableDetails): object {
        const jsonObj = {...obj, ...{
            





                    'managedTableDefinition': obj.managedTableDefinition ?
                
                
                model.ManagedTableDefinition.getDeserializedJsonObj(obj.managedTableDefinition) : undefined,
                    'externalTableDefinition': obj.externalTableDefinition ?
                
                
                model.ExternalTableDefinition.getDeserializedJsonObj(obj.externalTableDefinition) : undefined,
                    'tableFields': obj.tableFields ?
                
                obj.tableFields.map((item)=>{return model.TableFieldDetails.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'partitionKeys': obj.partitionKeys ?
                
                obj.partitionKeys.map((item)=>{return model.TableFieldDetails.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'tableProperties': obj.tableProperties ?
                
                obj.tableProperties.map((item)=>{return model.TableProperty.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
