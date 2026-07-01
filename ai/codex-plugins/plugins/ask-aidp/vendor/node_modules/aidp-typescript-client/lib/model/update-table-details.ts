// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The data to update a table.
*/
export interface UpdateTableDetails {
    /**
    * Table name.
    */
    'displayName'?: string;
    /**
    * Table description.
    */
    'description'?: string;
    /**
    * Columns for table.
    */
    'addTableFields'?: Array<model.TableFieldDetails>;
    /**
    * Columns for table.
    */
    'dropTableFields'?: Array<model.TableFieldDetails>;
    /**
    * Columns for table.
    */
    'renameTableFields'?: Array<model.RenameTableFieldDetails>;
    /**
    * Table properties.
    */
    'addTableProperties'?: Array<model.TableProperty>;
    /**
    * Table properties.
    */
    'dropTableProperties'?: Array<model.TableProperty>;
    /**
    * Update columns in table.
    */
    'updateTableFields'?: Array<model.UpdateTableFieldDetails>;

}

export namespace UpdateTableDetails {









    export function getJsonObj(obj: UpdateTableDetails): object {
        const jsonObj = {...obj, ...{
            


                'addTableFields': obj.addTableFields ?
                
                obj.addTableFields.map((item)=>{return model.TableFieldDetails.getJsonObj(item)})
                
                 : undefined,
                'dropTableFields': obj.dropTableFields ?
                
                obj.dropTableFields.map((item)=>{return model.TableFieldDetails.getJsonObj(item)})
                
                 : undefined,
                'renameTableFields': obj.renameTableFields ?
                
                obj.renameTableFields.map((item)=>{return model.RenameTableFieldDetails.getJsonObj(item)})
                
                 : undefined,
                'addTableProperties': obj.addTableProperties ?
                
                obj.addTableProperties.map((item)=>{return model.TableProperty.getJsonObj(item)})
                
                 : undefined,
                'dropTableProperties': obj.dropTableProperties ?
                
                obj.dropTableProperties.map((item)=>{return model.TableProperty.getJsonObj(item)})
                
                 : undefined,
                'updateTableFields': obj.updateTableFields ?
                
                obj.updateTableFields.map((item)=>{return model.UpdateTableFieldDetails.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateTableDetails): object {
        const jsonObj = {...obj, ...{
            


                    'addTableFields': obj.addTableFields ?
                
                obj.addTableFields.map((item)=>{return model.TableFieldDetails.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'dropTableFields': obj.dropTableFields ?
                
                obj.dropTableFields.map((item)=>{return model.TableFieldDetails.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'renameTableFields': obj.renameTableFields ?
                
                obj.renameTableFields.map((item)=>{return model.RenameTableFieldDetails.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'addTableProperties': obj.addTableProperties ?
                
                obj.addTableProperties.map((item)=>{return model.TableProperty.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'dropTableProperties': obj.dropTableProperties ?
                
                obj.dropTableProperties.map((item)=>{return model.TableProperty.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'updateTableFields': obj.updateTableFields ?
                
                obj.updateTableFields.map((item)=>{return model.UpdateTableFieldDetails.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
