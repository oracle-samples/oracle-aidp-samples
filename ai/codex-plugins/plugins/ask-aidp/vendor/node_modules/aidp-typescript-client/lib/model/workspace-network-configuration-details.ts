// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Private network configuration details.
*/
export interface WorkspaceNetworkConfigurationDetails {
    /**
    * The OCID of a subnet.
* 
    */
    'subnetId'?: string;
    /**
    * An array of network security group OCIDs.
* 
    */
    'nsgIds'?: Array<string>;
    /**
    * An array of fqdn/port pairs used to create private endpoint. Each object is a simple key-value pair with FQDN as key and port number as value.
* [ { fqdn: \"scan1.oracle.com\", port: \"1521\"}, { fqdn: \"scan2.oracle.com\", port: \"1521\" } ]
* 
    */
    'scanDetails'?: Array<model.Scan>;

}

export namespace WorkspaceNetworkConfigurationDetails {




    export function getJsonObj(obj: WorkspaceNetworkConfigurationDetails): object {
        const jsonObj = {...obj, ...{
            


                'scanDetails': obj.scanDetails ?
                
                obj.scanDetails.map((item)=>{return model.Scan.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: WorkspaceNetworkConfigurationDetails): object {
        const jsonObj = {...obj, ...{
            


                    'scanDetails': obj.scanDetails ?
                
                obj.scanDetails.map((item)=>{return model.Scan.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
