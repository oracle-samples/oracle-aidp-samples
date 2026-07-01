// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * @example Click {@link https://docs.oracle.com/en-us/iaas/tools/typescript-sdk-examples/latest/aidp/ListFiles.ts.html |here} to see how to use ListFilesRequest.
 */
export interface ListFilesRequest extends common.BaseRequest {
/**
 * The [OCID]({{DOC_SERVER_URL}}/iaas/Content/General/Concepts/identifiers.htm) of the AI Data Platform (Data Lake) instance.
 */
 'aiDataPlatformId': string;
/**
 * The key of the volume.
 */
 'volumeKey': string;
/**
 * The absolute path of the file or folder
 */
 'path': string;
/**
 * A boolean which decides if nested files should be in the list files in volume response.
 */
 'isRecursive'?: boolean;
/**
 * A filter to return only resources that match the given display name exactly.
 */
 'displayName'?: string;
/**
 * Comma separated keys to have in list response.
 */
 'metadataKeys'?: string;
/**
 * For list pagination. The maximum number of results per page, or items to return in a
* paginated \"List\" call. For important details about how pagination works, see
* [List Pagination]({{DOC_SERVER_URL}}/iaas/Content/API/Concepts/usingapi.htm#nine).
* 
 */
 'limit'?: number;
/**
 * For list pagination. The value of the opc-next-page response header from the previous
* \"List\" call. For important details about how pagination works, see
* [List Pagination]({{DOC_SERVER_URL}}/iaas/Content/API/Concepts/usingapi.htm#nine).
* 
 */
 'page'?: string;
/**
 * The sort order to use, either ascending ({@code ASC}) or descending ({@code DESC}).
 */
 'sortOrder'?: model.SortOrder;
/**
 * The field to sort by. You can provide only one sort order. Default order for {@code timeCreated}
* is descending. Default order for {@code displayName} is ascending.
* 
 */
 'sortBy'?: ListFilesRequest.SortBy;
/**
 * Unique Oracle-assigned identifier for the request. If you need to contact
* Oracle about a particular request, please provide the request ID.
* The only valid characters for request IDs are letters, numbers,
* underscore, and dash.
* 
 */
 'opcRequestId'?: string;
/**
 * A flag to identify if the recent list should be updated.
 */
 'shouldUpdateRecent'?: boolean;
}

export namespace ListFilesRequest {

  export enum SortBy {
    TimeCreated = ("timeCreated"),
    DisplayName = ("displayName")
  }

}
