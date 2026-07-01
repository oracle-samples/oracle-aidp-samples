// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * @example Click {@link https://docs.oracle.com/en-us/iaas/tools/typescript-sdk-examples/latest/aidp/ListGitBranches.ts.html |here} to see how to use ListGitBranchesRequest.
 */
export interface ListGitBranchesRequest extends common.BaseRequest {
/**
 * The [OCID]({{DOC_SERVER_URL}}/iaas/Content/General/Concepts/identifiers.htm) of the AI Data Platform (Data Lake) instance.
 */
 'aiDataPlatformId': string;
/**
 * The key of the Workspace
 */
 'workspaceKey': string;
/**
 * The Git repository key.
 */
 'gitRepositoryKey': string;
/**
 * A filter to return only resources that match the given display name exactly.
 */
 'displayName'?: string;
/**
 * A filter to return only resources that have a display name containing the text provided.
 */
 'displayNameContains'?: string;
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
 * The field to sort by. You can provide only one sort order. Default order for {@code timeCreated} is descending. Default order for {@code displayName} is ascending.
 */
 'sortBy'?: ListGitBranchesRequest.SortBy;
/**
 * Unique Oracle-assigned identifier for the request. If you need to contact
* Oracle about a particular request, please provide the request ID.
* The only valid characters for request IDs are letters, numbers,
* underscore, and dash.
* 
 */
 'opcRequestId'?: string;
}

export namespace ListGitBranchesRequest {

  export enum SortBy {
    TimeCreated = ("timeCreated"),
    DisplayName = ("displayName"),
    Status = ("status")
  }

}
