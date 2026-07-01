// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * @example Click {@link https://docs.oracle.com/en-us/iaas/tools/typescript-sdk-examples/latest/aidp/ListJobRuns.ts.html |here} to see how to use ListJobRunsRequest.
 */
export interface ListJobRunsRequest extends common.BaseRequest {
/**
 * The [OCID]({{DOC_SERVER_URL}}/iaas/Content/General/Concepts/identifiers.htm) of the AI Data Platform (Data Lake) instance.
 */
 'aiDataPlatformId': string;
/**
 * The key of the Workspace
 */
 'workspaceKey': string;
/**
 * A filter to return only resources that match the given display name exactly.
 */
 'displayName'?: string;
/**
 * The field to filter based on job key.
 */
 'jobKey'?: Array<string>;
/**
 * The field to filter based on state.
 */
 'status'?: Array<ListJobRunsRequest.Status>;
/**
 * Fetch objects from repository that were created after or at the exact timestamp provided in parameter
 */
 'timeCreatedGreaterThanOrEqualTo'?: Date;
/**
 * Fetch objects from repository that were created before or at the exact timestamp provided in parameter.
 */
 'timeCreatedLessThanOrEqualTo'?: Date;
/**
 * For list pagination. The maximum number of results per page, or items to return in a paginated List call.
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
 'sortBy'?: ListJobRunsRequest.SortBy;
/**
 * Unique Oracle-assigned identifier for the request. If you need to contact
* Oracle about a particular request, please provide the request ID.
* The only valid characters for request IDs are letters, numbers,
* underscore, and dash.
* 
 */
 'opcRequestId'?: string;
}

export namespace ListJobRunsRequest {

  export enum Status {
    Pending = ("PENDING"),
    Running = ("RUNNING"),
    Skipped = ("SKIPPED"),
    InternalError = ("INTERNAL_ERROR"),
    Blocked = ("BLOCKED"),
    Success = ("SUCCESS"),
    Failed = ("FAILED"),
    Canceled = ("CANCELED"),
    UpstreamCanceled = ("UPSTREAM_CANCELED"),
    UpstreamFailed = ("UPSTREAM_FAILED"),
    Excluded = ("EXCLUDED")
  }

  export enum SortBy {
    TimeCreated = ("timeCreated"),
    DisplayName = ("displayName"),
    Status = ("status")
  }

}
