/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
interface hasPage {
    page?: string;
}
interface hasStart {
    start?: string;
}
declare type Request = hasPage | hasStart;
export declare function genericPaginateRecords<ListRequest, ListResponse, ItemSummary>(request: ListRequest, listCall: (request: ListRequest) => Promise<ListResponse>, getNextPageToken: (response: ListResponse) => string | undefined, setNextPageToken: (request: ListRequest, nextPageToken: string) => void, getItems: (response: ListResponse) => ItemSummary[], limitedRecord?: number): AsyncIterableIterator<ItemSummary>;
export declare function paginateRecords<ListRequest extends Request, ListResponse, ItemSummary = any>(request: ListRequest, listCall: (request: ListRequest) => Promise<ListResponse>, limitedRecord?: number): AsyncIterableIterator<ItemSummary>;
export declare function genericPaginateResponses<ListRequest, ListResponse>(request: ListRequest, listCall: (request: ListRequest) => Promise<ListResponse>, getNextPageToken: (response: ListResponse) => string | undefined, setNextPageToken: (request: ListRequest, nextPageToken: string) => void, limitedRecord?: number): AsyncIterableIterator<ListResponse>;
export declare function paginateResponses<ListRequest extends Request, ListResponse>(request: ListRequest, listCall: (request: ListRequest) => Promise<ListResponse>, limitedRecord?: number): AsyncIterableIterator<ListResponse>;
export declare function paginatedRecordsWithLimit<ListRequest extends Request, ListResponse>(request: ListRequest, listCall: (request: ListRequest) => Promise<ListResponse>, limitedRecord?: number): Promise<IteratorYieldResult<any>[]>;
export declare function paginatedResponsesWithLimit<ListRequest extends Request, ListResponse>(request: ListRequest, listCall: (request: ListRequest) => Promise<ListResponse>, limitedRecord?: number): Promise<any>;
export {};
