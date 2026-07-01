"use strict";
/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __await = (this && this.__await) || function (v) { return this instanceof __await ? (this.v = v, this) : new __await(v); }
var __asyncGenerator = (this && this.__asyncGenerator) || function (thisArg, _arguments, generator) {
    if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
    var g = generator.apply(thisArg, _arguments || []), i, q = [];
    return i = {}, verb("next"), verb("throw"), verb("return"), i[Symbol.asyncIterator] = function () { return this; }, i;
    function verb(n) { if (g[n]) i[n] = function (v) { return new Promise(function (a, b) { q.push([n, v, a, b]) > 1 || resume(n, v); }); }; }
    function resume(n, v) { try { step(g[n](v)); } catch (e) { settle(q[0][3], e); } }
    function step(r) { r.value instanceof __await ? Promise.resolve(r.value.v).then(fulfill, reject) : settle(q[0][2], r); }
    function fulfill(value) { resume("next", value); }
    function reject(value) { resume("throw", value); }
    function settle(f, v) { if (f(v), q.shift(), q.length) resume(q[0][0], q[0][1]); }
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.paginatedResponsesWithLimit = exports.paginatedRecordsWithLimit = exports.paginateResponses = exports.genericPaginateResponses = exports.paginateRecords = exports.genericPaginateRecords = void 0;
function genericPaginate(request, listCall, getNextPageToken, setNextPageToken, getResults, limitedRecord) {
    return __asyncGenerator(this, arguments, function* genericPaginate_1() {
        let iteration = 0;
        const pageSize = request.limit || 0;
        if (limitedRecord && pageSize) {
            // We don't want to request a page size bigger than limitedRecord
            request.limit = Math.min(limitedRecord, pageSize);
        }
        else if (limitedRecord) {
            // If we do not have a page size, but we do have a limitedRecord, we will set page size = limitedRecord
            request.limit = limitedRecord;
        }
        while (true) {
            //  Return early if user does not want any records...
            if (limitedRecord === 0)
                return yield __await(void 0);
            //  If our request pageSize is bigger than the remaining records we need
            //  Set pageSize to the recordsRemaining to not retrieve more than necessary.
            if (request.limit && limitedRecord) {
                let recordsRemaining = limitedRecord - iteration;
                if (request.limit > recordsRemaining) {
                    request.limit = recordsRemaining;
                }
            }
            const response = yield __await(listCall(request));
            for (const result of getResults(response)) {
                if (result.items && result.items.length > 0) {
                    iteration += result.items.length;
                }
                else if (result.listObjects && result.listObjects.objects) {
                    iteration += result.listObjects.objects.length;
                }
                else {
                    iteration++;
                }
                yield yield __await(result);
                if (limitedRecord && iteration > limitedRecord - 1) {
                    return yield __await(void 0);
                }
            }
            const nextPageToken = getNextPageToken(response);
            if (!nextPageToken) {
                break;
            }
            setNextPageToken(request, nextPageToken);
        }
    });
}
function genericPaginateRecords(request, listCall, getNextPageToken, setNextPageToken, getItems, limitedRecord) {
    return genericPaginate(request, listCall, getNextPageToken, setNextPageToken, getItems, limitedRecord);
}
exports.genericPaginateRecords = genericPaginateRecords;
function paginateRecords(request, listCall, limitedRecord) {
    return genericPaginateRecords(request, listCall, res => findNextPageToken(res), (req, nextPageToken) => findSetPageToken(req, nextPageToken), res => findResult(res), limitedRecord);
}
exports.paginateRecords = paginateRecords;
function genericPaginateResponses(request, listCall, getNextPageToken, setNextPageToken, limitedRecord) {
    return genericPaginate(request, listCall, getNextPageToken, setNextPageToken, res => [res], limitedRecord);
}
exports.genericPaginateResponses = genericPaginateResponses;
function paginateResponses(request, listCall, limitedRecord) {
    return genericPaginateResponses(request, listCall, response => findNextPageToken(response), (req, nextPageToken) => findSetPageToken(req, nextPageToken), limitedRecord);
}
exports.paginateResponses = paginateResponses;
// Return a list of records up to limitedRecord. limitedRecord is optional. Do not pass limitedRecord if
// want to retrieve all records.
function paginatedRecordsWithLimit(request, listCall, limitedRecord) {
    return __awaiter(this, void 0, void 0, function* () {
        const records = [];
        const response = paginateRecords(request, listCall, limitedRecord);
        let record = yield response.next();
        while (record.done === false) {
            records.push(record);
            record = yield response.next();
        }
        return records;
    });
}
exports.paginatedRecordsWithLimit = paginatedRecordsWithLimit;
function paginatedResponsesWithLimit(request, listCall, limitedRecord) {
    return __awaiter(this, void 0, void 0, function* () {
        let records = [];
        const response = yield paginateResponses(request, listCall, limitedRecord);
        let iterator = yield response.next();
        let record = iterator.value;
        while (iterator.done === false) {
            record = iterator.value;
            records.push(record);
            iterator = yield response.next();
        }
        return records;
    });
}
exports.paginatedResponsesWithLimit = paginatedResponsesWithLimit;
// Not all function contains a property called ocpNextPage. ObjectStorage for example have an equivalence in listObject.nextStartWith
function findNextPageToken(res) {
    if ("listObjects" in res) {
        return res.listObjects.nextStartWith;
    }
    return res.opcNextPage;
}
// Not all function contain a property called 'page'. Object storage for example, uses 'start' to fetch the next pagination.
function findSetPageToken(req, nextPageToken) {
    if (req.start) {
        req.start = nextPageToken;
    }
    else {
        req.page = nextPageToken;
    }
}
function findResult(res) {
    if (res.listObjects && res.listObjects.objects) {
        return res.listObjects.objects;
    }
    return res.items;
}
//# sourceMappingURL=paginators.js.map