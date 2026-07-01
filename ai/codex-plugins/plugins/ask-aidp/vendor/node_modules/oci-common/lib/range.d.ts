/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
export declare class Range {
    private _startByte;
    private _endByte;
    private _contentLength;
    /**
     * Construct an instance of [[Range]].
     * @param startByte The start byte, if given.
     * @param endByte  The end byte, if given.
     * @param contentLength The content length as returned by the server, or null if unknown.
     */
    constructor(_startByte: number | null, _endByte: number | null, _contentLength: number | null);
    get startByte(): number | null;
    get endByte(): number | null;
    get contentLength(): number | null;
    /**
     * Parse the "content-range" header per https://tools.ietf.org/html/rfc7233#section-4.2.
     * <p>
     * Note, this is only for successful responses (206).  It does not handle 416 (range not satisfiable).
     * @param value The response header value.
     * @return A new Range header.
     */
    static parse(value: string): Range | undefined;
}
