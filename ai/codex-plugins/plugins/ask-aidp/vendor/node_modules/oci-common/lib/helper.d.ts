/// <reference types="node" />
import { OciError } from "./error";
import { Readable } from "stream";
import { RequestParams } from "./request-generator";
export declare function mapContainer(obj: {
    [k: string]: any;
}, getJsonObj: Function): object;
export declare function handleErrorResponse(response: Response, body: any, targetService: string, operationName: string, timestamp: string, endpoint: string, apiReferenceLink: string): OciError;
export declare function handleErrorBody(response: Response): Promise<string | object>;
export declare function convertStringToType(str: string | null, expectedType: string): any;
export declare function getStringFromResponseBody(body: any): Promise<string>;
export declare function readStringFromReadable(readable: Readable): Promise<string>;
export declare function getSignerAndReqBody(body?: any, forceExcludeBody?: boolean): {
    signerBody: any;
    requestBody: any;
};
export declare function addAdditionalHeaders(headers: Headers, params: RequestParams): void;
export declare function autoDetectContentLengthAndReadBody(headers: Headers, params: RequestParams): Promise<any>;
export declare function formatDateToRFC3339(date: Date): string;
export declare function getStringFromRequestBody(body: any): Promise<string>;
export declare function isReadableStream(body: any): Boolean;
export declare function byteLength(input: any): any;
