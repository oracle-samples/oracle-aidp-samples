"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.byteLength = exports.isReadableStream = exports.getStringFromRequestBody = exports.formatDateToRFC3339 = exports.autoDetectContentLengthAndReadBody = exports.addAdditionalHeaders = exports.getSignerAndReqBody = exports.readStringFromReadable = exports.getStringFromResponseBody = exports.convertStringToType = exports.handleErrorBody = exports.handleErrorResponse = exports.mapContainer = void 0;
const error_1 = require("./error");
const range_1 = require("./range");
const stream_1 = require("stream");
const headers_1 = require("./headers");
const utils_1 = require("./utils");
const chunker_1 = __importDefault(require("./chunker"));
function mapContainer(obj, getJsonObj) {
    const constructedObj = {};
    for (let key in obj) {
        constructedObj[key] = getJsonObj(obj[key]);
    }
    return constructedObj;
}
exports.mapContainer = mapContainer;
function handleErrorResponse(response, body, targetService, operationName, timestamp, endpoint, apiReferenceLink) {
    const statusCode = response.status || -1;
    const requestId = response.headers.get("opc-request-id");
    if (body && body.code && body.message) {
        return new error_1.OciError(statusCode, body.code, body.message, requestId, targetService, operationName, timestamp, endpoint, apiReferenceLink);
    }
    else if (typeof body == "string" && body.length > 0) {
        return new error_1.OciError(statusCode, "None", body, requestId, targetService, operationName, timestamp, endpoint, apiReferenceLink);
    }
    else if (response.statusText && response.statusText.length > 0) {
        // There is no body text but statusText exists
        return new error_1.OciError(statusCode, "None", response.statusText, requestId, targetService, operationName, timestamp, endpoint, apiReferenceLink);
    }
    else {
        return new error_1.OciError(statusCode, "None", "unknown reason.", requestId, targetService, operationName, timestamp, endpoint, apiReferenceLink);
    }
}
exports.handleErrorResponse = handleErrorResponse;
function handleErrorBody(response) {
    return __awaiter(this, void 0, void 0, function* () {
        let data = yield response.text();
        //  Try to parse string as an object
        try {
            data = JSON.parse(data);
        }
        catch (err) {
            return data;
        }
        return data;
    });
}
exports.handleErrorBody = handleErrorBody;
function convertStringToType(str, expectedType) {
    if (str == null)
        return str;
    expectedType = expectedType.toLowerCase();
    switch (expectedType) {
        case "string":
            return str;
        case "number":
            return Number(str);
        case "date":
            return formatDateToRFC3339(new Date(str));
        case "common.Range":
            return range_1.Range.parse(str);
    }
}
exports.convertStringToType = convertStringToType;
// get string content from response body
function getStringFromResponseBody(body) {
    return __awaiter(this, void 0, void 0, function* () {
        if (typeof body === "string") {
            return body;
        }
        if (body instanceof stream_1.Readable) {
            // body is a stream type
            return readStringFromReadable(body);
        }
        // else if (body instanceof Blob) {
        //   // body is a blob type
        //   return readStringFromBlob(body);
        // } else if (body instanceof ReadableStream) {
        //   // body is a fetch readableStream type
        //   return readStringFromFetchReadableStream(body);
        // }
        else {
            // unknown type, unable to read body content for signing, reject it
            throw new Error("Unable to read body content to sign the request");
        }
    });
}
exports.getStringFromResponseBody = getStringFromResponseBody;
// read string from Readable asynchronously, return a string content of it
function readStringFromReadable(readable) {
    return __awaiter(this, void 0, void 0, function* () {
        let contentBuffer = [];
        let size = 0;
        const MEMIBYTES = 1024 * 1024;
        const sizeLimit = 2000 * MEMIBYTES;
        // set the encoding to return string instead of Buffer
        readable.setEncoding("utf8");
        return new Promise((resolve, reject) => {
            readable.on("end", () => {
                resolve(contentBuffer.join(""));
            });
            readable.on("data", chunk => {
                if (size > sizeLimit) {
                    throw Error("Tried to read stream but content length is greater than 2GB.");
                }
                contentBuffer.push(chunk);
                size += chunk.length;
            });
            readable.on("error", err => {
                // if error happened, it will be catched at http signer global error handling
                reject(err);
            });
        });
    });
}
exports.readStringFromReadable = readStringFromReadable;
// read string from fetch ReadbaleString asynchronously, return a string content of it
// export async function readStringFromFetchReadableStream(readable: ReadableStream): Promise<string> {
//   let contentBuffer: Array<string> = [];
//   const reader = readable.getReader();
//   const decoder = new TextDecoder("utf-8");
//   return new Promise<string>((resolve, reject) => {
//     reader
//       .read()
//       .then(function processText({ done, value }): any {
//         if (done) {
//           // reading stream done, resolve it
//           resolve(contentBuffer.join(""));
//         }
//         // put each chunk into a buffer
//         contentBuffer.push(decoder.decode(value));
//         // read more data and call processText function again to read more
//         return reader.read().then(processText);
//       })
//       .catch(function(e) {
//         // reject if has error
//         reject(e);
//       });
//   });
// }
// read string from Blob asynchronously, return a string content of it
// export async function readStringFromBlob(blob: Blob): Promise<string> {
//   const reader = new FileReader();
//   return new Promise<string>((resolve, reject) => {
//     reader.onerror = err => {
//       reader.abort();
//       reject(err);
//     };
//     reader.onload = () => {
//       // read as Text is called, so this will be a string
//       resolve(reader.result as string);
//     };
//     // utf-8 default encoding is used here
//     reader.readAsText(blob);
//   });
// }
// returns duplicated body for separate consumption by signer and Fetch Request
function getSignerAndReqBody(body, forceExcludeBody) {
    const singerAndReqBody = { signerBody: undefined, requestBody: undefined };
    // If body does not exist or empty body
    if (!body || utils_1.isEmpty(body)) {
        return singerAndReqBody;
    }
    // If body is excluded for signing, no need to send signer body
    if (forceExcludeBody) {
        singerAndReqBody.requestBody = body;
        return singerAndReqBody;
    }
    // if body of type string, can be duplicated.
    if (typeof body === "string") {
        return { signerBody: body, requestBody: body };
    }
    // If body instance of Readable , duplicate the stream for signer and request body
    else if (body instanceof stream_1.Readable) {
        const signerbody = body.pipe(new stream_1.PassThrough());
        const reqBody = body.pipe(new stream_1.PassThrough());
        return { signerBody: signerbody, requestBody: reqBody };
    }
    // //if body instance of blob, can be duplicated.
    // else if (body instanceof Blob) {
    //   return { signerBody: body, requestBody: body };
    // }
    // // if body instance of ReadableStream, tee() it.
    // else if (body instanceof ReadableStream) {
    //   // body.tee() not supported in IE.
    //   // https://jira.oci.oraclecorp.com/browse/DEX-7126
    //   const duplicateStream = body.tee();
    //   return { signerBody: duplicateStream[0], requestBody: duplicateStream[1] };
    // }
    // unknown type, unable to read body content.
    else
        throw new Error("Unable to read body content");
}
exports.getSignerAndReqBody = getSignerAndReqBody;
function addAdditionalHeaders(headers, params) {
    headers_1.addOpcRequestId(headers);
    headers_1.addUserAgent(headers);
}
exports.addAdditionalHeaders = addAdditionalHeaders;
function autoDetectContentLengthAndReadBody(headers, params) {
    return __awaiter(this, void 0, void 0, function* () {
        // Auto Detect content-length if needed, also read binary content if stream length cannot be determined.
        const reqHeaders = params.headerParams;
        if (reqHeaders) {
            const shouldReadBodyAndCalculateContentLength = ("content-length" in reqHeaders && reqHeaders["content-length"] === undefined) ||
                ("Content-Length" in reqHeaders && reqHeaders["Content-Length"] === undefined) ||
                params.backupBinaryBody;
            if (shouldReadBodyAndCalculateContentLength) {
                const { body, contentLength } = yield calculateContentLengthAndBodyContent(params.bodyContent);
                headers.append("content-length", String(contentLength));
                return body;
            }
        }
    });
}
exports.autoDetectContentLengthAndReadBody = autoDetectContentLengthAndReadBody;
// Helper method to auto detect content-length if not given.
function calculateContentLengthAndBodyContent(body) {
    return __awaiter(this, void 0, void 0, function* () {
        try {
            const dataFeeder = chunker_1.default(body, Number.MAX_SAFE_INTEGER);
            const dataPart = (yield dataFeeder.next()).value;
            const content = dataPart.data;
            const contentLength = dataPart.size;
            return { body: content, contentLength };
        }
        catch (e) {
            throw Error("SDK could not calculate contentLength from the request stream, please add contentLength and try again.");
        }
    });
}
// Helper method to format Date Objects to RFC3339 timestamp string.
function formatDateToRFC3339(date) {
    return (date.getFullYear() +
        "-" +
        ("0" + (date.getMonth() + 1)).slice(-2) +
        "-" +
        ("0" + date.getDate()).slice(-2) +
        "T" +
        date.getHours() +
        ":" +
        ("0" + date.getMinutes()).slice(-2) +
        ":" +
        ("0" + date.getSeconds()).slice(-2) +
        "Z");
}
exports.formatDateToRFC3339 = formatDateToRFC3339;
// get string content from body
function getStringFromRequestBody(body) {
    return __awaiter(this, void 0, void 0, function* () {
        if (typeof body === "string") {
            return body;
        }
        if (body instanceof stream_1.Readable) {
            // body is a stream type
            return readStringFromReadable(body);
        }
        // else if (body instanceof Blob) {
        //   // body is a blob type
        //   return readStringFromBlob(body);
        // } else if (body instanceof ReadableStream) {
        //   // body is a fetch readableStream type
        //   return readStringFromFetchReadableStream(body);
        // }
        else {
            // unknown type, unable to read body content for signing, reject it
            throw new Error("Unable to read body content to sign the request");
        }
    });
}
exports.getStringFromRequestBody = getStringFromRequestBody;
function isReadableStream(body) {
    // Check if the body object contains all property of a ReadableStream
    if (body.cancel && body.getReader && body.pipeThrough && body.pipeTo && body.tee) {
        return true;
    }
    return false;
}
exports.isReadableStream = isReadableStream;
function byteLength(input) {
    if (input === null || input === undefined)
        return 0;
    if (typeof input === "string")
        input = Buffer.from(input);
    if (typeof input.byteLength === "number") {
        return input.byteLength;
    }
    else if (typeof input.length === "number") {
        return input.length;
    }
    else if (typeof input.size === "number") {
        return input.size;
    }
    return undefined;
}
exports.byteLength = byteLength;
//# sourceMappingURL=helper.js.map