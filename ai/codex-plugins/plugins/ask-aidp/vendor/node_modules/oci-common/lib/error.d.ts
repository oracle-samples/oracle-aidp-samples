export declare class OciError extends Error {
    statusCode: number;
    serviceCode: string;
    message: string;
    opcRequestId: string | null;
    targetService: string;
    operationName: string;
    timestamp: string;
    requestEndpoint: string;
    clientVersion: string;
    loggingTips: string;
    troubleshootingTips: string;
    constructor(statusCode: number, serviceCode: string, message: string, opcRequestId: string | null, targetService: string, operationName: string, timestamp: string, requestEndpoint: string, apiReferenceLink: string);
}
