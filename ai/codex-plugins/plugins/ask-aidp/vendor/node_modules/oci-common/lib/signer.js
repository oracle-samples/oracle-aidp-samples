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
Object.defineProperty(exports, "__esModule", { value: true });
exports.DefaultRequestSigner = exports.SignerRequest = void 0;
const helper_1 = require("./helper");
const jssha = require("jssha");
const sshpk_1 = require("sshpk");
const UrlParser = require("url");
const delegate_auth_provider_1 = require("./auth/helpers/delegate-auth-provider");
// tslint:disable-next-line:no-var-requires
const httpSignature = require("http-signature");
const HEADER_CONTENT_SHA = "x-content-sha256";
const HEADER_CONTENT_LEN = "Content-Length";
const HEADER_CONTENT_TYPE = "Content-Type";
const OPC_OBO_TOKEN = "opc-obo-token";
// The Subtle crypto implementation in IE11 will silently fail to digest an empty string.
// We have to manually define that value here to avoid hanging forever
const EMPTY_SHA = "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=";
class SignerRequest {
    constructor(method, url, headers) {
        this.headers = headers;
        this.method = method;
        this.path = UrlParser.parse(url).path;
    }
    getHeader(name) {
        return this.headers.get(name);
    }
    setHeader(name, value) {
        this.headers.set(name, value);
    }
}
exports.SignerRequest = SignerRequest;
/**
 * The default implementation of [[RequestSigner]].
 */
class DefaultRequestSigner {
    /**
     * Construct an instance of [[DefaultRequestSigner]].
     * @param authenticationDetailsProvider the authentication details provider.
     */
    constructor(authenticationDetailsProvider) {
        this.authenticationDetailsProvider = authenticationDetailsProvider;
        this.delegationToken = "";
        this.privateKey = "";
        let options = {};
        if (this.authenticationDetailsProvider.getPassphrase()) {
            Object.assign(options, { passphrase: this.authenticationDetailsProvider.getPassphrase() });
        }
        // We can skip parsing private Key if we have an auth type that is not file based authentication.
        // We will also set DefaultRequestSigner's delegation token from authenticationDetailsProvider before
        // authenticationDetialsProvider gets changed to its true authentication provider.
        if (this.authenticationDetailsProvider.getAuthType &&
            this.authenticationDetailsProvider.getAuthType()) {
            this.privateKeyBuffer = null;
            const delegationToken = this.authenticationDetailsProvider.getDelegationToken();
            this.delegationToken = delegationToken;
            return;
        }
        this.privateKey = this.authenticationDetailsProvider.getPrivateKey();
        this.privateKeyBuffer = sshpk_1.parsePrivateKey(this.authenticationDetailsProvider.getPrivateKey(), "auto", options).toBuffer("pem", {});
    }
    /**
     * Sign the http request.
     * @param request http request.
     * @param forceExcludeBody exclude body or not.
     */
    signHttpRequest(request, forceExcludeBody = false) {
        return __awaiter(this, void 0, void 0, function* () {
            // Populate missing headers required for signing
            let options = {};
            if (this.authenticationDetailsProvider.getPassphrase()) {
                Object.assign(options, { passphrase: this.authenticationDetailsProvider.getPassphrase() });
            }
            if (!request.headers.has("host")) {
                const url = UrlParser.parse(request.uri);
                if (url.host) {
                    request.headers.set("host", url.host);
                }
                else {
                    throw new Error("Cannot parse host from url");
                }
            }
            if (!request.headers.has("x-date")) {
                request.headers.set("x-date", new Date().toUTCString());
            }
            var headersToSign = [...DefaultRequestSigner.headersToSign];
            if (this.delegationToken) {
                request.headers.set(OPC_OBO_TOKEN, this.delegationToken);
                headersToSign.push(OPC_OBO_TOKEN);
            }
            if (!forceExcludeBody &&
                DefaultRequestSigner.methodsThatRequireExtraHeaders.indexOf(request.method.toUpperCase()) !==
                    -1) {
                if (!request.headers.has(HEADER_CONTENT_TYPE)) {
                    request.headers.set(HEADER_CONTENT_TYPE, "application/json");
                }
                let contentLen = 0;
                const shaObj = new jssha("SHA-256", "TEXT");
                if (request.body) {
                    const bodyStringContent = yield helper_1.getStringFromRequestBody(request.body);
                    shaObj.update(bodyStringContent);
                    request.headers.set(HEADER_CONTENT_SHA, shaObj.getHash("B64"));
                    contentLen = Buffer.byteLength(bodyStringContent, "utf8");
                }
                if (contentLen == 0) {
                    // if buffer is empty, it can only be an empty string payload
                    request.headers.set(HEADER_CONTENT_SHA, EMPTY_SHA);
                }
                if (!request.headers.has(HEADER_CONTENT_LEN)) {
                    request.headers.set(HEADER_CONTENT_LEN, `${contentLen}`);
                }
                headersToSign = headersToSign.concat(HEADER_CONTENT_TYPE, HEADER_CONTENT_LEN, HEADER_CONTENT_SHA);
            }
            // Always make the check to see if there is a true authenticationDetailsProvider to use
            const provider = yield delegate_auth_provider_1.delegateAuthProvider(this.authenticationDetailsProvider);
            if (provider) {
                this.authenticationDetailsProvider = provider;
            }
            const keyId = yield this.authenticationDetailsProvider.getKeyId();
            // Check if privateKey exists or if the authenticationDetailsProvider's private key have changed.
            let authPrivateKey = this.authenticationDetailsProvider.getPrivateKey();
            if (!this.privateKey || this.privateKey !== authPrivateKey) {
                this.privateKey = authPrivateKey;
                this.privateKeyBuffer = sshpk_1.parsePrivateKey(authPrivateKey, "auto", options).toBuffer("pem", {});
            }
            httpSignature.sign(new SignerRequest(request.method, request.uri, request.headers), {
                key: this.privateKeyBuffer,
                keyId: keyId,
                headers: headersToSign
            });
            const authorizationHeader = request.headers.get("authorization");
            if (authorizationHeader) {
                request.headers.set("authorization", authorizationHeader.replace("Signature ", 'Signature version="1",'));
            }
            else {
                throw new Error("Unable to sign request");
            }
        });
    }
}
exports.DefaultRequestSigner = DefaultRequestSigner;
DefaultRequestSigner.headersToSign = ["x-date", "(request-target)", "host"];
DefaultRequestSigner.methodsThatRequireExtraHeaders = ["POST", "PUT", "PATCH"];
//# sourceMappingURL=signer.js.map