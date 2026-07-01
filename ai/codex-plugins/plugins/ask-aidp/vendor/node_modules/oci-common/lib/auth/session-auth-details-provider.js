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
Object.defineProperty(exports, "__esModule", { value: true });
exports.SessionAuthDetailProvider = void 0;
const signer_1 = require("../signer");
const http_1 = require("../http");
const request_generator_1 = require("../request-generator");
const config_file_auth_1 = require("./config-file-auth");
const helper_1 = require("../helper");
const region_1 = require("../region");
class SessionAuthDetailProvider extends config_file_auth_1.ConfigFileAuthenticationDetailsProvider {
    constructor(configurationFilePath, profile) {
        super(configurationFilePath, profile);
    }
    getKeyId() {
        return __awaiter(this, void 0, void 0, function* () {
            return "ST$" + (yield this.getSecurityToken());
        });
    }
    getSecurityToken() {
        return __awaiter(this, void 0, void 0, function* () {
            return this.sessionToken;
        });
    }
    refreshSessionToken() {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                const signer = new signer_1.DefaultRequestSigner(this);
                const client = new http_1.FetchHttpClient(signer);
                const regionId = this.getRegion().regionId;
                const region = region_1.Region.fromRegionId(regionId);
                const secondLevelDomain = region.realm.secondLevelDomain;
                const request = yield request_generator_1.composeRequest({
                    baseEndpoint: `https://auth.${regionId}.${secondLevelDomain}`,
                    path: `/v1/authentication/refresh`,
                    method: "POST",
                    defaultHeaders: { "content-type": "application/json" },
                    bodyContent: JSON.stringify({ currentToken: this.sessionToken })
                });
                const response = yield client.send(request);
                if (response.status === 200) {
                    const tokenJson = yield response.json();
                    const tokenStr = tokenJson.token;
                    this.sessionToken = tokenStr;
                    return this.sessionToken;
                }
                else if (response.status === 401) {
                    const errBody = yield helper_1.handleErrorBody(response);
                    throw new Error(`Authentication Error calling Identity to refresh token. Error: ${JSON.stringify(errBody)}`);
                }
                else {
                    const errBody = yield helper_1.handleErrorBody(response);
                    throw new Error(`Token cannot be refreshed. Error: ${JSON.stringify(errBody)}`);
                }
            }
            catch (e) {
                throw new Error(`Failed to refresh the session token due to ${e}`);
            }
        });
    }
}
exports.SessionAuthDetailProvider = SessionAuthDetailProvider;
//# sourceMappingURL=session-auth-details-provider.js.map