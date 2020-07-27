import { environment } from "../main";
import { Environment } from "../Environment";

// This file exports a function getConnection(), which returns the headers
// needed to talk to our Parse Server backend db.
// The environment is based on the setting in main.ts.
interface IConnection {
    headers: {
        "Content-Type": string;
        "X-Parse-Application-Id": string;
        "X-Parse-Session-Token"?: string;
    };
    url: string;
}
const prod: IConnection = {
    headers: {
        "Content-Type": "text/json",
        "X-Parse-Application-Id": "R6qNTeumQXjJCMutAJYAwPtip1qBulkFyLefkCE5",
    },
    url: "https://bloom-parse-server-production.azurewebsites.net/parse/",
};
// eslint-disable-next-line @typescript-eslint/no-unused-vars
const dev: IConnection = {
    headers: {
        "Content-Type": "text/json",
        "X-Parse-Application-Id": "yrXftBF6mbAuVu3fO6LnhCJiHxZPIdE7gl1DUVGR",
    },
    url: "https://bloom-parse-server-develop.azurewebsites.net/parse/",
};

const local: IConnection = {
    headers: {
        "Content-Type": "text/json",
        "X-Parse-Application-Id": "myAppId",
    },
    url: "http://localhost:1337/parse/",
};

export function getConnection(): IConnection {
    switch (environment) {
        case Environment.Dev:
            return dev;
        case Environment.Prod:
            return prod;
        case Environment.Local:
            return local;
        default:
            throw new Error("Invalid environment set");
    }
}
