import axios from "axios";
//import { LoggedInUser, User } from "./LoggedInUser";

// This file exports a function getConnection(), which returns the headers
// needed to talk to our Parse Server backend db.
// It keeps track of whether we're working with dev/staging or production or
// (via a one-line code change) a local database, and also stores and returns
// the token we get from parse-server when authorized as a particular user.
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
    //return local;
    //return dev;
    return prod;
}
