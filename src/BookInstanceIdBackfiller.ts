import axios, { AxiosRequestConfig, AxiosResponse } from "axios";
import { getConnection as getParseConnection } from "./connections/ParseServerConnection";
import PostgresConnection from "./connections/PostgresConnection";
import { environment } from "./main";
import { Environment } from "./Environment";
import { performance } from "perf_hooks";

export class BookInstanceIdBackfiller {
    private sqlDb: PostgresConnection;

    // This controls whether or not the update will be performed.
    // false = Just print out the query
    // true = Actually execute the query
    private reallyRunUpdate: boolean = false;

    public constructor() {
        this.sqlDb = PostgresConnection.getInstance();
    }

    public async backfill(): Promise<void> {
        console.log("Starting backfill function.");

        const startTime = performance.now();
        const [parseResponse, sqlResults] = await Promise.all([
            this.getParseCall(),
            this.getSqlCall(),
        ]);
        this.printTimeTakenSince(startTime, "Time to get Parse/SQL results");

        if (
            parseResponse.status !== 200 ||
            !parseResponse.data ||
            !parseResponse.data.results
        ) {
            throw new Error(
                "Parse request returned with problems. Status was " +
                    parseResponse.status
            );
        }

        // Start processing the Parse response, now that we know it succeeded.
        const books = this.getBooksFromResponse(parseResponse).filter((x) => {
            return !!x.title && !!x.bookInstanceId;
        });

        const parseTuples = books.map((book) => {
            return {
                title: book.title,
                bookInstanceId: book.bookInstanceId,
            };
        });
        const parseMap = this.getBookInstanceIdsByTitle(parseTuples);

        // Now start processing the SQL response
        console.log("Num Results from SQL: " + sqlResults.length);

        const sqlPagesReadTuples = sqlResults.map((row) => {
            return {
                title: row.title,
                bookInstanceId: row.book_instance_id,
            };
        });

        const sqlMap = this.getBookInstanceIdsByTitle(sqlPagesReadTuples);

        // Merge the two
        const combinedMap = this.mergeMaps(parseMap, sqlMap);
        console.log(`parseMap.size = ${parseMap.size}`);
        console.log(`sqlMap.size = ${sqlMap.size}`);
        console.log(`combinedMap.size = ${combinedMap.size}`);

        const [
            safeInstanceIdsToUpdate,
            ambiguousTitles,
        ] = this.getSafeVsAmbiguousUpdates(combinedMap);

        console.log(
            `safeInstanceIdsToUpdate.size = ${safeInstanceIdsToUpdate.size}`
        );

        const updateDatabaseTableStartTime = performance.now();
        await this.updateDatabaseTables(safeInstanceIdsToUpdate);
        this.printTimeTakenSince(
            updateDatabaseTableStartTime,
            "updateDatabaseTable() Time"
        );

        const updateInstanceIdsForNullOnlyStartTime = performance.now();
        await this.updateInstanceIdsForNullOnly(
            safeInstanceIdsToUpdate,
            ambiguousTitles
        );
        this.printTimeTakenSince(
            updateInstanceIdsForNullOnlyStartTime,
            "updateInstanceIdsForNullOnly() Time"
        );

        this.printTimeTakenSince(startTime, "Total Time");
    }

    private printTimeTakenSince(startTime: number, message: string): void {
        const timeTaken = (performance.now() - startTime) / 1000;
        console.log(`${message}: ${timeTaken.toFixed(1)} seconds`);
    }

    // Returns a promise which gets the Parse results for each book in Bloom LIbrary with its title and instanceId
    private getParseCall(): Promise<AxiosResponse<any>> {
        const connection = getParseConnection();
        const axiosRequestConfig: AxiosRequestConfig = {
            headers: connection.headers,
            params: {
                keys: "objectId,bookInstanceId,title",
                // Need to specify limit, or else it'll default to 100, which is probably not what is wanted.
                limit: 1000000,
            },
        };

        const url = `${connection.url}/classes/books`;
        const parseCall = axios.get(url, axiosRequestConfig).catch((error) => {
            throw new Error("Parse Request failed: " + error.message);
        });

        return parseCall;
    }

    private getSchemaNames(): string[] {
        switch (environment) {
            case Environment.Dev:
            case Environment.Local:
                return ["bloomreadertest"];
            case Environment.Prod:
                return ["bloomreader", "bloomreaderbeta"]; // the real thing
            default:
                throw new Error("Invalid environment set");
        }
    }

    private getTablesToUpdate(): string[] {
        return [
            "pages_read",
            "book_or_shelf_opened",
            "comprehension",
            "questions_correct",
        ];
    }

    private getSchemaTablesToUpdate(): string[] {
        const arr: string[] = [];
        const schemasToUpdate = this.getSchemaNames();
        const tablesToUpdate = this.getTablesToUpdate();
        for (const schemaName of schemasToUpdate) {
            for (const table of tablesToUpdate) {
                arr.push(`${schemaName}.${table}`);
            }
        }

        switch (environment) {
            case Environment.Dev:
            case Environment.Local:
                arr.push("bloomlibrary_test.download_book");
                break;
            case Environment.Prod:
                arr.push("bloomlibrary_org.download_book");
                break;
            default:
                throw new Error("Invalid environment set");
        }

        return arr;
    }

    private getTitleFieldForTable(tableName: string): string {
        if (
            tableName === "download_book" ||
            tableName.endsWith(".download_book")
        ) {
            return "book_title";
        } else {
            return "title";
        }
    }

    // Returns a promise to execute the relevant SQL query
    private getSqlCall(): Promise<any[]> {
        const query = this.getSqlLookupQuery();
        const sqlCall = this.sqlDb.executeQuery(query).catch((error) => {
            throw new Error("Postgresql Request failed: " + error.message);
        });

        return sqlCall;
    }

    private getSqlLookupQuery(): string {
        const schemaTables = this.getSchemaTablesToUpdate();
        const subQueries = schemaTables.map((schemaTable) => {
            const titleFieldName = this.getTitleFieldForTable(schemaTable);
            return `SELECT ${titleFieldName} AS title, book_instance_id FROM ${schemaTable} WHERE ${titleFieldName} is not null AND book_instance_id is not null`;
        });

        const combinedQuery = subQueries.join(" UNION "); // UNION implies DISTINCT
        return combinedQuery;
    }

    private getBooksFromResponse(response: AxiosResponse<any>): IBook[] {
        // Just assuming that the data is in the expected format, which is where each result is an IBook
        return response.data.results as IBook[];
    }

    private getBookInstanceIdsByTitle(
        tuples: { title: string; bookInstanceId: string }[]
    ): Map<string, Set<string>> {
        const map = new Map<string, Set<string>>();
        tuples.forEach((tuple) => {
            const matchingInstanceIds = map.get(tuple.title);
            if (!matchingInstanceIds) {
                map.set(
                    tuple.title,
                    new Set<string>([tuple.bookInstanceId])
                );
            } else {
                matchingInstanceIds.add(tuple.bookInstanceId);
            }
        });

        return map;
    }

    // Merges two maps of title -> Set<bookInstanceId> together.
    // Returns a new map. The old map is not affected.
    private mergeMaps(
        map1: Map<string, Set<string>>,
        map2: Map<string, Set<string>>
    ): Map<string, Set<string>> {
        const combinedMap = new Map(map1);
        map2.forEach((bookInstanceIdSet2, title2) => {
            const existingSet = combinedMap.get(title2);

            if (!existingSet) {
                const newSet = new Set<string>(bookInstanceIdSet2);
                combinedMap.set(title2, newSet);
            } else {
                bookInstanceIdSet2.forEach((instanceId: string) => {
                    existingSet.add(instanceId);
                });
            }
        });

        return combinedMap;
    }

    // private prettyPrintMap(map: Map<string, Set<string>>) {
    //     map.forEach((instanceIdSet, title) => {
    //         console.log(
    //     });
    // }

    private getSafeVsAmbiguousUpdates(
        titleToInstanceIdSet: Map<string, Set<string>>
    ): [Map<string, string>, string[]] {
        // Right now, we consider it to be safe if there is only one distinct instance ID a title is associated with.
        //
        // TODO: We should also consider a safe update to be if there is only one distinct non-auto-generated instanceId.
        const safeUpdates = new Map<string, string>();
        const ambiguousTitles: string[] = [];
        console.log("=====");
        console.log("=====");
        titleToInstanceIdSet.forEach((instanceIdSet, title) => {
            if (instanceIdSet.size === 1) {
                const instanceId = instanceIdSet.values().next().value;
                safeUpdates.set(title, instanceId);
            } else {
                // A list of non-safe updates to try to deal with manually?
                console.log(
                    `Title with multiple distinct instance ids: ${title}, NumMatches: ${instanceIdSet.size}`
                );

                ambiguousTitles.push(title);
            }
        });

        console.log("AmbiguousCount = " + ambiguousTitles.length);
        return [safeUpdates, ambiguousTitles];
    }

    private async updateDatabaseTables(
        safeInstanceIdsToUpdate: Map<string, string>
    ) {
        const schemaTablesToUpdate = this.getSchemaTablesToUpdate();

        for (const schemaTable of schemaTablesToUpdate) {
            await this.updateDatabaseTable(
                schemaTable,
                safeInstanceIdsToUpdate
            );
        }

        // Not sure if this is any faster?
        // Will the DB be upset if it has to work on multiple queries at once?
        // One downside of this approach is it will make the logs more confusing to read.
        //
        // const updateTasks = schemaTablesToUpdate.map((schemaTable) => {
        //     return this.updateDatabaseTable(
        //         schemaTable,
        //         safeInstanceIdsToUpdate
        //     );
        // });

        // return Promise.all(updateTasks);
    }

    private async updateDatabaseTable(
        table: string,
        safeInstanceIdsToUpdate: Map<string, string>
    ) {
        const updateQueries = await this.generateUpdateSqlQuery(
            safeInstanceIdsToUpdate,
            table
        );

        if (updateQueries && updateQueries.length) {
            await this.performUpdate(table, updateQueries);
        } else {
            console.log("No updates for " + table);
        }
    }

    private async generateUpdateSqlQuery(
        titleToInstanceIds: Map<string, string>,
        tableName: string
    ): Promise<string[]> {
        if (titleToInstanceIds.size <= 0) {
            return [];
        }

        const titleField = this.getTitleFieldForTable(tableName);

        const titlesWhichNeedUpdate: string[] = await this.getTitlesWhichNeedUpdate(
            tableName
        );

        const queries: string[] = [];
        titleToInstanceIds.forEach((instanceId, title) => {
            // // Crude check against SQL injection
            // if (title && title.indexOf(";") >= 0) {
            //     console.error(
            //         `Did not update title ${title} because it contains a semi-colon`
            //     );
            //     return;
            // }

            const sanitizedTitle = title.replace(/'/g, "''");
            if (titlesWhichNeedUpdate.includes(title)) {
                const query = `UPDATE ${tableName} SET book_instance_id = '${instanceId}' WHERE ${titleField} = '${sanitizedTitle}' AND book_instance_id is null;`;
                queries.push(query);
            }
        });
        return queries;
    }

    private async getTitlesWhichNeedUpdate(
        tableName: string
    ): Promise<string[]> {
        const titleField = this.getTitleFieldForTable(tableName);

        const query = `SELECT DISTINCT ${titleField} AS title FROM ${tableName} WHERE book_instance_id IS NULL`;
        const sqlResults = await this.sqlDb
            .executeQuery(query)
            .catch((error) => {
                throw new Error("Postgresql Request failed: " + error.message);
            });

        return sqlResults.map((r) => r.title);
    }

    private async performUpdate(tableName: string, updateQueries: string[]) {
        console.log("=====");
        console.log("=====");
        console.log(
            `${tableName}: Num update queries = ${updateQueries.length}`
        );

        const titleField = this.getTitleFieldForTable(tableName);
        const queryPart1 = `SELECT COUNT(*) AS cnt, COUNT(DISTINCT ${titleField}) AS count_distinct_problem_titles FROM ${tableName} WHERE book_instance_id IS NULL; `;

        const queryPart2Real = updateQueries.join(" ");
        const queryPart2Fake = `SELECT * FROM ${tableName} limit 1; SELECT * FROM ${tableName} limit 1;`;
        const queryPart2 = this.reallyRunUpdate
            ? queryPart2Real
            : queryPart2Fake;

        console.log("=====");
        console.log(`${tableName}: update statements: ${queryPart2Real}`);

        const queryPart3 = queryPart1;

        const combinedQuery = queryPart1 + queryPart2 + queryPart3;

        const results = await this.sqlDb.executeMultiStatementQuery(
            combinedQuery
        );
        const beforeRowCount = results[0].rows[0].cnt;
        const afterRowCount = results[results.length - 1].rows[0].cnt;
        const beforeTitleCount =
            results[0].rows[0].count_distinct_problem_titles;
        const afterTitleCount =
            results[results.length - 1].rows[0].count_distinct_problem_titles;

        console.log("=====");
        console.log(
            `${tableName}: NumProblemRows Before: ${beforeRowCount}, NumProblemRows After: ${afterRowCount}, NumUpdated=${
                beforeRowCount - afterRowCount
            }`
        );
        console.log(
            `${tableName}: NumDistinctProblemTitles Before: ${beforeTitleCount}, NumDistinctProblemTitles After: ${afterTitleCount}, NumUpdated=${
                beforeTitleCount - afterTitleCount
            }`
        );
    }

    private async updateInstanceIdsForNullOnly(
        safeInstanceIdsToUpdate: Map<string, string>,
        ambiguousTitles: string[]
    ) {
        console.log(
            "===\nGenerating fake book-instance-ids for titles with no non-null bookInstanceIds associated with them\n==="
        );
        const nullOnlyTitles = await this.getTitlesWithOnlyNullInstanceIds(
            safeInstanceIdsToUpdate,
            ambiguousTitles
        );

        console.log(
            "Number of titles with no non-null bookInstanceIds: " +
                nullOnlyTitles.length
        );

        const titlesToNewIds = new Map<string, string>();
        nullOnlyTitles.forEach((title) => {
            const id = this.getFakeId();
            titlesToNewIds.set(title, id);
        });

        await this.updateDatabaseTables(titlesToNewIds);
    }

    // Retrieves the set of titles without instance IDs from the SQL database,
    // then removes the titles with 1 or more instance IDs associated with them
    // in either other records in the SQL database OR th PARSE database.
    // These are therefore titles for which we need an instanceID but have no way
    // to obtain a correct one.
    // (It really just wants a set of titles are associated with some instanceId,
    // but the one caller has these two separate data structures so we made it accept them.)
    private async getTitlesWithOnlyNullInstanceIds(
        titlesWithKnownInstanceIds: Map<string, string>, // That is, exactly 1 non-null isntance id is associated with this title.
        titlesWithAmbiguousInstanceIds: string[] // That is, 2+ non-null instance ids are associated with this title
    ): Promise<string[]> {
        // Massage the input parameters into the ideal format
        const titlesWithNonNullInstanceIds = new Set<string>(
            titlesWithKnownInstanceIds.keys()
        );

        titlesWithAmbiguousInstanceIds.forEach((title) => {
            titlesWithNonNullInstanceIds.add(title);
        });

        return this.getTitlesWithOnlyNullInstanceIdsInternal(
            titlesWithNonNullInstanceIds
        );
    }

    // Retrieves the set of titles without instance IDs from the SQL database,
    // then removes the titles with 1 or more instance IDs associated with them
    // in either other records in the SQL database OR th PARSE database.
    private async getTitlesWithOnlyNullInstanceIdsInternal(
        titlesWithNonNullInstanceIds: Set<string>
    ): Promise<string[]> {
        const queryComponents: string[] = [];

        const schemaTablesToUpdate = this.getSchemaTablesToUpdate();
        for (const schemaTable of schemaTablesToUpdate) {
            // Gets all titles from SQL
            const titleField = this.getTitleFieldForTable(schemaTable);
            const sqlQuery = `SELECT ${titleField} AS title FROM ${schemaTable} WHERE ${titleField} IS NOT NULL`;
            queryComponents.push(sqlQuery);
        }

        const combinedQuery = queryComponents.join(" UNION "); // FYI, Union performs deduplication

        const results = await this.sqlDb.executeQuery(combinedQuery);

        // Now filter out titles that we know have one or more non-null bookInstanceIds across both Parse and SQL
        // Note: Even though you could write a SQL query to get titles which don't have any non-null book_instance_ids IN THE SQL DATABASE,
        //       what we actually need is those that are lacking them in both.
        const titleCandidates = results.map((result) => result.title);
        const nullOnlyTitles = titleCandidates.filter((title) => {
            return !titlesWithNonNullInstanceIds.has(title);
        });

        return nullOnlyTitles;
    }

    // Generates a fake book-instance-id for books where we cannot find any existing book-instance-id.
    private getFakeId(): string {
        return `auto_${this.createUuid()}`;
    }

    // from http://stackoverflow.com/questions/105034/create-guid-uuid-in-javascript
    private createUuid(): string {
        // http://www.ietf.org/rfc/rfc4122.txt
        const s: string[] = [];
        const hexDigits = "0123456789abcdef";
        for (let i = 0; i < 36; i++) {
            s[i] = hexDigits.substr(Math.floor(Math.random() * 0x10), 1);
        }
        s[14] = "4"; // bits 12-15 of the time_hi_and_version field to 0010
        // tslint:disable-next-line: no-bitwise
        s[19] = hexDigits.substr((s[19].charCodeAt(0) & 0x3) | 0x8, 1); // bits 6-7 of the clock_seq_hi_and_reserved to 01
        s[8] = s[13] = s[18] = s[23] = "-";

        const uuid = s.join("");
        return uuid;
    }
}
