import axios, { AxiosRequestConfig, AxiosResponse } from "axios";
import { getConnection as getParseConnection } from "./connections/ParseServerConnection";
import PostgresConnection from "./connections/PostgresConnection";

export class BookInstanceIdBackfiller {
    private sqlDb: PostgresConnection;

    public constructor() {
        this.sqlDb = PostgresConnection.getInstance();
    }

    public async backfill(): Promise<void> {
        console.log("Starting backfill function.");

        const [parseResponse, sqlResults] = await Promise.all([
            this.getParseCall(),
            this.getSqlCall(),
        ]);

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
            return !!x.bookInstanceId;
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

        const safeInstanceIdsToUpdate = this.getSafeUpdates(combinedMap);
        console.log("Original length: " + safeInstanceIdsToUpdate.size);

        // Filter out the ones that aren't relevant to SQL
        // const filteredSafeInstanceIds = new Map<string, string>();
        // safeInstanceIdsToUpdate.forEach((instanceId, title) => {
        //     const instanceIdSet = sqlMap.get(title);
        //     const doesSqlHaveTitle = !!instanceIdSet;
        //     if (doesSqlHaveTitle && instanceIdSet && instanceIdSet.size <= 1) {
        //         const firstExistingValue = instanceIdSet.values().next().value;
        //         if (firstExistingValue !== instanceId) {
        //             console.log(`Setting ${title} to ${instanceId}`);
        //             filteredSafeInstanceIds.set(title, instanceId);
        //         } else {
        //             // console.log(
        //             //     `Skipping ${instanceId} because it already has the correct value.`
        //             // );
        //         }
        //     }
        // });

        //console.log("New length: " + filteredSafeInstanceIds.size);

        // ENHANCE: Think of a better way to make sure we get the right schema of the table
        const updateQueries = await this.generateUpdateSqlQuery(
            safeInstanceIdsToUpdate,
            "bloomreadertest.pages_read"
        );

        if (updateQueries && updateQueries.length) {
            console.log(
                "Update queries (top 10): " +
                    JSON.stringify(updateQueries.slice(0, 10))
            );

            await this.performUpdate(
                "bloomreadertest.pages_read",
                updateQueries
            );
            // TODO: Perform the DB update, when you're sure you got it right.
        }
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
        // console.log(
        //     "axiosRequestConfig: " + JSON.stringify(axiosRequestConfig)
        // );
        const parseCall = axios.get(url, axiosRequestConfig).catch((error) => {
            throw new Error("Parse Request failed: " + error.message);
        });

        return parseCall;
    }

    // Returns a promise to execute the relevant SQL query
    private getSqlCall(): Promise<any[]> {
        // ENHANCE: is there union distinct vs. union all?
        const schema = "bloomreader";
        //const schema = "bloomreadertest";
        const query =
            this.getSqlLookupQuery("bloomreader") +
            " UNION " +
            this.getSqlLookupQuery("bloomreaderbeta");
        const sqlCall = this.sqlDb.executeQuery(query).catch((error) => {
            throw new Error("Postgresql Request failed: " + error.message);
        });

        return sqlCall;
    }

    private getSqlLookupQuery(schema: string): string {
        return (
            `SELECT DISTINCT title, book_instance_id FROM ${schema}.pages_read WHERE book_instance_id is not null` +
            " UNION " +
            `SELECT DISTINCT title, book_instance_id FROM ${schema}.book_or_shelf_opened WHERE book_instance_id is not null` +
            " UNION " +
            `SELECT DISTINCT title, book_instance_id FROM ${schema}.comprehension WHERE book_instance_id is not null`
        );
    }

    private getBooksFromResponse(response: AxiosResponse<any>): IBook[] {
        // Just assuming that the data is in the expected format, which is where each result is an IBook
        return response.data.results as IBook[];
    }

    // // If more than one book has the same title, the map will store all of them in the array of books.
    // private getBooksByTitle(books: IBook[]): Map<string, IBook[]> {
    //     const map = new Map<string, IBook[]>();
    //     for (let i = 0; i < books.length; ++i) {
    //         const book = books[i];

    //         const matchingBooks: IBook[] | undefined = map.get(book.title);
    //         if (!matchingBooks) {
    //             map.set(book.title, [book]);
    //         } else {
    //             matchingBooks.push(book);
    //         }
    //     }

    //     return map;
    // }

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

    private getSafeUpdates(
        titleToInstanceIdSet: Map<string, Set<string>>
    ): Map<string, string> {
        // RIght now, we consider it to be safe if there is only one instance ID a title is associated with.
        const safeUpdates = new Map<string, string>();
        titleToInstanceIdSet.forEach((instanceIdSet, title) => {
            if (instanceIdSet.size === 1) {
                const instanceId = instanceIdSet.values().next().value;
                safeUpdates.set(title, instanceId);
            } else {
                // ENHANCE: Maybe we'd like to print out a list of non-safe updates to try to deal with manually?
                console.log();
                console.log("=====");
                console.log("Titles with multiple instance ids:");
                console.log(
                    `Title: ${title}, NumMatches: ${instanceIdSet.size}, Values=???`
                );
                console.log("=====");
                console.log();
            }
        });
        return safeUpdates;
    }

    private async generateUpdateSqlQuery(
        titleToInstanceIds: Map<string, string>,
        tableName: string
    ): Promise<string[]> {
        if (titleToInstanceIds.size <= 0) {
            return [];
        }

        const titlesWhichNeedUpdate: string[] = await this.getTitlesWhichNeedUpdate(
            tableName
        );

        const queries: string[] = [];
        titleToInstanceIds.forEach((instanceId, title) => {
            // Crude check against SQL injection
            if (title && title.indexOf(";") >= 0) {
                console.error(
                    `Did not update title ${title} because it contains a semi-colon`
                );
                return;
            }

            if (titlesWhichNeedUpdate.includes(title)) {
                const query = `UPDATE ${tableName} SET book_instance_id = '${instanceId}' WHERE title = '${title}' AND book_instance_id is null;`;
                queries.push(query);
            }
        });
        console.log("queries.length = " + queries.length);
        return queries;
    }

    private async getTitlesWhichNeedUpdate(
        tableName: string
    ): Promise<string[]> {
        const query = `SELECT DISTINCT title FROM ${tableName} WHERE book_instance_id IS NULL`;
        const sqlResults = await this.sqlDb
            .executeQuery(query)
            .catch((error) => {
                throw new Error("Postgresql Request failed: " + error.message);
            });

        return sqlResults.map((r) => r.title);
    }

    private async performUpdate(tableName: string, updateQueries: string[]) {
        const queryPart1 = `SELECT COUNT(DISTINCT title) AS count_distinct_problem_titles FROM ${tableName} WHERE book_instance_id IS NULL; `;

        // TODO: updateQueries would go in here
        const queryPart2 = queryPart1;

        const queryPart3 = queryPart1;

        const combinedQuery = queryPart1 + queryPart2 + queryPart3;

        const results = await this.sqlDb.executeMultiStatementQuery(
            combinedQuery
        );
        const beforeCount = results[0].rows[0].count_distinct_problem_titles;
        const afterCount = results[2].rows[0].count_distinct_problem_titles;
        console.log(`Before: ${beforeCount}, After: ${afterCount}`);
    }
}

interface Asdf {
    bookInstanceId: string;
    source: string;
    IParseBook;
    sqlObject: any;
}

// TODO: Update pages_read, book_or_shelf_opened, questions_correct (need to add column too), comprehension

// SELECT DISTINCT title, bookInstanceId FROM bloomreadertest.pages_read WHERE bookInstanceId is not null

// SELECT id, title FROM bloomreadertest.pages_read WHERE bookInstanceId is null
