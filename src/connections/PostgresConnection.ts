import { Pool, Result } from "pg";

export default class PostgresConnection {
    private static instance: PostgresConnection | undefined;

    private pool: Pool;

    private constructor() {
        this.pool = new Pool();
    }

    public static getInstance(): PostgresConnection {
        if (!this.instance) {
            this.instance = new PostgresConnection();
        }

        return this.instance;
    }

    public dispose(): void {
        this.pool.end();
        PostgresConnection.instance = undefined;
    }

    public async executeQuery(sqlQuery: string): Promise<any[]> {
        const result = await this.pool.query(sqlQuery);
        //console.log("Result: " + JSON.stringify(result));
        return result.rows;
    }

    public async executeMultiStatementQuery(
        sqlQuery: string
    ): Promise<Result[]> {
        return this.pool.query(sqlQuery);
    }
}
