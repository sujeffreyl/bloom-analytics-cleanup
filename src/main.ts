import { BookInstanceIdBackfiller } from "./BookInstanceIdBackfiller";
import PostgresConnection from "./connections/PostgresConnection";
import { Environment } from "./Environment";

// Controls which parse database and postgresql schema(s) we connect to...
export const environment: Environment = Environment.Dev;

main();

async function main(): Promise<void> {
    console.log();
    console.log(
        "########################################################################"
    );
    console.log(
        `Environment: ${
            environment === Environment.Prod
                ? "Production"
                : environment === Environment.Dev
                ? "Development"
                : "Local"
        }`
    );
    console.log(
        "########################################################################"
    );
    console.log();

    const backfiller = new BookInstanceIdBackfiller();
    await backfiller.backfill();

    PostgresConnection.getInstance().dispose();
}
