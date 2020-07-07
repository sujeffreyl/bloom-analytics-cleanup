import { BookInstanceIdBackfiller } from "./BookInstanceIdBackfiller";
import PostgresConnection from "./connections/PostgresConnection";

main();

async function main(): Promise<void> {
    const backfiller = new BookInstanceIdBackfiller();
    await backfiller.backfill();

    PostgresConnection.getInstance().dispose();
}
