// db.js
const sqlite3 = require('sqlite3');
const path = require('path');

const DB_PATH = path.resolve(__dirname, 'events.db'); // Path to your SQLite database file

class EventCacheDB {
    constructor() {
        this.db = new sqlite3.Database(DB_PATH, (err) => {
            if (err) {
                console.error('Error opening database:', err.message);
            } else {
                console.log('Database opened successfully.');
                this.createTable();
            }
        });
    }

    createTable() {
        // blockNumber and logIndex are indexed for efficient range queries and ordering
        // fromAddress, toAddress, and tokenId are also indexed for potential filtering needs
        const createTableSql = `
            CREATE TABLE IF NOT EXISTS transfers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                blockNumber INTEGER NOT NULL,
                logIndex INTEGER NOT NULL,
                transactionHash TEXT NOT NULL,
                fromAddress TEXT NOT NULL,
                toAddress TEXT NOT NULL,
                tokenId TEXT NOT NULL,
                UNIQUE(blockNumber, logIndex, transactionHash) -- Ensures no duplicate events
            );
            CREATE INDEX IF NOT EXISTS idx_blockNumber ON transfers (blockNumber);
            CREATE INDEX IF NOT EXISTS idx_fromAddress ON transfers (fromAddress);
            CREATE INDEX IF NOT EXISTS idx_toAddress ON transfers (toAddress);
            CREATE INDEX IF NOT EXISTS idx_tokenId ON transfers (tokenId);
        `;
        this.db.exec(createTableSql, (err) => {
            if (err) {
                console.error('Error creating transfers table:', err.message);
            } else {
                console.log('Transfers table checked/created.');
            }
        });
    }

    // Insert a single event
    async insertEvent(event) {
        return new Promise((resolve, reject) => {
            const sql = `
                INSERT OR IGNORE INTO transfers (
                    blockNumber, logIndex, transactionHash, fromAddress, toAddress, tokenId
                ) VALUES (?, ?, ?, ?, ?, ?)
            `;
            // Use run for INSERT, UPDATE, DELETE
            this.db.run(
                sql,
                [
                    event.blockNumber,
                    event.logIndex,
                    event.transactionHash,
                    event.args.from,
                    event.args.to,
                    event.args.tokenId.toString(), // Store tokenId as string for consistency
                ],
                function (err) {
                    // Use a regular function to access 'this' context for lastID/changes
                    if (err) {
                        reject(err);
                    } else {
                        resolve(this.lastID); // Return the ID of the new row
                    }
                }
            );
        });
    }

    // Bulk insert events
    async bulkInsertEvents(events) {
        return new Promise((resolve, reject) => {
            this.db.serialize(() => {
                // Ensures operations are sequential
                this.db.run('BEGIN TRANSACTION;');
                const stmt = this.db.prepare(`
                    INSERT OR IGNORE INTO transfers (
                        blockNumber, logIndex, transactionHash, fromAddress, toAddress, tokenId
                    ) VALUES (?, ?, ?, ?, ?, ?)
                `);
                for (const event of events) {
                    // console.log(event);
                    let result = stmt.run(
                        event.blockNumber,
                        event.index,
                        event.transactionHash,
                        event.args[0],
                        event.args[1],
                        event.args[2].toString()
                    );
                }
                stmt.finalize((err) => {
                    // Finalize statement
                    // console.log('Finalizing statement...');
                    if (err) {
                        this.db.run('ROLLBACK;');
                        console.error('Error inserting events:', err.message);
                        reject(err);
                    } else {
                        this.db.run('COMMIT;', (commitErr) => {
                            if (commitErr) {
                                console.error('Error committing transaction:', commitErr.message);
                                reject(commitErr);
                            } else {
                                // console.log('Transaction committed successfully.');
                                resolve();
                            }
                        });
                    }
                });
            });
        });
    }

    // Get events within a block range
    async getEventsByBlockRange(fromBlock, toBlock) {
        return new Promise((resolve, reject) => {
            const sql = `
                SELECT blockNumber, logIndex, transactionHash, fromAddress, toAddress, tokenId
                FROM transfers
                WHERE blockNumber >= ? AND blockNumber <= ?
                ORDER BY blockNumber ASC, logIndex ASC;
            `;
            // Use all for SELECT with multiple rows
            this.db.all(sql, [fromBlock, toBlock], (err, rows) => {
                if (err) {
                    reject(err);
                } else {
                    console.log(`Received ${rows.length} logs`);
                    // Reconstruct a format similar to ethers.js event.args
                    const formattedRows = rows.map((row) => ({
                        blockNumber: row.blockNumber,
                        logIndex: row.logIndex,
                        transactionHash: row.transactionHash,
                        args: {
                            from: row.fromAddress,
                            to: row.toAddress,
                            tokenId: BigInt(row.tokenId), // Convert back to BigInt if needed by consumer
                        },
                        // You might need to add other fields if your event processing code relies on them
                    }));
                    resolve(formattedRows);
                }
            });
        });
    }

    // Get the highest block number currently in the cache
    async GetHighestCachedBlock() {
        return new Promise((resolve, reject) => {
            const sql = `SELECT MAX(blockNumber) as maxBlock FROM transfers;`;
            this.db.get(sql, [], (err, row) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(row.maxBlock || -1); // Return -1 if no blocks are cached
                }
            });
        });
    }

    // Close the database connection (important when your app shuts down)
    close() {
        this.db.close((err) => {
            if (err) {
                console.error('Error closing SQLite database:', err.message);
            } else {
                console.log('SQLite database connection closed.');
            }
        });
    }
}

module.exports = EventCacheDB;
