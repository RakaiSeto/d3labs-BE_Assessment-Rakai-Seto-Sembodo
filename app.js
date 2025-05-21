require('dotenv').config();
const { ethers } = require('ethers');
const EventCacheDB = require('./db');
const RateLimiter = require('./ratelimit');
const fs = require('fs');

// init providers
const provider = new ethers.JsonRpcProvider('https://mainnet.infura.io/v3/d6badd75497e433d97404405f5a1f8bc');
const batchProvider = new ethers.JsonRpcProvider('https://eth-mainnet.g.alchemy.com/v2/Py4lN_HYFBhb-rxU7HdJ2_a56N_ZyGRG');
const contractAddress = '0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d';

// init rate limiter
const rateLimiter = new RateLimiter(25);

// init contract ABI
const contractABI = ['event Transfer(address indexed from, address indexed to, uint256 indexed tokenId)'];

const startBlock = 12286690; // Start from the first block when the contract was deployed

// init db
const db = new EventCacheDB();

async function findBlockByTimestamp(provider, targetTimestamp) {
    let startBlock = 0;
    let endBlock = await provider.getBlockNumber();
    let foundBlock = null;

    // use binary search to find the block
    while (startBlock <= endBlock) {
        const midBlock = Math.floor((startBlock + endBlock) / 2);
        const block = await provider.getBlock(midBlock);

        if (!block) {
            // Handle cases where the block doesn't exist
            console.error(`Block ${midBlock} not found.`);
            break;
        }

        const blockTimestamp = block.timestamp;

        // Define an acceptable tolerance for the timestamp difference in seconds
        const tolerance = 300;

        if (Math.abs(blockTimestamp - targetTimestamp) <= tolerance) {
            // Found a block within the acceptable tolerance
            foundBlock = block;
            break;
        } else if (blockTimestamp < targetTimestamp) {
            // Target timestamp is in the future, search in the upper half
            startBlock = midBlock + 1;
        } else {
            // Target timestamp is in the past, search in the lower half
            endBlock = midBlock - 1;
        }
    }

    return foundBlock;
}

async function getTransactionThroughBlock(provider, contractAddress, targetBlockNumber) {
    // init contract
    const contract = new ethers.Contract(contractAddress, contractABI, provider);
    // init filter
    const holder = contract.filters.Transfer();

    let currentBlock = startBlock;
    let maxBlock = targetBlockNumber;

    // Dynamic Chunk Sizing Parameters
    let currentChunkSize = 50000;
    const MIN_CHUNK_SIZE = 100;
    const MAX_CHUNK_SIZE = 150000;
    const MAX_LOGS_PER_QUERY = 9500;
    const MIN_LOGS_TO_INCREASE_CHUNK = 1000;

    // get highest cached block
    let highestCachedBlock = await db.GetHighestCachedBlock();
    if (highestCachedBlock >= maxBlock) {
        // if the highest cached block is greater than the max block, exit. no need to query
        console.log('No events to query, exiting...');
        return;
    }

    let startTime = Date.now();

    while (currentBlock <= maxBlock) {
        // if the highest cached block is greater than the current block, skip to the highest cached block
        if (highestCachedBlock > currentBlock) {
            console.log('Skipping to current block...');
            currentBlock = highestCachedBlock;
            continue;
        }

        // get the end block of the chunk
        let toBlockChunk = Math.min(currentBlock + currentChunkSize - 1, maxBlock);
        console.log(`Querying events from block ${currentBlock} to ${toBlockChunk}...`);

        try {
            // query the events
            const events = await contract.queryFilter(holder, currentBlock, toBlockChunk);
            console.log(`Received ${events.length} logs`);

            // insert the events into the db for caching
            await db.bulkInsertEvents(events);

            // if the events length is greater than the max logs per query, decrease the chunk size
            if (events.length >= MAX_LOGS_PER_QUERY) {
                console.warn(`High event density detected (received ${events.length} logs). Decreasing next chunk size.`);
                currentChunkSize = Math.max(MIN_CHUNK_SIZE, Math.floor(currentChunkSize * 0.75)); // Reduce by 25%
                console.log(`chunk size changed to ${currentChunkSize}`);
            } else if (events.length < MIN_LOGS_TO_INCREASE_CHUNK && currentChunkSize < MAX_CHUNK_SIZE) {
                console.log(`Low event density detected (received ${events.length} logs). Increasing next chunk size.`);
                currentChunkSize = Math.min(MAX_CHUNK_SIZE, Math.floor(currentChunkSize * 1.25)); // Increase by 25%
                console.log(`chunk size changed to ${currentChunkSize}`);
            } else if (events.length === 0 && currentChunkSize < MAX_CHUNK_SIZE) {
                // if no events, dramatically increase chunk size
                currentChunkSize = Math.min(MAX_CHUNK_SIZE, currentChunkSize * 2);
                console.log(`chunk size changed to ${currentChunkSize}`);
            }

            currentBlock = toBlockChunk + 1;
            console.log(`--------------------------------`);
        } catch (error) {
            console.error(`Error querying events from ${currentBlock} to ${toBlockChunk}:`, error.message);

            // if the query returned more than 10000 logs, cut the chunk size in half
            if (error.message.includes('query returned more than')) {
                console.log('Query returned more than 10000 logs, cutting chunk size in half...');
                currentChunkSize = Math.max(MIN_CHUNK_SIZE, Math.floor(currentChunkSize * 0.5));
                console.log(`chunk size changed to ${currentChunkSize}`);
            }

            // implement retry logic or backoff here if hitting rate limits
            await new Promise((resolve) => setTimeout(resolve, 150));
            console.log(`--------------------------------`);
            continue;
        }
    }
    let timeEnd = Date.now();
    // log the time taken
    console.log(`Time taken: ${timeEnd - startTime}ms`);
}

async function getHolderMapByBlock(startBlock, targetBlockNumber) {
    // get the events from the db
    const transferEvent = await db.getEventsByBlockRange(startBlock, targetBlockNumber);
    console.log(`Queried ${transferEvent.length} logs between block ${startBlock} and ${targetBlockNumber}`);

    // init holder map
    let holderMap = new Map();
    // iterate the events
    for (const [index, event] of transferEvent.entries()) {
        // get the holder
        const holder = event.args.to.toString();
        // get the token id
        const tokenId = event.args.tokenId.toString();

        // if the holder is the zero address, delete the token id
        if (holder === '0x0000000000000000000000000000000000000000') {
            holderMap.delete(tokenId);
        } else {
            // set the holder
            holderMap.set(tokenId, holder);
        }

        console.log(`${index} of ${transferEvent.length}`);
    }

    return holderMap;
}

async function getWalletBalance(provider, walletAddress, targetBlockNumber) {
    let completed = 0;
    const updateInterval = Math.max(1, Math.floor(walletAddress.length / 100));
    const fetchBalanceWithRetry = async (address) => {
        const MAX_RETRIES = Infinity;
        const RETRY_DELAY_MS = 2000;

        let attempts = 0;
        while (attempts < MAX_RETRIES) {
            attempts++;
            try {
                const balance = await provider.getBalance(address, targetBlockNumber);
                // return the balance
                return balance;
            } catch (error) {
                console.warn(
                    `Attempt ${attempts}: Could not get balance for ${address} at block ${targetBlockNumber}. ` +
                    `Error: ${error.message}. Retrying in ${RETRY_DELAY_MS / 1000} seconds...`
                );
                // add a delay before retrying
                await new Promise(resolve => setTimeout(resolve, RETRY_DELAY_MS));
            }
        }
        // if MAX_RETRIES is not Infinity and loop finishes, it means all retries failed.
        // for infinite retries, this part should ideally not be reached unless a critical error
        // prevents the loop from continuing.
        console.error(`Failed to get balance for ${address} after ${MAX_RETRIES} attempts.`);
        return ethers.parseUnits('0', 'wei');
    };

    // insert to rate limiter
    const balances = walletAddress.map((address) => {
        return rateLimiter.add(() => {
            return fetchBalanceWithRetry(address, targetBlockNumber).then((balance) => {
                completed++;
                console.log(`${completed} of ${walletAddress.length}`);
                return balance;
            }).catch((error) => {
                // retry
                console.error(`A critical error occurred for ${address} after retries:`, error);
                if (completed % updateInterval === 0 || completed === walletAddress.length) {
                    console.log(`  Balance requests completed: ${completed}/${walletAddress.length} (${((completed / walletAddress.length) * 100).toFixed(1)}%)`);
                }
                return ethers.parseUnits('0', 'wei');
            });
        });
    });

    try {
        // get the balances
        const balanceEth = await Promise.all(balances);
        let sum = ethers.toBigInt(0);

        balanceEth.forEach(balance => {
            sum += balance;
        });
 
        // format to ether and return
        return ethers.formatEther(sum);
    } catch (error) {
        console.error('Error fetching balances:', error);
        throw error;
    }
}

const main = async () => {
    // get timestamp from user input
    const targetUnixTimestamp = process.argv[2];
    // find block by timestamp
    const block = await findBlockByTimestamp(provider, targetUnixTimestamp);

    if (block) {
        console.log(`Found block ${block.number} with timestamp ${block.timestamp}`);
        // get all transaction through the block
        await getTransactionThroughBlock(provider, contractAddress, block.number);
    } else {
        console.log(`Could not find a block close to timestamp ${targetUnixTimestamp}`);
    }

    const holderMap = await getHolderMapByBlock(startBlock, block.number);
    let holderObject = Object.fromEntries(holderMap);

    // get all unique holders to prevent duplicate
    const uniqueHolders = new Set();
    // iterate holderObject and add each holder to the uniqueHolders set
    for (const tokenId in holderObject) {
        const holder = holderObject[tokenId];
        uniqueHolders.add(holder);
    }

    // convert the set to an array
    const uniqueHoldersArray = Array.from(uniqueHolders);

    // get the total sum of the balances in the block
    const totalSum = await getWalletBalance(batchProvider, uniqueHoldersArray, block.number);

    // get the total sum of the balances in the block
    console.log('Total sum in ether: ', totalSum);
};

main();