// rateLimiter.js (or directly in your main file)
class RateLimiter {
    constructor(requestsPerSecond) {
        this.interval = 1000 / requestsPerSecond; // Milliseconds per request
        this.queue = [];
        this.lastDispatchTime = 0;
        this.timeoutId = null; // To manage the dispatch loop
    }

    /**
     * Adds a function to the queue to be executed under rate limit.
     * @param {Function} fn The async function to execute (e.g., () => provider.getBalance(...)).
     * @returns {Promise} A promise that resolves with the result of fn, or rejects if fn rejects.
     */
    add(fn) {
        return new Promise((resolve, reject) => {
            this.queue.push({ fn, resolve, reject });
            this.dispatch(); // Try to dispatch immediately
        });
    }

    dispatch() {
        if (this.timeoutId) {
            return; // Already scheduled for dispatch
        }

        const now = Date.now();
        const timeSinceLastDispatch = now - this.lastDispatchTime;

        if (this.queue.length > 0 && timeSinceLastDispatch >= this.interval) {
            // Dispatch immediately if we have requests and enough time has passed
            const { fn, resolve, reject } = this.queue.shift();
            this.lastDispatchTime = now;
            fn()
                .then(resolve)
                .catch(reject)
                .finally(() => {
                    this.timeoutId = null; // Clear timeout ID for next dispatch
                    this.dispatch(); // Try to dispatch the next one
                });
        } else if (this.queue.length > 0) {
            // Schedule the next dispatch
            const delay = this.interval - timeSinceLastDispatch;
            this.timeoutId = setTimeout(() => {
                this.timeoutId = null;
                this.dispatch();
            }, delay);
        }
    }
}

module.exports = RateLimiter;