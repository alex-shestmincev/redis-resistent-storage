const redis = require('redis');
const { promisify } = require('util')

const Redlock = require('redlock');



const port = '6379';
const server = 'localhost';
const clientOptions = {};

// lock: redlock.lock,
//     extend: redlock.extend,
//     unlock: redlock.unlock,

let _redisClient;
function getRedis() {
    if (_redisClient) return _redisClient;
    _redisClient = redis.createClient(port, server, clientOptions);

    Object.entries(Reflect.getPrototypeOf(_redisClient))
        .filter(([_, value]) => typeof value === 'function')
        .forEach(([name, value]) => {
            _redisClient[`${name}Async`] = promisify(value).bind(_redisClient);
        });


    _redisClient.on("connect", function() {
        console.log('_redisClient', _redisClient.server_info)
    })

    return _redisClient;
}



const storageName = 'myStorage';
async function addToStorage(data) {
    return getRedis().rpushAsync(storageName, JSON.stringify(data));
}

const maxItems = 100;
const getFromStorage = (max = maxItems) => {
    return getRedis().lrangeAsync(storageName, 0, max);
}

const removeFromStorage = async (n) => {
    const removed = [];
    for (let i=0; i < n; i++) {
        removed.push(await getRedis().lpopAsync(storageName));
    }
    return removed;
}

const redlock = new Redlock([getRedis()], {
    // the expected clock drift; for more details
    // see http://redis.io/topics/distlock
    driftFactor: 0.01, // multiplied by lock ttl to determine drift time

    // the max number of times Redlock will attempt
    // to lock a resource before erroring
    retryCount:  9999999,

    // the time in ms between attempts
    retryDelay:  1000, // time in ms

    // the max time in ms randomly added to retries
    // to improve performance under high contention
    // see https://www.awsarchitectureblog.com/2015/03/backoff.html
    retryJitter:  200 // time in ms
});


let waitingForLock = 0;
const lockName = 'locks:processAsyncFunctionWithLock';
const lockTtl = 30000;
const logPreefix = 'procLockAsync';
async function processAsyncFunctionWithLock(asyncFunc) {
    try{
        console.log(`${logPreefix}: waiting for lock`, ++waitingForLock);
        const lock = await redlock.lock(lockName, lockTtl);

        console.log(`${logPreefix}: processing`);
        const result = await asyncFunc();

        console.log(`${logPreefix}: done`, --waitingForLock);

        await lock.unlock();
        return result;
    } catch (err) {
        console.log(`${logPreefix}: err`, err);
        throw err;
    }

}


module.exports = {
    addToStorage,
    getFromStorage,
    removeFromStorage,
    processAsyncFunctionWithLock,
};
