const { addToStorage,
    getFromStorage,
    removeFromStorage,
    processAsyncFunctionWithLock } = require('./redis');

async function promiseTimeoutSec(delaySec) {
    if (!delaySec) return;
    return new Promise(resolve =>
        setTimeout(() => resolve(), delaySec * 1000));
}

let i=0;
async function main() {
    setInterval(async () => {
        const val = i++;

        await addToStorage(val);
        console.log(`main: added to a storage`, val);
    }, 1000)
}

async function remWorker() {
    const dataInStorage = await getFromStorage(100);
    console.log(`remWorker: dataInStorage`, dataInStorage, dataInStorage.length);

    for (let item of dataInStorage) {
        await promiseTimeoutSec(1);
    }

    const removed = await removeFromStorage(dataInStorage.length);
    console.log(`remWorker: removed from Storage`, removed, removed.length);
}

main();
setInterval(() => {
    return processAsyncFunctionWithLock(remWorker);
}, 10000)
