const config = require('config');
const db = require('sqlite');
const Log = require('log');
const fs = require('fs');
const fsp = require('filesize-parser');
const ipfsAPI = require('ipfs-api');
const cleanMultihash = require('ipfs-api/src/clean-multihash');
const mh = require('multihashes');
const request = require('request');
const PromisePool = require('es6-promise-pool');

function getConfig(key, defaultVal) {
    if (config.has(key)) {
        return config.get(key);
    }
    log.warning("Config key %s not found using default $s", key, defaultVal);
    return defaultVal;
}

function refreshPublishedMedia() {
    log.info('Fetching media items');
    return new Promise((resolve, reject) => {
        request(libraryMediaURL, function (err, resp, body) {
            if (!err && resp.statusCode === 200) {
                let media = JSON.parse(body);
                log.debug("Library refresh found %d media items", media.length);
                for (let m of media) {
                    processMediaFiles(m);
                }
                resolve();
            } else
                reject();
        });
    });
}

function processMediaFiles(item) {
    if (item['media-data'] && item['media-data']['alexandria-media']) {
        processAlexandriaMedia(item);
    } else if (item['oip-041']) {
        processOip041(item['oip-041']);
    }
}

function processAlexandriaMedia(am) {
    if (am) {
        let extraInfo = am['media-data']['alexandria-media']['info']['extra-info'];
        let filename = extraInfo['filename'];

        let dhtHash = extraInfo['DHT Hash'];
        if (validMultihash(dhtHash))
            dhtHash = dhtHash.trim();
        let posterFrame = extraInfo['posterFrame'];
        let coverArt = extraInfo['coverArt'];
        let poster = extraInfo['poster'];
        let trailer = extraInfo['trailer'];
        let track01 = extraInfo['track01'];
        let track02 = extraInfo['track02'];

        if (filename) {
            if (filename === "none") {
                // pin each field available

                if (validMultihash(dhtHash)) addFileToDB(dhtHash, dhtHash);
                if (validMultihash(posterFrame)) addFileToDB(posterFrame, posterFrame);
                if (validMultihash(coverArt)) addFileToDB(coverArt, coverArt);
                if (validMultihash(poster)) addFileToDB(poster, poster);
                if (validMultihash(trailer)) addFileToDB(trailer, trailer);
                if (validMultihash(track01)) addFileToDB(track01, track01);
                if (validMultihash(track02)) addFileToDB(track02, track02);

            } else {
                // pin each field as a filename relative to the DHT hash
                if (validMultihash(dhtHash)) {

                    if (posterFrame) {
                        addFileToDB(dhtHash + "/" + posterFrame, dhtHash);
                    }
                    if (coverArt) {
                        addFileToDB(dhtHash + "/" + coverArt, dhtHash);
                    }
                    if (poster) {
                        addFileToDB(dhtHash + "/" + poster, dhtHash);
                    }
                    if (trailer) {
                        addFileToDB(dhtHash + "/" + trailer, dhtHash);
                    }
                    if (track01) {
                        addFileToDB(dhtHash + "/" + track01, dhtHash);
                    }
                    if (track02) {
                        addFileToDB(dhtHash + "/" + track02, dhtHash);
                    }
                }
            }
        }
    }
}

function processOip041(oip) {
    let dhtHash = oip.artifact.storage.location.trim();
    let files = oip.artifact.storage.files;

    if (!validMultihash(dhtHash)) {
        log.warning('Invalid DHT Hash found. title:`%s` - dht:`%s`', oip.artifact.info.title, dhtHash)
    }

    if (!files) {
        log.alert('No file information on artifact timestamped (%d)', oip.artifact.timestamp);
        //addFileToDB(dhtHash, dhtHash);
        return
    }

    for (let f of files)
        if (f.fname)
            addFileToDB(dhtHash + '/' + f.fname, dhtHash)
}

let stmtAddFileToDB;
function addFileToDB(filePath, dhtHash) {
    stmtAddFileToDB.run(filePath, dhtHash);
}

function validMultihash(string) {
    if (string) {
        if (string[0] === "Q" && string[1] === "m") {
            return true;
        }
    }
    return false;
    //
    // try {
    //     multihash = cleanMultihash(multihash, options)
    // } catch (err) {
    //     return callback(err)
    // }
}

function getMyIpfsId() {
    return new Promise((resolve, reject) => {
        ipfs.id().then(res => resolve(res.id)).catch(err => reject(err));
    })
}

function refreshPinCounts(ipfsId) {
    let ts = Date.now(); // (pinCount = 0 OR pinCount IS NULL) AND
    let results;
    let i = -1;

    let pinCountProducer = function () {
        if (i < results.length - 1 && results.length !== 0) {
            i++;
            return updatePinCount(ipfsId, results[i]);
        } else
            return null;
    };


    return db.all('SELECT * FROM pinTracker WHERE fileAddress IS NOT NULL AND (lastCheck < $ts OR lastCheck IS NULL);', {$ts: ts})
        .then(function (rows) {
            log.info("Updating pin counts for %d items.", rows.length);
            results = rows;
            let pool = new PromisePool(pinCountProducer, concurrency);
            return pool.start();
        })
}

function updatePinCount(ipfsId, row) {
    return ipfs.dht.findprovs(row.fileAddress)
        .then(function (peerInfos) {
            let count = 0;
            let isPinned = false;

            for (let peer of peerInfos) {
                if (peer.Type === 4) {
                    count++;
                    for (let response of peer.Responses)
                        if (response.ID === ipfsId)
                            isPinned = true;
                }
            }

            log.info("%s - isPinned[%s] pinCount[%d]", row.fileAddress, isPinned ? 'True ' : 'False', count);

            return db.run("UPDATE pinTracker SET pinCount = $pinCount, lastCheck = $lastCheck, isPinned = $isPinned WHERE fileAddress = $fileAddress", {
                $fileAddress: row.fileAddress,
                $pinCount: count,
                $isPinned: isPinned,
                $lastCheck: Date.now()
            });
        })
        .catch(function (err) {
            log.error("Failed to load IPFS Item [%s]", row.fileAddress);
            console.log(err)
        });
}

function updateFileSizes() {
    let results;
    let i = -1;

    let fileSizeProducer = function () {
        if (i < results.length - 1 && results.length !== 0) {
            i++;
            return updateFileSize(results[i]);
        } else
            return null;
    };


    return db.all('SELECT * FROM pinTracker WHERE bytes IS NULL;')
        .then(function (rows) {
            log.info("Updating file sizes for %d items.", rows.length);
            results = rows;
            let pool = new PromisePool(fileSizeProducer, concurrency);
            return pool.start();
        })
}

function updateFileSize(row, i) {
    return ipfs.object.get(row.ipfsAddress)
        .then(function (res) {
            if (res._links.length === 0) {
                db.run("UPDATE pinTracker SET bytes = $bytes, fileAddress = $fileAddress WHERE ipfsAddress = $ipfsAddress", {
                    $ipfsAddress: row.ipfsAddress,
                    $fileAddress: row.ipfsAddress,
                    $bytes: res._data.length
                });
                log.debug("%s [%d bytes]", row.ipfsAddress, res._data.length);
            } else {
                for (let link of res._links) {
                    if (row.ipfsAddress + '/' + link._name === row.fileID)
                        db.run("UPDATE pinTracker SET bytes = $bytes, fileAddress = $fileAddress WHERE fileID = $fileID", {
                            $fileID: row.ipfsAddress + '/' + link._name,
                            $fileAddress: mh.toB58String(link._multihash),
                            $bytes: link._size
                        });
                    log.debug("%s [%d bytes]", row.ipfsAddress + '/' + link._name, link._size);
                }
            }
            console.log("%d Done.", i);
        })
        .catch(function (err) {
            log.error("Failed to load IPFS Item [%s]", row.ipfsAddress);
            console.log(err);
            db.run("UPDATE pinTracker SET bytes = $bytes WHERE ipfsAddress = $ipfsAddress", {
                $ipfsAddress: row.ipfsAddress,
                $bytes: -1
            });
        });
}

function pinMedia() {
    let diskUse = 0;
    let results;
    let i = -1;

    function pinProducer() {
        if (i < results.length - 1 && results.length !== 0) {
            i++;
            if (diskUse + results[i].bytes < diskUsageLimit) {
                diskUse += results[i].bytes;
                return pinArtifact(results[i]);
            }
        } else
            return null;
    }


    return db.all('SELECT * FROM pinTracker WHERE isPinned = 1;')
        .then(function (rows) {
            log.info("%d media pieces are pinned.", rows.length);
            for (let row of rows) {
                diskUse += row.bytes;
            }
            log.info("Disk utilization %d bytes", diskUse);
        })
        .then(() =>
            db.all('SELECT * FROM pinTracker WHERE pinCount < $minPinThreshold AND bytes > 0 AND isPinned == 0 ORDER BY pinCount ASC, id ASC;',
                {$minPinThreshold: minPinThreshold}))
        .then(function (rows) {
            log.info("Possibly pinning %d media pieces.", rows.length);
            results = rows;
            let pool = new PromisePool(pinProducer, concurrency);
            return pool.start();
        })
}

function pinArtifact(row) {
    log.info("Pinning %s for %d bytes", row.fileAddress, row.bytes);
    return ipfs.pin.add(row.fileAddress).then(res => {
        log.info('%s pinned %d bytes', row.fileAddress, row.bytes);
        return db.run("UPDATE pinTracker SET pinCount = pinCount + 1, isPinned = $isPinned WHERE fileAddress = $fileAddress", {
            $fileAddress: row.fileAddress,
            $isPinned: 1
        });
    }).catch(err => {
        log.error('Failed to pin [%s]', row.fileAddress);
    })
}

let log = new Log(getConfig('logging.level', 'debug'),
    fs.createWriteStream(getConfig('logging.path', './pinbot.log'), {flags: 'a'}));
let concurrency = getConfig('concurrency', 5);
let diskUsageLimit = fsp(getConfig("resourceLimits.disk", "10GB"));
let minPinThreshold = getConfig('minPinThreshold', 1);
let stopPinLimit = getConfig('stopPinLimit', 10);
let libraryMediaURL = getConfig('libraryD.url', 'https://api.alexandria.io/alexandria/v2/media/get/all');
let ipfs = ipfsAPI(getConfig('IPFSNode', {host: 'localhost', port: '5001', protocol: 'http'}));


// Don't judge, I'm learning Promises as I go here and this will 100% be getting redone
Promise.resolve()
    .then(() => db.open(getConfig("database.db", ":memory:"), {Promise}))
    .then(() => db.migrate())
    .then(() => db.prepare("INSERT OR IGNORE INTO main.pinTracker (fileID, ipfsAddress) VALUES (?, ?);"))
    .then(stmt => stmtAddFileToDB = stmt)
    .then(refreshPublishedMedia)
    .then(updateFileSizes)
    .then(getMyIpfsId)
    .then(id => refreshPinCounts(id))
    .then(pinMedia)
    .catch(err => {
        console.log(err);
        console.error(err.stack)
    });
