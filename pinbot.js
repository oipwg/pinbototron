let config = require('config');
let db = require('sqlite');
let Log = require('log');
let fs = require('fs');
let fsp = require('filesize-parser');
var ipfsAPI = require('ipfs-api');
let request = require('request');

function getConfig(key, defaultVal) {
    if (config.has(key)) {
        return config.get(key);
    }
    log.warning("Config key %s not found using default $s", key, defaultVal);
    return defaultVal;
}

function refreshPublishedMedia() {
    log.info('Fetching media items');
    request(libraryMediaURL, function (err, resp, body) {
        if (!err && resp.statusCode === 200) {
            let media = JSON.parse(body);
            log.debug("Library refresh found %d media items", media.length);
            for (let k in media) {
                if (media.hasOwnProperty(k)) {
                    processMediaFiles(media[k]);
                }
            }
            console.log("meh")
        }
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

    for (let k in files)
        if (files.hasOwnProperty(k))
            if (files[k].fname)
                addFileToDB(dhtHash + '/' + files[k].fname, dhtHash)
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
}

function refreshPinCounts() {

}

function updateFileSizes() {
    db.all('SELECT * FROM pinTracker WHERE bytes IS NULL;', {Promise})
        .then(function (rows) {
            log.info("Updating file sizes for %d items.", rows.length);
            let i = 1;
            for (let row of rows) {
                setTimeout(function() {
                    updateFileSize(row);
                },i++*500);
            }
        })
        .catch(function (err) {
            console.log(err);
        })
}

function updateFileSize(row) {
    if (row.fileID !== row.ipfsAddress)
        ipfs.ls(row.ipfsAddress)
            .then(function (res) {
                for (let obj of res.Objects)
                    for (let link of obj.Links) {
                        if (obj.Hash + '/' + link.Name === row.fileID)
                            db.run("UPDATE pinTracker SET bytes = $bytes, fileAddress = $fileAddress WHERE fileID = $fileID", {
                                $fileID: obj.Hash + '/' + link.Name,
                                $fileAddress: link.Hash,
                                $bytes: link.Size
                            });
                        log.debug(obj.Hash + '/' + link.Name, link.Size)
                    }
            })
            .catch(function (err) {
                console.log(err)
            });
}

function main_hehe() {
    refreshPublishedMedia();
    setTimeout(updateFileSizes, 1.5 * 1000);
    setTimeout(refreshPinCounts, 1.5 * 1000);
}


let log = new Log(getConfig('logging.level', 'debug'),
    fs.createWriteStream(getConfig('logging.path', './pinbot.log'), {flags: 'a'}));
let diskUsageLimit = fsp(getConfig("resourceLimits.disk", "10GB"));
let libraryMediaURL = getConfig('libraryD.url', 'https://api.alexandria.io/alexandria/v2/media/get/all');
let ipfs = ipfsAPI(getConfig('IPFSNode', {host: 'localhost', port: '5001', protocol: 'http'}));


// Don't judge, I'm learning Promises as I go here and this will 100% be getting redone
Promise.resolve()
    .then(() => db.open(getConfig("database.db", ":memory:"), {Promise}))
    .then(() => db.migrate())
    .then(() => db.prepare("INSERT OR IGNORE INTO main.pinTracker (fileID, ipfsAddress) VALUES (?, ?);")
        .then((stmt) => stmtAddFileToDB = stmt))
    .then(() => main_hehe())
    .catch(err => console.error(err.stack));


// ipfs.ls('QmWS344nttYkMJo1ooHejcuEEdZauu4T1mfJmpatssmXP9')
//     .then(function (res) {
//         console.log('my res is: ');
//         console.log(res);
//     })
//     .catch(function (err) {
//         console.log('Fail: ', err)
//     });