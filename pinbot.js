const config = require('config')
const db = require('sqlite')
const Log = require('log')
const fs = require('fs')
const fsp = require('filesize-parser')
const ipfsAPI = require('ipfs-api')
const cleanMultihash = require('ipfs-api/src/clean-multihash')
const mh = require('multihashes')
const request = require('request')
const PromisePool = require('es6-promise-pool')

function getConfig (key, defaultVal) {
  if (config.has(key)) {
    return config.get(key)
  }
  log.warning('Config key %s not found using default $s', key, defaultVal)
  return defaultVal
}

function refreshPublishedMedia () {
  log.info('Fetching media items')
  return new Promise((resolve, reject) => {
    request(libraryMediaURL, function (err, resp, body) {
      if (!err && resp.statusCode === 200) {
        let media = JSON.parse(body)
        log.debug('Library refresh found %d media items', media.length)
        for (let m of media) {
          processMediaFiles(m)
        }
        resolve()
      } else
        reject()
    })
  })
}

function processMediaFiles (item) {
  if (item['media-data'] && item['media-data']['alexandria-media']) {
    processAlexandriaMedia(item)
  } else if (item['oip-041']) {
    processOip041(item['oip-041'])
  }
}

function processAlexandriaMedia (am) {
  if (am) {
    let extraInfo = am['media-data']['alexandria-media']['info']['extra-info']
    let filename = extraInfo['filename']

    let dhtHash = extraInfo['DHT Hash']
    if (validMultihash(dhtHash))
      dhtHash = dhtHash.trim()
    let posterFrame = extraInfo['posterFrame']
    let coverArt = extraInfo['coverArt']
    let poster = extraInfo['poster']
    let trailer = extraInfo['trailer']
    let track01 = extraInfo['track01']
    let track02 = extraInfo['track02']

    if (filename) {
      if (filename === 'none') {
        // pin each field available

        if (validMultihash(dhtHash)) addFileToDB(dhtHash, dhtHash)
        if (validMultihash(posterFrame)) addFileToDB(posterFrame, posterFrame)
        if (validMultihash(coverArt)) addFileToDB(coverArt, coverArt)
        if (validMultihash(poster)) addFileToDB(poster, poster)
        if (validMultihash(trailer)) addFileToDB(trailer, trailer)
        if (validMultihash(track01)) addFileToDB(track01, track01)
        if (validMultihash(track02)) addFileToDB(track02, track02)

      } else {
        // pin each field as a filename relative to the DHT hash
        if (validMultihash(dhtHash)) {
          if (filename) {
            addFileToDB(dhtHash + '/' + filename, dhtHash)
          }
          if (posterFrame) {
            addFileToDB(dhtHash + '/' + posterFrame, dhtHash)
          }
          if (coverArt) {
            addFileToDB(dhtHash + '/' + coverArt, dhtHash)
          }
          if (poster) {
            addFileToDB(dhtHash + '/' + poster, dhtHash)
          }
          if (trailer) {
            addFileToDB(dhtHash + '/' + trailer, dhtHash)
          }
          if (track01) {
            addFileToDB(dhtHash + '/' + track01, dhtHash)
          }
          if (track02) {
            addFileToDB(dhtHash + '/' + track02, dhtHash)
          }
        }
      }
    }
  }
}

function processOip041 (oip) {
  let dhtHash = oip.artifact.storage.location.trim()
  let files = oip.artifact.storage.files

  if (!validMultihash(dhtHash)) {
    log.warning('Invalid DHT Hash found. title:`%s` - dht:`%s`', oip.artifact.info.title, dhtHash)
  }

  if (!files) {
    log.alert('No file information on artifact timestamped (%d)', oip.artifact.timestamp)
    //addFileToDB(dhtHash, dhtHash);
    return
  }

  for (let f of files)
    if (f.fname)
      addFileToDB(dhtHash + '/' + f.fname, dhtHash)
}

let stmtAddFileToDB

function addFileToDB (filePath, dhtHash) {
  if (!dhtHash.includes('/'))
    stmtAddFileToDB.run(filePath, dhtHash)
}

function validMultihash (string) {
  // if (string) {
  //   if (string[0] === 'Q' && string[1] === 'm') {
  //     return true
  //   }
  // }
  // return false

  try {
    cleanMultihash(string)
    return true
  } catch (err) {
    return false
  }
}

function getMyIpfsId () {
  return new Promise((resolve, reject) => {
    ipfs.id().then(res => resolve(res.id)).catch(err => reject(err))
  })
}

function refreshPinCounts (ipfsId) {
  let d = new Date
  d.setHours(d.getHours() - 1)
  let ts = d.getTime()
  let results
  let i = -1

  let pinCountProducer = function () {
    if (i < results.length - 1 && results.length !== 0) {
      i++
      return updatePinCount(ipfsId, results[i])
    } else
      return null
  }

  return db.all('SELECT * FROM pinTracker WHERE fileAddress IS NOT NULL AND (lastCheck < $ts OR lastCheck IS NULL);', {$ts: ts})
    .then(function (rows) {
      log.info('Updating pin counts for %d items.', rows.length)
      results = rows
      let pool = new PromisePool(pinCountProducer, concurrency)
      return pool.start()
    })
}

function updatePinCount (ipfsId, row) {
  return ipfs.dht.findprovs(row.fileAddress, {timeout: '60s'})
    .then(function (peerInfos) {
      let count = 0
      let isPinned = false

      for (let peer of peerInfos) {
        if (peer.Type === 4) {
          count++
          for (let response of peer.Responses)
            if (response.ID === ipfsId)
              isPinned = true
        }
      }

      log.info('%s - isPinned[%s] pinCount[%d]', row.fileAddress, isPinned ? 'True ' : 'False', count)

      return db.run('UPDATE pinTracker SET pinCount = $pinCount, lastCheck = $lastCheck, isPinned = $isPinned WHERE fileAddress = $fileAddress', {
        $fileAddress: row.fileAddress,
        $pinCount: count,
        $isPinned: isPinned,
        $lastCheck: Date.now()
      })
    })
    .catch(function (err) {
      log.error('Failed to load IPFS Item [%s]', row.fileAddress)
      console.log(err)
    })
}

function updateFileSizes () {
  let results
  let i = -1

  let fileSizeProducer = function () {
    if (i < results.length - 1 && results.length !== 0) {
      i++
      return updateFileSize(results[i])
    } else
      return null
  }

  return db.all('SELECT * FROM pinTracker WHERE bytes IS NULL or bytes = -1;')
    .then(function (rows) {
      log.info('Updating file sizes for %d items.', rows.length)
      results = rows
      let pool = new PromisePool(fileSizeProducer, concurrency)
      return pool.start()
    })
}

const skipHashes = ['Qmeke1CyonqgKErvGhE18WLBuhrLaScbpSAS6vGLuoSCXM', 'Qmeke1CyonqgKErvGhE18WLBuhrLaScbpSAS6vGLuoSCXM', 'Qmeke1CyonqgKErvGhE18WLBuhrLaScbpSAS6vGLuoSCXM', 'QmcCsR75CkFKeeP8U8TzZ1YszgxHtcubmwFxCx2CxSFQty', 'QmTrYQieyjPA31Ht78nZRYN8J7KHW4fTf1rgH6biRVDNBH', 'QmTrYQieyjPA31Ht78nZRYN8J7KHW4fTf1rgH6biRVDNBH', 'QmTfyzeNwSrpWfjocmQrqWNmQXLhXECgBhkbbcMLzEYw1G', 'QmTfyzeNwSrpWfjocmQrqWNmQXLhXECgBhkbbcMLzEYw1G', 'QmY7U9eBMEGQh6SahbQx1E7g91fCVukZL8bi2BJjYXpqiw', 'QmY7U9eBMEGQh6SahbQx1E7g91fCVukZL8bi2BJjYXpqiw', 'QmdJekV4KTf4c4McLTeMSfjHatQWpF2UdXPJgbiZxiGhr5', 'QmdJekV4KTf4c4McLTeMSfjHatQWpF2UdXPJgbiZxiGhr5', 'QmVxh4Qj8otoYJScmhBEKirysQuh5wPFsn7FiYmDhVUru6', 'QmVxh4Qj8otoYJScmhBEKirysQuh5wPFsn7FiYmDhVUru6', 'Qmd9jLxKitTM89c7vwfiiAsN6f6oSbc8fX8e5T8pdtcuMo', 'QmNr7wdyEHTEFrFgCBsDTKPMwj4PHKAYbjMz5Md5hvQPeq', 'QmVDxpAKuHMBfUTxGj8ZfKuC3bc4Bs4raGREQZGD7nqcVe', 'QmZ1KP7KJf2oTMcgtxx9giGL8R2v3StrcAv5im24Gnw6kf', 'QmYwYSRUxrBMnS9Pnk6HanDUudEDgA47GTctqSirZ3Xdxn', 'QmW3WSN2Y8hASFmF4eecn6hrLNrdAid7CyAiGUY8yCP8Ny', 'QmaPhDebderaugizZjgb7cuz5tVNhEMczdqzDpQcFeK5Bj', 'QmQBkuG7T5boGYrzJkTYTc4foaMNtgQaurzTGNQPx98WkU', 'QmQBkuG7T5boGYrzJkTYTc4foaMNtgQaurzTGNQPx98WkU', 'QmNPXCHop47uRek7Ug2pqPhmpXVtsqNNPawor8rYNoEGah', 'QmPZUckSCUhppmebsSKG5aj1RC1vNz5PWeaijsawFYGxFj', 'QmR6gc6uUCnKX336JCiyZre8wooBHUrEkyyAN1tT8f9nGq', 'QmUYY6nLo7bKEjPPh82U6oHrsXUpF1uenZtzgpUUYdTkLf', 'QmPbAHcDZB7HqPpzYy8ZyPpP73F8DLDkpMuHow8dvShg8s', 'QmPbAHcDZB7HqPpzYy8ZyPpP73F8DLDkpMuHow8dvShg8s', 'QmSfKUYxmbytBZFK2vzqPN5xbxYUZtro5Q9HEg8gqMQVME', 'QmSJmpieSp3NZJ36ogWhUsqxbiYbxrCZy79vY3KSwz4XfS', 'QmRHcphM97HvPk8o85GoodHs3BQHyrGcTmgQHVreibHwu2', 'QmccLiRW7ajqctW3euZZFxaYnFWb9ntkiTikJbjaRamWBM', 'QmaofgCXLaFnHugZVqwZRvCQL5EbMDrh9qQhyB2rfQ5cke', 'QmUj8qLY8aGhagDAZzsdNhoKVJqsmhhhh8oTPJVeoFtEBF', 'QmdZGrLjW8XNS4duh5ZcZpmQL2jCi3VzXkrKf5qxMpqtu6', 'QmQAzdyR7KrZynC8SH4hDVZHiikCPXrsfS7vSaFwGG52Gu', 'QmQPptJRz41M6KkjjT1x6VNdnieFBZe29388ZpswnHoqHc', 'QmVAbF3jNAMATcvAv681GhMdxCQLdJVLw9fi94V4BG2uV5', 'QmXDL3Q8eoNgJpJSaotRxaJ225ywmqrgAsUTQUtAZdcErG', 'QmRyPmJTpRqdAp4DKsuN7qgG2bHevhhe2pj3RbieXpLhPq', 'QmNTUCJ1Dcp1z5ptgDEz1MTjVCd5xCoayK6P28WRGYj7WU', 'QmR1tQmq1EyVijrTQ7k3eYnQx8Fkjyko2WZCREZcnSniDP', 'Qma3Bk3z2kjHXx849BVFGzVZFgMFDnRgFmvgw67xHcoDzy', 'QmbFMke1KXqnYyBBWxB74N4c5SBnJMVAiMNRcGu6x1AwQH', 'QmWXAw6Jc7orbUHLHF1TdbtK1g87BVEYd1ePgsoRyxFYcA', 'QmSCCwuytSwYe7CquSuq1NzcPLXZutZkGV4yRSQxpb5zYh', 'QmQaetCsudRh7i5yzeAtb73qMQKsDR73o5F67bx8jS7YPz', 'QmQaetCsudRh7i5yzeAtb73qMQKsDR73o5F67bx8jS7YPz', 'QmX7FevmtyT2DVVsUGpCSFxazCFU7MCxEvdU5uWQm98PYA', 'QmX7FevmtyT2DVVsUGpCSFxazCFU7MCxEvdU5uWQm98PYA', 'QmdYGrXAQFSR79HuR4BBLVVeCimaL3E5SBTc9SUW7z31vy', 'QmWHCVxW7km2rBSPixA92TdwQgXze8Z8LHSdJqh1SaYjnF', 'QmV5CxbSXNfBdU2Uw6pCLUEarDJM6foy5dp16nTh845zmC', 'QmSuBSefFtqrEiqpKDbicA3nSGAVXSQhYAsjed5mGwwogh', 'QmZQHXEF1HcbqQmjZzgH4itxXhyZZYSvgRZs3tGp1iBCfc', 'QmSH5DF6W8rWqV7wxDkiWr2uwtZF97cuhveA17HTXEMVjg', 'QmUJU7ZKekkms6CPiJTGaQuRDG5pSCsoPFCSG3opeDuRy5', 'QmUdp18RCZkMs7UrnacQW8PN2QDFyDjd3YHktSzBMiRrZL', 'QmTWVdvb42HC88wQ3M7XcqSGc5ippSc95zGrsJMdzTbwp2', 'QmYo7b1ANuBRs72WkYKUf8xGXEg4Sde61Dkm8Af7wJD5Kz', 'QmToimRMjnLvgbRUbsjBQUf9qdQdk6S2yxobVDN5bvGXtk', 'QmRYr2XoLeutFkCeykNEDg5ziyPLDMLuZufAH33Qoqu5r3', 'QmQJYTCKz4JkNWWViH3XqD8RD49a6jzuexKDGvVaeQTMf3', 'QmX7dHH7ymnVTU7G2VrTdi1SZMRrCqibR671mzv7fim29W', 'QmRPHTTV6Xs6VJV9xNBqHCd1Qiv8Us6iA9ZLiZzbjhkKME', 'QmfTxECGdDoDqHQXUyTQWumvPaAWTQJKtaNojJc4CM3zKi', 'QmX3mPcEg59L8VoTKqqAmKz369N76CAT98je11o8b3MqLX', 'QmX3mPcEg59L8VoTKqqAmKz369N76CAT98je11o8b3MqLX', 'QmdiY6sWsC1mKhnkRfemZnUonFmyE8KhPemob4owvmaqst', 'QmdiY6sWsC1mKhnkRfemZnUonFmyE8KhPemob4owvmaqst', 'QmeP7Ta3Rjp6jCAwrsXpA6mLCC5HfMkqzDRk2dBa35rfVv', 'QmeP7Ta3Rjp6jCAwrsXpA6mLCC5HfMkqzDRk2dBa35rfVv', 'QmU6heh6AihE2vMzco1htiyB4CU6i7Jb6eQNYgirPsehNv', 'QmU6heh6AihE2vMzco1htiyB4CU6i7Jb6eQNYgirPsehNv', 'QmV4jg2VjQwvUkazrdbazBBndB9BBaDa4rR3iE28T58eAn', 'QmV4jg2VjQwvUkazrdbazBBndB9BBaDa4rR3iE28T58eAn',
  'QmQkWhNPzb7nhdTJZCiDPFNSTiEDg34EKc3sE7c2dA7J7x', 'QmYW1M8nyZbJDBRdBRGE9Mnks8raof2MK8zjPq7tM2eo36', 'Qme2DQu9uiiDqKZBMzaEE91JKG4Dd1Hd7QCuhTJcZJ3mAs', 'Qmd6jZFH7oQddBiWoQt1tgqU9GjUNJLi8hNcVgSApfqxwt', 'QmRA3NWM82ZGynMbYzAgYTSXCVM14Wx1RZ8fKP42G6gjgj', 'QmbjP9Q7SGFLjzgfYYWKpaafyefn8C9Rm5hYEPnJJr3BVU', 'QmbjP9Q7SGFLjzgfYYWKpaafyefn8C9Rm5hYEPnJJr3BVU', 'QmY4KXsCA1DEix74emacdCDR8TQHd7wieFLR47RQMqWpec', 'QmbZ2VPeBDXkVScdF1u8Hb8duxFrYLPqQgMQoydp3Hk5G1', 'QmbZ2VPeBDXkVScdF1u8Hb8duxFrYLPqQgMQoydp3Hk5G1', 'QmXibd3yTUwryQ6syd5fpUhM9nXRdn19TZdKQKyPrYQn9F', 'QmU8gm3Ffpt9jcpLEg4sGV7E18TR22Hjg2c5ztsebpDXTy', 'QmWhRkj4r2evQpr8QVyLefpRDTJVU8mbGqGwFbqC3gbnhG', 'QmQAiBJsYmsPbnnDKNVdTebWQHnkK9ieyrRhNds6Dudy3K', 'QmQnGKBwNSVRgS3DKKH5ifHFMvSZWunf5ACN4dVdZ66PnP', 'QmZorHqGwwVU9UCrwP82dzDyvTTBLpu94BmXpHfRGCKs2S', 'QmZorHqGwwVU9UCrwP82dzDyvTTBLpu94BmXpHfRGCKs2S', 'Qmcqtw8FfrVSBaRmbWwHxt3AuySBhJLcvmFYi3Lbc4xnwj', 'Qmcqtw8FfrVSBaRmbWwHxt3AuySBhJLcvmFYi3Lbc4xnwj', 'Qmcqtw8FfrVSBaRmbWwHxt3AuySBhJLcvmFYi3Lbc4xnwj', 'QmWKYk6Npr55RpaDsd6TqrWz5mdg8PX5i8wecMBu2kvgPB', 'QmU8gm3Ffpt9jcpLEg4sGV7E18TR22Hjg2c5ztsebpDXTy', 'QmU8gm3Ffpt9jcpLEg4sGV7E18TR22Hjg2c5ztsebpDXTy', 'QmQkWhNPzb7nhdTJZCiDPFNSTiEDg34EKc3sE7c2dA7J7x']

function updateFileSize (row) {
  log.debug('%s [%d bytes]', row.ipfsAddress, -1)
  if (skipHashes.includes(row.ipfsAddress)) {
    log.alert('Skipping %s, it\'s dead.', row.ipfsAddress)
    return Promise.resolve()
  }
  return ipfs.object.get(row.ipfsAddress, {timeout: '60s'})
    .then(function (res) {
      if (res._data[1] === 2) { // is file
        return ipfs.object.stat(row.ipfsAddress)
          .then((res) => {
            db.run('UPDATE pinTracker SET bytes = $bytes, fileAddress = $fileAddress WHERE fileID = $fileID', {
              $fileID: row.ipfsAddress,
              $fileAddress: row.ipfsAddress,
              $bytes: res.CumulativeSize
            })
            log.debug('%s [%d bytes]', row.ipfsAddress, res.CumulativeSize)
          })
          .catch((err) => {
            log.error('Failed to load IPFS Item [%s]', row.ipfsAddress)
            console.log(err)
            db.run('UPDATE pinTracker SET bytes = $bytes WHERE ipfsAddress = $ipfsAddress', {
              $ipfsAddress: row.ipfsAddress,
              $bytes: -1
            })
          })
      } else {
        for (let link of res._links) {
          if (row.ipfsAddress + '/' + link._name === row.fileID)
            db.run('UPDATE pinTracker SET bytes = $bytes, fileAddress = $fileAddress WHERE fileID = $fileID', {
              $fileID: row.ipfsAddress + '/' + link._name,
              $fileAddress: mh.toB58String(link._multihash),
              $bytes: link._size
            })
          log.debug('%s [%d bytes]', row.ipfsAddress + '/' + link._name, link._size)
        }
      }
    })
    .catch(function (err) {
      log.error('Failed to load IPFS Item [%s]', row.ipfsAddress)
      console.log(err)
      db.run('UPDATE pinTracker SET bytes = $bytes WHERE ipfsAddress = $ipfsAddress', {
        $ipfsAddress: row.ipfsAddress,
        $bytes: -1
      })
    })
}

function pinMedia () {
  let diskUse = 0
  let results
  let i = -1

  function pinProducer () {
    if (i < results.length - 1 && results.length !== 0) {
      i++
      if (diskUse + results[i].bytes < diskUsageLimit) {
        diskUse += results[i].bytes
        return pinArtifact(results[i])
      }
    } else
      return null
  }

  return db.all('SELECT * FROM pinTracker WHERE isPinned = 1;')
    .then(function (rows) {
      log.info('%d media pieces are pinned.', rows.length)
      for (let row of rows) {
        diskUse += row.bytes
      }
      log.info('Disk utilization %d bytes', diskUse)
    })
    .then(() =>
      db.all('SELECT * FROM pinTracker WHERE pinCount < $minPinThreshold AND bytes > 0 AND isPinned == 0 ORDER BY pinCount ASC, id ASC;',
        {$minPinThreshold: minPinThreshold}))
    .then(function (rows) {
      log.info('Possibly pinning %d media pieces.', rows.length)
      results = rows
      let pool = new PromisePool(pinProducer, concurrency)
      return pool.start()
    })
}

function pinArtifact (row) {
  log.info('Pinning %s for %d bytes', row.fileAddress, row.bytes)
  return ipfs.pin.add(row.fileAddress).then(res => {
    log.info('%s pinned %d bytes', row.fileAddress, row.bytes)
    return db.run('UPDATE pinTracker SET pinCount = pinCount + 1, isPinned = $isPinned WHERE fileAddress = $fileAddress', {
      $fileAddress: row.fileAddress,
      $isPinned: 1
    })
  }).catch(err => {
    log.error('Failed to pin [%s]', row.fileAddress)
  })
}

let log = new Log(getConfig('logging.level', 'debug'),
  fs.createWriteStream(getConfig('logging.path', './pinbot.log'), {flags: 'a'}))
let concurrency = getConfig('concurrency', 5)
let diskUsageLimit = fsp(getConfig('resourceLimits.disk', '10GB'))
let minPinThreshold = getConfig('minPinThreshold', 1)
let stopPinLimit = getConfig('stopPinLimit', 10)
let libraryMediaURL = getConfig('libraryD.url', 'https://api.alexandria.io/alexandria/v2/media/get/all')
let ipfs = ipfsAPI(getConfig('IPFSNode', {host: 'localhost', port: '5001', protocol: 'http'}))

// Don't judge, I'm learning Promises as I go here and this will 100% be getting redone
Promise.resolve()
  .then(() => db.open(getConfig('database.db', ':memory:'), {Promise}))
  .then(() => db.migrate())
  .then(() => db.prepare('INSERT OR IGNORE INTO main.pinTracker (fileID, ipfsAddress) VALUES (?, ?);'))
  .then(stmt => stmtAddFileToDB = stmt)
  .then(refreshPublishedMedia)
  .then(updateFileSizes)
  .then(getMyIpfsId)
  .then(id => refreshPinCounts(id))
  .then(pinMedia)
  .catch(err => {
    console.log(err)
    console.error(err.stack)
  })
