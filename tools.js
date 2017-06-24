let config = require('config');
let log;


module.exports = {
    getConfig: function (key, defaultVal) {
        if (config.has(key)) {
            return config.get(key);
        }
        log.warning("Config key %s not found using default $s", key, defaultVal);
        return defaultVal;
    },

    setLogger: function (logger) {
        log = logger;
    },
    initializeDB: function (db) {
        log.notice('Initializing DB');
        db.migrate({ force: 'last' });
    }
};

