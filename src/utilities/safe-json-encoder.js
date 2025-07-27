function encodeLastKeyForQueryParam(keyObj) {
    return encodeURIComponent(JSON.stringify(keyObj));
}

module.exports = encodeLastKeyForQueryParam