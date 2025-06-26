function log({ object_type, code, status, message, data }) {
  console.info({ object_type, code, status, message, data });
}

function sendResponse({ res, code, status, message, data }) {
  console.log({ code, status, message, data });
  return res.status(code).send({ status, message, data });
}

function createEvent({ object_type, code, status, message, data }) {
  return { object_type, code, status, message, data };
}

const defaultParams = {
  success: { code: 200, status: 1 },
  failed: { code: 400, status: 0 },
  error: { code: 500, status: 0 },
  unavailable: { code: 404, status: 0 },
  unauthorized: { code: 403, status: 1 }
};

function wrapHandler(fn) {
  return (params) => {
    const defaults = defaultParams[fn.name] || {};
    return fn({ ...defaults, ...params });
  };
}

exports.handlers = {
  response: {
    success: (params) => sendResponse({ ...defaultParams.success, ...params }),
    failed: (params) => sendResponse({ ...defaultParams.failed, ...params }),
    error: (params) => sendResponse({ ...defaultParams.error, ...params }),
    unavailable: (params) =>
      sendResponse({ ...defaultParams.unavailable, ...params }),
    unauthorized: (params) =>
      sendResponse({ ...defaultParams.unauthorized, ...params })
  },
  event: {
    success: (params) => createEvent({ ...defaultParams.success, ...params }),
    failed: (params) => createEvent({ ...defaultParams.failed, ...params }),
    error: (params) => createEvent({ ...defaultParams.error, ...params }),
    unavailable: (params) =>
      createEvent({ ...defaultParams.unavailable, ...params }),
    unauthorized: (params) =>
      createEvent({ ...defaultParams.unauthorized, ...params })
  }
};
