function log({ object_type, code, status, message, data }) {
  console.info({ object_type, code, status, message, data });
}

function sendResponse({ res, code, status, message, data }) {
  const success = status === 1;
  const responsePayload = {
    success,
    status,
    message,
    data,
  };

  console.log({ code, ...responsePayload });
  return res.status(code).send(responsePayload);
}

function createEvent({ object_type, code, status, message, data }) {
  const success = status === 1;
  return {
    object_type,
    code,
    status,
    success,
    message,
    data,
  };
}

const defaultParams = {
  success: { code: 200, status: 1 },
  failed: { code: 400, status: 0 },
  error: { code: 500, status: 0 },
  unavailable: { code: 404, status: 0 },
  unauthorized: { code: 403, status: 0 },
  unauthenticated: { code: 401, status: 0 },
  warning: { code: 206, status: 0 },
  security: { code: 419, status: 0 },
};

function wrapHandler(fn) {
  return params => {
    const defaults = defaultParams[fn.name] || {};
    return fn({ ...defaults, ...params });
  };
}

exports.handlers = {
  logger: {
    success: params => log({ ...defaultParams.success, ...params }),
    failed: params => log({ ...defaultParams.failed, ...params }),
    error: params => log({ ...defaultParams.error, ...params }),
    unavailable: params => log({ ...defaultParams.unavailable, ...params }),
    unauthorized: params => log({ ...defaultParams.unauthorized, ...params }),
    unauthenticated: params =>
      log({ ...defaultParams.unauthenticated, ...params }),
    warning: params => log({ ...defaultParams.warning, ...params }), // Added
    security: params => log({ ...defaultParams.security, ...params }),
  },
  response: {
    success: params => sendResponse({ ...defaultParams.success, ...params }),
    failed: params => sendResponse({ ...defaultParams.failed, ...params }),
    error: params => sendResponse({ ...defaultParams.error, ...params }),
    unavailable: params =>
      sendResponse({ ...defaultParams.unavailable, ...params }),
    unauthorized: params =>
      sendResponse({ ...defaultParams.unauthorized, ...params }),
    unauthenticated: params =>
      sendResponse({ ...defaultParams.unauthenticated, ...params }),
    warning: params => sendResponse({ ...defaultParams.warning, ...params }), // Added
    security: params => sendResponse({ ...defaultParams.security, ...params }),
  },
  event: {
    success: params => createEvent({ ...defaultParams.success, ...params }),
    failed: params => createEvent({ ...defaultParams.failed, ...params }),
    error: params => createEvent({ ...defaultParams.error, ...params }),
    unavailable: params =>
      createEvent({ ...defaultParams.unavailable, ...params }),
    unauthorized: params =>
      createEvent({ ...defaultParams.unauthorized, ...params }),
    unauthenticated: params =>
      createEvent({ ...defaultParams.unauthenticated, ...params }),
    warning: params => createEvent({ ...defaultParams.warning, ...params }), // Added
    security: params => createEvent({ ...defaultParams.security, ...params }),
  },
};
