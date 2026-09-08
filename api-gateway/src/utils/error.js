export class AppError extends Error {
     constructor(message, statusCode, code) {
          super(message);
          this.statusCode = statusCode;
          this.code = code; // custom error code
          Error.captureStackTrace(this, this.constructor);
     }
}

export class BadRequestError extends AppError {
     constructor(message, code = 'BAD_REQUEST') {
          super(message, 400, code);
     }
}

export class UnauthorizedError extends AppError {
     constructor(message, code = 'UNAUTHORIZED') {
          super(message, 401, code);
     }
}

export class ForbiddenError extends AppError {
     constructor(message, code = 'FORBIDDEN') {
          super(message, 403, code);
     }
}

export class NotFoundError extends AppError {
     constructor(message, code = 'NOT_FOUND') {
          super(message, 404, code);
     }
}

export class ConflictError extends AppError {
     constructor(message, code = 'CONFLICT') {
          super(message, 409, code);
     }
}

export class TooManyRequestsError extends AppError {
     constructor(message, code = 'TOO_MANY_REQUESTS') {
          super(message, 429, code);
     }
}

export class InternalServerError extends AppError {
     constructor(message = 'Internal Server Error', code = 'SERVER_ERROR') {
          super(message, 500, code);
     }
}

export class ServiceUnavailableError extends AppError {
     constructor(message, errorCode = 'SERVICE_UNAVAILABLE') {
          super(message, 503, errorCode);
     }
}

export class GatewayTimeoutError extends AppError {
     constructor(message, errorCode = 'GATEWAY_TIMEOUT') {
          super(message, 504, errorCode);
     }
}