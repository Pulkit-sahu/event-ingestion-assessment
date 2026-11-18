class AppError(Exception):
    status = 400
    code = "bad_request"

    def __init__(self, message, details=None, status=None, code=None):
        super().__init__(message)
        self.message = message
        self.details = details or []
        if status is not None:
            self.status = status
        if code is not None:
            self.code = code


class ValidationError(AppError):
    status = 400
    code = "validation_error"


class ConflictError(AppError):
    status = 409
    code = "conflict"


class PayloadTooLarge(AppError):
    status = 413
    code = "payload_too_large"
