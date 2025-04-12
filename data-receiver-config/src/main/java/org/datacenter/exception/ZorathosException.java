package org.datacenter.exception;

import lombok.Getter;

@SuppressWarnings("CallToPrintStackTrace")
@Getter
public class ZorathosException extends RuntimeException {
    private final String message;

    public ZorathosException(Throwable cause) {
        super(cause);
        cause.printStackTrace();
        this.message = cause.getMessage();
    }

    public ZorathosException(Throwable cause, String message) {
        super(cause);
        cause.printStackTrace();
        this.message = message;
    }

    public ZorathosException(String message) {
        super(message);
        this.message = message;
    }
}
