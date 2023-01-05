package com.github.shawven.calf.track.server.web;

public class Result {
    public static final String SUCCESS = "success";
    public static final String ERROR = "error";

    private String code;
    private String msg;

    public Result(String code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    public static Result success(String msg) {
        return new Result(SUCCESS, msg);
    }

    public static Result error(String msg) {
        return new Result(ERROR, msg);
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}
