CREATE TABLE IF NOT EXISTS experiments
(
    EXP_ID  VARCHAR(255) NOT NULL,
    NAME    VARCHAR(255),
    CREATOR VARCHAR(255),
    START   DOUBLE,
    END     DOUBLE,
    CREATED DOUBLE,
    STATUS  VARCHAR(255) NOT NULL,
    CONSTRAINT experiments_pk PRIMARY KEY (EXP_ID)
);

CREATE TABLE IF NOT EXISTS nodeinfo
(
    EXP_ID      VARCHAR(255) NOT NULL,
    NODE        VARCHAR(255) NOT NULL,
    INFO_KEY    VARCHAR(255),
    INFO_VALUE  VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS telemetry
(
    EXP_ID    VARCHAR(255) NOT NULL,
    TIMESTAMP DOUBLE       NOT NULL,
    METRIC    varchar(255) NOT NULL,
    NODE      varchar(255) NOT NULL,
    VALUE     DOUBLE       NOT NULL
);

CREATE TABLE IF NOT EXISTS events
(
    EXP_ID      VARCHAR(255) NOT NULL,
    TIMESTAMP   DOUBLE       NOT NULL,
    NAME        VARCHAR(255) NOT NULL,
    VALUE       VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS traces
(
    REQUEST_ID  VARCHAR(36)  NOT NULL,
    CLIENT      VARCHAR(255) NOT NULL,
    SERVICE     VARCHAR(255) NOT NULL,
    CREATED     DOUBLE       NOT NULL,
    SENT        DOUBLE       NOT NULL,
    DONE        DOUBLE       NOT NULL,
    STATUS      INT,
    RESPONSE    VARCHAR(255),
    SERVER      VARCHAR (255),
    EXP_ID      VARCHAR (255),
    CONSTRAINT traces_pk PRIMARY KEY (REQUEST_ID)
);
