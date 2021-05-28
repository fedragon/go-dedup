CREATE TABLE IF NOT EXISTS media
(
    path      text not null primary key,
    hash      text not null,
    unix_time integer
);