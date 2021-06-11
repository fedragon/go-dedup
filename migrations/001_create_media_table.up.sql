CREATE TABLE IF NOT EXISTS hashes
(
    id integer primary key,
    hash text not null unique
);

CREATE TABLE IF NOT EXISTS media
(
    hash_id references hash(id),
    path      text not null primary key,
    unix_time integer not null
);