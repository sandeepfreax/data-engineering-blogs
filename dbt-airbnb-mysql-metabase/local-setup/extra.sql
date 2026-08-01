use airbnb;

show tables;

drop table if exists raw_listings;

CREATE TABLE raw_listings(id int,
  listing_url varchar(500),
  name varchar(500),
  room_type varchar(50),
  minimum_nights int,
  host_id int,
  price varchar(10),
  created_at datetime,
  updated_at datetime);

LOAD DATA INFILE '/var/lib/mysql-files/listings/listings.csv'
    INTO TABLE raw_listings
    FIELDS
    TERMINATED BY ','
    OPTIONALLY ENCLOSED BY '"'
    LINES
    TERMINATED BY '\n'
    IGNORE 1 LINES
(id, listing_url, name, room_type, minimum_nights, host_id, price, @created, @updated)
SET
    created_at = STR_TO_DATE(@created, '%Y-%m-%dT%H:%i:%sZ'),
    updated_at = STR_TO_DATE(@updated, '%Y-%m-%dT%H:%i:%sZ');

select * from raw_listings limit 10;

drop table if exists raw_reviews;

CREATE TABLE raw_reviews
                    (listing_id int,
                     date datetime,
                     reviewer_name varchar(100),
                     comments varchar(10000),
                     sentiment varchar(100));

LOAD DATA INFILE '/var/lib/mysql-files/reviews/reviews.csv'
    INTO TABLE raw_reviews
    FIELDS
    TERMINATED BY ','
    OPTIONALLY ENCLOSED BY '"'
    LINES
    TERMINATED BY '\n'
    IGNORE 1 LINES
(listing_id, @date, reviewer_name, comments, sentiment)
SET
    date = STR_TO_DATE(@date, '%Y-%m-%dT%H:%i:%sZ');

select * from raw_reviews limit 10;

drop table if exists raw_hosts;

CREATE TABLE raw_hosts
                    (id int,
                     name varchar(100),
                     is_superhost varchar(10),
                     created_at datetime,
                     updated_at datetime);

LOAD DATA INFILE '/var/lib/mysql-files/hosts/hosts.csv'
    INTO TABLE raw_hosts
    FIELDS
    TERMINATED BY ','
    OPTIONALLY ENCLOSED BY '"'
    LINES
    TERMINATED BY '\n'
    IGNORE 1 LINES;

select * from raw_hosts limit 10;
