create database hedgefund_db;

show databases;

use hedgefund_db;

create table stock_prices (
	id INT AUTO_INCREMENT PRIMARY KEY,
    ticker VARCHAR(10),
    date DATE,
    open DECIMAL(10,2),
    high DECIMAL(10,2),
    low DECIMAL(10,2),
    close DECIMAL(10,2),
    volume BIGINT
);

