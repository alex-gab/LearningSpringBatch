DROP TABLE IF EXISTS customer;
CREATE TABLE customer (
                               id INT NOT NULL auto_increment,
                               firstName VARCHAR(255) default NULL,
                               lastName VARCHAR(255) default NULL,
                               birthdate VARCHAR(255),
                               PRIMARY KEY (id)
);

DROP TABLE IF EXISTS new_customer;
CREATE TABLE new_customer (
                          id INT NOT NULL auto_increment,
                          firstName VARCHAR(255) default NULL,
                          lastName VARCHAR(255) default NULL,
                          birthdate VARCHAR(255),
                          PRIMARY KEY (id)
);