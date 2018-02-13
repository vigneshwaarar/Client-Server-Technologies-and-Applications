CREATE DATABASE hadoopguide;

GRANT ALL PRIVILEGES ON hadoopguide.* TO 'root'@'localhost';


CREATE TABLE hadoopguide.widgets(
id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
widget_name VARCHAR(64) NOT NULL,
price DECIMAL(10,2),
design_date DATE,
version INT,
design_comment VARCHAR(100));

INSERT INTO hadoopguide.widgets VALUES(NULL, 'sprocket', 0.25, '2010-02-10', 1, 'Connects two gizmos');
INSERT INTO hadoopguide.widgets VALUES(NULL,'gizmo',4.00,'2009-11-30',4,NULL);
INSERT INTO hadoopguide.widgets VALUES(NULL,'gadget',99.99, '1983-08-13',13,'Our flagship product');