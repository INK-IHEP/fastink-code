create table job_info_table(
	id INT AUTO_INCREMENT PRIMARY KEY,
	uid int not null,
	jobid int not null,
	outpath varchar(200),
	errpath varchar(200),
	job_type char(20),
	job_path varchar(255),
	job_status char(20),
	iptable_status smallint(5),
	iptable_clean tinyint,
	create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
