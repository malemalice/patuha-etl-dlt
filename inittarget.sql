CREATE TABLE IF NOT EXISTS corez_transaksi_scrap (
  id_transaksi varchar(50) NOT NULL,
  detailid int(1) NOT NULL,
  id_program varchar(30) NOT NULL COMMENT 'ID Program',
  dtu datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id_transaksi,detailid)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;