CREATE TABLE cdwdb_repro
(
 property_id              character varying(10)   encode lzo       ,
 property_name            character varying(100)  encode lzo       ,
 sob                      character varying(40)   encode lzo  PRIMARY KEY  ,
 is_logged_in             boolean                 encode runlength ,
 mobile_device_locale     character varying(20)   encode lzo       ,
 page_views               bigint                  encode bytedict  ,
 property_family          character varying(20)   encode bytedict  ,
 utc_date                 date                    ,
 core_action              character varying(20)   encode lzo       ,
 )
 DISTSTYLE KEY
 DISTKEY(sob)
 SORTKEY(utc_date);
