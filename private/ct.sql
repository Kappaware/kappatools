
CREATE TABLE logs2
(
	rid BIGSERIAL PRIMARY KEY,
	type  CHARACTER VARYING,
  	src_timestamp TIMESTAMP with time zone,
  	in_timestamp TIMESTAMP with time zone,
	hostname  CHARACTER VARYING,
	file  CHARACTER VARYING,
	position BIGINT,
	message  CHARACTER VARYING,
	rhostname CHARACTER VARYING,
	islog boolean,
	mimetype  CHARACTER VARYING,
	filekey  CHARACTER VARYING,
	remote CHARACTER VARYING,
	gate_id CHARACTER VARYING,
	src_counter BIGINT,
	in_counter BIGINT,
	key_counter BIGINT,
    --kfk_value CHARACTER VARYING,
  	--kfk_key CHARACTER VARYING,
	kfk_topic CHARACTER VARYING,
  	kfk_partition INTEGER,
  	kfk_offset BIGINT
)
WITH (
  OIDS=FALSE
);
ALTER TABLE test1  OWNER TO postgres;

CREATE INDEX logs2_in_timestamp_idx
  ON logs2
  USING btree
  (in_timestamp);


