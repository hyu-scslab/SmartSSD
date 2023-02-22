CREATE OR REPLACE FUNCTION MY_SCAN() RETURNS void AS
$$
DECLARE
	begintime timestamp;
	curtime timestamp;
BEGIN
	begintime := now();

END;

$$
LANGUAGE plpgsql;
