-- Table: public.partition_logs

-- DROP TABLE IF EXISTS public.partition_logs;

CREATE TABLE IF NOT EXISTS public.partition_logs
(
    activity_name text COLLATE pg_catalog."default",
    log_time timestamp without time zone DEFAULT now(),
    log_level text COLLATE pg_catalog."default",
    log_message text COLLATE pg_catalog."default"
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.partition_logs
    OWNER to postgres;