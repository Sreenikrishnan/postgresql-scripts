-- Table: public.part_tbl

-- DROP TABLE IF EXISTS public.part_tbl;

CREATE TABLE IF NOT EXISTS public.part_tbl
(
    table_name text COLLATE pg_catalog."default" NOT NULL,
    partition_type character(1) COLLATE pg_catalog."default",
    retention_duration text COLLATE pg_catalog."default",
    partition_key text COLLATE pg_catalog."default",
    CONSTRAINT part_tbl_pkey PRIMARY KEY (table_name)
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.part_tbl
    OWNER to postgres;