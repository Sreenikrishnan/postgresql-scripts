-- Table: public.tbl_part_dtls

-- DROP TABLE IF EXISTS public.tbl_part_dtls;

CREATE TABLE IF NOT EXISTS public.tbl_part_dtls
(
    tbl_name text COLLATE pg_catalog."default",
    part_key text COLLATE pg_catalog."default",
    part_type "char",
    part_name text COLLATE pg_catalog."default",
    st_date date,
    end_date date,
    created_on date
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public.tbl_part_dtls
    OWNER to postgres;