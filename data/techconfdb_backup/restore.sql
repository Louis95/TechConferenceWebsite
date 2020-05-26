--
-- NOTE:
--
-- File paths need to be edited. Search for $$PATH$$ and
-- replace it with the path to the directory containing
-- the extracted data files.
--
--
-- PostgreSQL database dump
--

-- Dumped from database version 12.2
-- Dumped by pg_dump version 12.2

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

DROP DATABASE techconfdb;
--
-- Name: techconfdb; Type: DATABASE; Schema: -; Owner: -
--

CREATE DATABASE techconfdb WITH TEMPLATE = template0 ENCODING = 'UTF8' LC_COLLATE = 'English_United States.1252' LC_CTYPE = 'English_United States.1252';


\connect techconfdb

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: attendee; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.attendee (
    id integer NOT NULL,
    first_name text NOT NULL,
    last_name text NOT NULL,
    conference_id integer NOT NULL,
    job_position text NOT NULL,
    email text NOT NULL,
    company text NOT NULL,
    city text NOT NULL,
    state text NOT NULL,
    interests text,
    submitted_date timestamp with time zone NOT NULL,
    comments text
);


--
-- Name: attendee_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.attendee ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.attendee_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: conference; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.conference (
    id integer NOT NULL,
    name character varying(100) NOT NULL,
    active bit(1) NOT NULL,
    date date NOT NULL,
    price double precision NOT NULL,
    address character varying(300) NOT NULL
);


--
-- Name: conference_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.conference ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.conference_id_seq
    START WITH 2
    INCREMENT BY 1
    MINVALUE 2
    MAXVALUE 99999
    CACHE 1
);


--
-- Name: notification; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.notification (
    id integer NOT NULL,
    status text NOT NULL,
    message text NOT NULL,
    submitted_date timestamp with time zone,
    completed_date timestamp with time zone,
    subject text NOT NULL
);


--
-- Name: notification_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.notification ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.notification_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Data for Name: attendee; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.attendee (id, first_name, last_name, conference_id, job_position, email, company, city, state, interests, submitted_date, comments) FROM stdin;
\.
COPY public.attendee (id, first_name, last_name, conference_id, job_position, email, company, city, state, interests, submitted_date, comments) FROM '$$PATH$$/2836.dat';

--
-- Data for Name: conference; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.conference (id, name, active, date, price, address) FROM stdin;
\.
COPY public.conference (id, name, active, date, price, address) FROM '$$PATH$$/2834.dat';

--
-- Data for Name: notification; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.notification (id, status, message, submitted_date, completed_date, subject) FROM stdin;
\.
COPY public.notification (id, status, message, submitted_date, completed_date, subject) FROM '$$PATH$$/2838.dat';

--
-- Name: attendee_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.attendee_id_seq', 5, true);


--
-- Name: conference_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.conference_id_seq', 2, true);


--
-- Name: notification_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.notification_id_seq', 4, true);


--
-- Name: attendee attendee_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.attendee
    ADD CONSTRAINT attendee_pkey PRIMARY KEY (id);


--
-- Name: conference conference_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.conference
    ADD CONSTRAINT conference_pkey PRIMARY KEY (id);


--
-- Name: notification notification_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.notification
    ADD CONSTRAINT notification_pkey PRIMARY KEY (id);


--
-- Name: attendee conference; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.attendee
    ADD CONSTRAINT conference FOREIGN KEY (conference_id) REFERENCES public.conference(id);


--
-- PostgreSQL database dump complete
--

