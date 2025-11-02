-- WARNING: This schema is for context only and is not meant to be run.
-- Table order and constraints may not be valid for execution.

CREATE TABLE public.bmw_cars (
  car_id integer NOT NULL,
  first_seen_date date NOT NULL,
  last_seen_date date NOT NULL,
  current_status text NOT NULL DEFAULT 'active'::text CHECK (current_status = ANY (ARRAY['active'::text, 'sold'::text])),
  created_at timestamp with time zone DEFAULT now(),
  updated_at timestamp with time zone DEFAULT now(),
  link text,
  CONSTRAINT bmw_cars_pkey PRIMARY KEY (car_id)
);
CREATE TABLE public.bmw_cars_equipment (
  id bigint NOT NULL DEFAULT nextval('bmw_cars_equipment_id_seq'::regclass),
  car_id integer NOT NULL CHECK (car_id > 0),
  category text NOT NULL,
  equipment_name text NOT NULL,
  valid_from date NOT NULL,
  valid_to date,
  is_latest boolean NOT NULL DEFAULT true,
  scrape_date timestamp with time zone NOT NULL,
  created_at timestamp with time zone DEFAULT now(),
  CONSTRAINT bmw_cars_equipment_pkey PRIMARY KEY (id),
  CONSTRAINT fk_bmw_cars_equipment_car_id FOREIGN KEY (car_id) REFERENCES public.bmw_cars(car_id)
);
CREATE TABLE public.bmw_cars_history (
  id bigint NOT NULL DEFAULT nextval('bmw_cars_history_id_seq'::regclass),
  car_id integer NOT NULL CHECK (car_id > 0),
  model_name text,
  price numeric,
  kilometers integer,
  registration_date date,
  horse_power_kw integer,
  horse_power_ps integer,
  battery_range_km integer,
  equipments jsonb,
  first_seen_date date NOT NULL,
  last_seen_date date NOT NULL,
  valid_from date NOT NULL,
  valid_to date,
  is_latest boolean NOT NULL DEFAULT true,
  status text NOT NULL DEFAULT 'active'::text CHECK (status = ANY (ARRAY['active'::text, 'sold'::text])),
  link text,
  scrape_date timestamp with time zone NOT NULL,
  created_at timestamp with time zone DEFAULT now(),
  CONSTRAINT bmw_cars_history_pkey PRIMARY KEY (id),
  CONSTRAINT fk_bmw_cars_history_car_id FOREIGN KEY (car_id) REFERENCES public.bmw_cars(car_id)
);
CREATE TABLE public.bmw_cars_scores (
  id bigint NOT NULL DEFAULT nextval('bmw_cars_scores_id_seq'::regclass),
  car_id integer NOT NULL CHECK (car_id > 0),
  value_efficiency_score numeric CHECK (value_efficiency_score IS NULL OR value_efficiency_score >= 0::numeric AND value_efficiency_score <= 100::numeric),
  age_usage_score numeric CHECK (age_usage_score IS NULL OR age_usage_score >= 0::numeric AND age_usage_score <= 100::numeric),
  performance_range_score numeric CHECK (performance_range_score IS NULL OR performance_range_score >= 0::numeric AND performance_range_score <= 100::numeric),
  equipment_score numeric CHECK (equipment_score IS NULL OR equipment_score >= 0::numeric AND equipment_score <= 100::numeric),
  final_score numeric CHECK (final_score IS NULL OR final_score >= 0::numeric AND final_score <= 100::numeric),
  valid_from date NOT NULL,
  valid_to date,
  is_latest boolean NOT NULL DEFAULT true,
  scrape_date timestamp with time zone NOT NULL,
  created_at timestamp with time zone DEFAULT now(),
  CONSTRAINT bmw_cars_scores_pkey PRIMARY KEY (id),
  CONSTRAINT fk_bmw_cars_scores_car_id FOREIGN KEY (car_id) REFERENCES public.bmw_cars(car_id)
);
CREATE TABLE public.bmw_preferences (
  id bigint NOT NULL DEFAULT nextval('bmw_preferences_id_seq'::regclass),
  preference_name text NOT NULL UNIQUE CHECK (preference_name IS NOT NULL AND preference_name <> ''::text),
  created_date date NOT NULL,
  total_desired_equipment integer NOT NULL,
  note text,
  desired_equipment jsonb NOT NULL CHECK (jsonb_typeof(desired_equipment) = 'array'::text),
  metadata jsonb,
  is_active boolean NOT NULL DEFAULT true,
  created_at timestamp with time zone DEFAULT now(),
  updated_at timestamp with time zone DEFAULT now(),
  CONSTRAINT bmw_preferences_pkey PRIMARY KEY (id)
);