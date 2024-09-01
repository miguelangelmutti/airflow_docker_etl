CREATE TABLE public.espacios_culturales (
    id SERIAL PRIMARY KEY,
    cod_localidad integer, 
    id_provincia integer, 
    id_departamento integer, 
    categoria character varying(200) NOT NULL, 
    provincia character varying(200), 
    localidad character varying(200), 
    nombre character varying(200) NOT NULL, 
    domicilio character varying(200), 
    cp character varying(200), 
    telefono character varying(200), 
    mail character varying(200), 
    web character varying(200), 
    creado timestamp without time zone
);

CREATE TABLE public.cines(
provincia character varying(200),
cant_pantallas integer, 
cant_butacas integer, 
cant_espacios_incaa integer
);

CREATE TABLE public.indicadores(
descripcion character varying(200),
Cant_registros integer
);