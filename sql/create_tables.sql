CREATE TABLE public.provincias (
	id SERIAL PRIMARY KEY,
	descripcion character varying(200)
	);

CREATE TABLE public.localidades (
	id SERIAL PRIMARY KEY,
	descripcion character varying(200),
	id_provincia integer REFERENCES public.provincias(id)
	);
	
CREATE TABLE public.categorias(
	id SERIAL PRIMARY KEY,
	descripcion character varying(200)
	);	
	
CREATE TABLE public.espacios_culturales_normalizado(
    id SERIAL PRIMARY KEY,
    id_localidad integer REFERENCES public.localidades(id), 
    id_categoria  integer REFERENCES public.categorias(id),
    nombre character varying(200) NOT NULL, 
    domicilio character varying(200), 
    cp character varying(200), 
    Latitud	character varying(200),
    Longitud character varying(200),
    telefono character varying(200), 
    mail character varying(200), 
    web character varying(200), 
    creado date
);	

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
    creado date
);

CREATE TABLE public.cines_indicadores(
provincia character varying(200),
cant_pantallas integer, 
cant_butacas integer, 
cant_espacios_incaa integer,
creado date
);

CREATE TABLE public.indicadores(
descripcion character varying(200),
Cant_registros integer,
creado date
);


CREATE TABLE public.censo(
    id_provincia integer REFERENCES public.provincias(id),     
    cant_habitantes integer,
    creado date
);


CREATE TABLE public.museos(
    id SERIAL PRIMARY KEY,
    id_categoria  integer REFERENCES public.categorias(id),
    id_localidad integer REFERENCES public.localidades(id),     
    nombre	character varying(200),
    direccion	character varying(200),
    piso	character varying(200),
    CP	character varying(200),
    cod_area character varying(200),
    telefono character varying(200),
    Mail character varying(200),
    Web	character varying(200),
    Latitud	character varying(200),
    Longitud character varying(200),
    TipoLatitudLongitud	character varying(200),
    Info_adicional	character varying(200),
    fuente	character varying(200),
    jurisdiccion character varying(200),
    anio_inauguracion character varying(200),
    actualizacion character varying(200),
    creado date
    );

    CREATE TABLE public.cines(
    id SERIAL PRIMARY KEY,
    id_categoria  integer REFERENCES public.categorias(id),
    id_localidad integer REFERENCES public.localidades(id),     
    nombre	character varying(200),	
    direccion	character varying(200),	
    piso	character varying(200),	
    cp	character varying(200),	
    web	character varying(200),	
    latitud	character varying(200),	
    longitud	character varying(200),	
    tipo_latitud_longitud	character varying(200),	
    fuente	character varying(200),	
    sector	character varying(200),	
    pantallas	character varying(200),	
    butacas	character varying(200),	
    tipo_de_gestion	character varying(200),	
    espacio_incaa	character varying(200),	
    anio_actualizacion character varying(200),
    creado date
    );


CREATE TABLE public.bibliotecas(
    id SERIAL PRIMARY KEY,
    id_categoria  integer REFERENCES public.categorias(id),
    id_localidad integer REFERENCES public.localidades(id),     
	nombre	 character varying(200),	
	domicilio	 character varying(200),	
	piso	 character varying(200),	
	cp	 character varying(200),	
	cod_tel	 character varying(200),	
	telefono	 character varying(200),	
	mail	 character varying(200),	
	web	 character varying(200),	
	informacion_adicional	 character varying(200),	
	latitud	 character varying(200),	
	longitud	 character varying(200),	
	tipo_latitud_longitud	 character varying(200),	
	fuente	 character varying(200),	
	fecha_fundacion	 character varying(200),	
	nro_conabip	 character varying(200),	
	anio_actualizacion  character varying(200),
	creado date
	);


CREATE TABLE public.raw_censo(
    jurisdiccion character varying(200),
    cant_habitantes integer,
    creado date
);



CREATE TABLE public.raw_museos(
    Cod_Loc character varying(200),	
    IdProvincia	character varying(200),
    IdDepartamento	character varying(200),
    Observaciones	character varying(200),
    categoria	character varying(200),
    subcategoria	character varying(200),
    provincia	character varying(200),
    localidad	character varying(200),
    nombre	character varying(200),
    direccion	character varying(200),
    piso	character varying(200),
    CP	character varying(200),
    cod_area character varying(200),
    telefono character varying(200),
    Mail character varying(200),
    Web	character varying(200),
    Latitud	character varying(200),
    Longitud character varying(200),
    TipoLatitudLongitud	character varying(200),
    Info_adicional	character varying(200),
    fuente	character varying(200),
    jurisdiccion character varying(200),
    anio_inauguracion character varying(200),
    actualizacion character varying(200),
    creado date
    );

    CREATE TABLE public.raw_cines(
    cod_localidad character varying(200),	
    id_provincia  character varying(200),		
    id_departamento	 character varying(200),	
    categoria	character varying(200),	
    provincia	character varying(200),	
    departamento	character varying(200),	
    localidad	character varying(200),	
    nombre	character varying(200),	
    direccion	character varying(200),	
    piso	character varying(200),	
    cp	character varying(200),	
    web	character varying(200),	
    latitud	character varying(200),	
    longitud	character varying(200),	
    tipo_latitud_longitud	character varying(200),	
    fuente	character varying(200),	
    sector	character varying(200),	
    pantallas	character varying(200),	
    butacas	character varying(200),	
    tipo_de_gestion	character varying(200),	
    espacio_incaa	character varying(200),	
    anio_actualizacion character varying(200),
    creado date
    );


CREATE TABLE public.raw_bibliotecas(
	cod_localidad  character varying(200),	
	id_provincia  character varying(200),	
	id_departamento	 character varying(200),	
	observacion	 character varying(200),	
	categoria	 character varying(200),	
	subcategoria	 character varying(200),	
	provincia	 character varying(200),	
	departamento	 character varying(200),	
	localidad	 character varying(200),	
	nombre	 character varying(200),	
	domicilio	 character varying(200),	
	piso	 character varying(200),	
	cp	 character varying(200),	
	cod_tel	 character varying(200),	
	telefono	 character varying(200),	
	mail	 character varying(200),	
	web	 character varying(200),	
	informacion_adicional	 character varying(200),	
	latitud	 character varying(200),	
	longitud	 character varying(200),	
	tipo_latitud_longitud	 character varying(200),	
	fuente	 character varying(200),	
	fecha_fundacion	 character varying(200),	
	nro_conabip	 character varying(200),	
	anio_actualizacion  character varying(200),
	creado date
	);