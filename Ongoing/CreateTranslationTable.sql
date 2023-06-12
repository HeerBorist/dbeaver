-- Drop table

-- DROP TABLE public.capturetranslations;

CREATE TABLE public.capturetranslations (
	"type" varchar(9) NOT NULL,
	typeid int4 NOT NULL,
	parentmoduleid int4 NOT NULL,
	parentobjectid int4 NOT NULL,
	consecutivenumber int4 NOT NULL,
	id int4 NOT NULL,
	"name" varchar(255) NOT NULL,
	"language" varchar(5) NOT NULL,
	"translation" varchar(1000) NULL,
	skey varchar(14) NOT NULL,
	uniquekey varchar(25) NOT NULL,
	CONSTRAINT pk_translations PRIMARY KEY (uniquekey)
);
