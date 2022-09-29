CREATE TABLE t_estab_saude (
    cod_ibge      NUMBER(10) NOT NULL,
    ano           NUMBER(10) NOT NULL,
    tipo_estab    VARCHAR2(100) NOT NULL,
    qt_estadual   NUMBER(10),
    qt_federal    NUMBER(10),
    qt_municipal  NUMBER(10),
    qt_particular NUMBER(10)
);

COMMENT ON COLUMN t_estab_saude.cod_ibge IS
    'Código IBGE do Município - Chave primária';

COMMENT ON COLUMN t_estab_saude.ano IS
    'Data de referencia - Ano';

COMMENT ON COLUMN t_estab_saude.tipo_estab IS
    'Tipo de estabelecimento de saúde';

COMMENT ON COLUMN t_estab_saude.qt_estadual IS
    'Número de estabelecimento de saúde - estadual';

COMMENT ON COLUMN t_estab_saude.qt_federal IS
    'Número de estabelecimento de saúde - federal';

COMMENT ON COLUMN t_estab_saude.qt_municipal IS
    'Número de estabelecimento de saúde - municipal';

COMMENT ON COLUMN t_estab_saude.qt_particular IS
    'Número de estabelecimento de saúde - particular';

ALTER TABLE t_estab_saude
    ADD CONSTRAINT t_estab_saude_pk PRIMARY KEY ( cod_ibge,
                                                  ano,
                                                  tipo_estab );

CREATE TABLE t_leitos (
    cod_ibge NUMBER(10) NOT NULL,
    ano      NUMBER(10) NOT NULL,
    qt_sus   NUMBER(10),
    qt_nsus  NUMBER(10)
);

COMMENT ON COLUMN t_leitos.cod_ibge IS
    'Código IBGE do Município - Chave primária';

COMMENT ON COLUMN t_leitos.ano IS
    'Data de referencia - Ano';

COMMENT ON COLUMN t_leitos.qt_sus IS
    'Número de Leitos SUS';

COMMENT ON COLUMN t_leitos.qt_nsus IS
    'Número de Leitos Não SUS';

ALTER TABLE t_leitos ADD CONSTRAINT t_leitos_pk PRIMARY KEY ( cod_ibge,
                                                              ano );

CREATE TABLE t_municipios (
    cod_ibge   NUMBER(10) NOT NULL,
    municipio  VARCHAR2(100),
    cod_ra     NUMBER(10),
    regiao_adm VARCHAR2(100),
    regiao     VARCHAR2(100),
    latitude   NUMBER(10, 3),
    longitude  NUMBER(10, 3)
);

COMMENT ON COLUMN t_municipios.cod_ibge IS
    'Código IBGE do Município - Chave primária';

COMMENT ON COLUMN t_municipios.municipio IS
    'Nome do Municipio';

COMMENT ON COLUMN t_municipios.cod_ra IS
    'Código da Região Administrativa';

COMMENT ON COLUMN t_municipios.regiao_adm IS
    'Nome da Região Administrativa';

COMMENT ON COLUMN t_municipios.regiao IS
    'Nome da Região Metropolitana';

COMMENT ON COLUMN t_municipios.latitude IS
    'Coordenadas geográficas - Latitude';

COMMENT ON COLUMN t_municipios.longitude IS
    'Coordenadas geográficas - Longitude';

ALTER TABLE t_municipios ADD CONSTRAINT t_municipios_pk PRIMARY KEY ( cod_ibge );

CREATE TABLE t_populacao (
    cod_ibge  NUMBER(10) NOT NULL,
    ano       NUMBER(10) NOT NULL,
    qt_pop    NUMBER(10)
);

COMMENT ON COLUMN t_populacao.cod_ibge IS
    'Código IBGE do Município - Chave primária';

COMMENT ON COLUMN t_populacao.ano IS
    'Data de referencia - Ano';

COMMENT ON COLUMN t_populacao.qt_pop IS
    'Número de pessoas/população';

ALTER TABLE t_populacao ADD CONSTRAINT t_populacao_pk PRIMARY KEY ( cod_ibge,
                                                                    ano );

CREATE TABLE t_prof_saude (
    cod_ibge  NUMBER(10) NOT NULL,
    ano       NUMBER(10) NOT NULL,
    tipo_prof VARCHAR2(100) NOT NULL,
    qt_sus    NUMBER(10),
    qt_nsus   NUMBER(10)
);

COMMENT ON COLUMN t_prof_saude.cod_ibge IS
    'Código IBGE do Município - Chave primária';

COMMENT ON COLUMN t_prof_saude.ano IS
    'Data de referencia - Ano';

COMMENT ON COLUMN t_prof_saude.tipo_prof IS
    'Categórico: Médico ou Enfermeiro';

COMMENT ON COLUMN t_prof_saude.qt_sus IS
    'Número de profissionais SUS';

COMMENT ON COLUMN t_prof_saude.qt_nsus IS
    'Número de profissionais não SUS';

ALTER TABLE t_prof_saude
    ADD CONSTRAINT t_prof_saude_pk PRIMARY KEY ( cod_ibge,
                                                 ano,
                                                 tipo_prof );

ALTER TABLE t_estab_saude
    ADD CONSTRAINT t_estab_saude_t_municipios_fk FOREIGN KEY ( cod_ibge )
        REFERENCES t_municipios ( cod_ibge );

ALTER TABLE t_leitos
    ADD CONSTRAINT t_leitos_t_municipios_fk FOREIGN KEY ( cod_ibge )
        REFERENCES t_municipios ( cod_ibge );

ALTER TABLE t_populacao
    ADD CONSTRAINT t_populacao_t_municipios_fk FOREIGN KEY ( cod_ibge )
        REFERENCES t_municipios ( cod_ibge );

ALTER TABLE t_prof_saude
    ADD CONSTRAINT t_prof_saude_t_municipios_fk FOREIGN KEY ( cod_ibge )
        REFERENCES t_municipios ( cod_ibge );