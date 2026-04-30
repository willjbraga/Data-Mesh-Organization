/* RH_Data_Mesh_simplificado_Modelo_Logico: */

CREATE TYPE enum_status_colaborador AS ENUM ('Ativo', 'Inativo', 'Afastado', 'Ferias');
CREATE TYPE enum_tipo_vinculo AS ENUM ('CLT', 'PJ', 'Estagio', 'Temporario');
CREATE TYPE enum_nivel AS ENUM ('Junior', 'Pleno', 'Senior', 'Especialista', 'Gestao');
CREATE TYPE enum_movimentacao AS ENUM ('Promocao', 'Transferencia', 'Readequacao');
CREATE TYPE enum_tipo_ausencia AS ENUM ('Ferias', 'Atestado', 'Licenca', 'Falta');
CREATE TYPE enum_aprovacao AS ENUM ('Pendente', 'Aprovado', 'Reprovado');
CREATE TYPE enum_status_vaga AS ENUM ('Aberta', 'Pausada', 'Fechada', 'Cancelada');
CREATE TYPE enum_status_candidatura AS ENUM ('Triagem', 'Entrevista', 'Aprovado', 'Reprovado');
CREATE TYPE enum_origem_candidatura AS ENUM ('LinkedIn', 'Gupy', 'Indicacao', 'Site');


CREATE TABLE Colaborador (
    Colaborador_id bigint PRIMARY KEY,
    Nome varchar(200) NOT NULL,
    Telefone varchar(20) NOT NULL,
    Data_admissao date NOT NULL,
    Status enum_status_colaborador NOT NULL,
    Tipo_vinculo enum_tipo_vinculo NOT NULL,
    Email varchar(200) NOT NULL UNIQUE,
    FK_cargo_id bigint NOT NULL,
    FK_departamento_id bigint NOT NULL,
    FK_gestor_id bigint
);

CREATE TABLE Cargo (
    Cargo_id bigint PRIMARY KEY,
    Nome_cargo varchar(50) NOT NULL UNIQUE,
    Nivel enum_nivel NOT NULL,
    Descricao text
);

CREATE TABLE Departamento (
    Departamento_id bigint PRIMARY KEY,
    Nome_departamento varchar(100) NOT NULL UNIQUE
);

CREATE TABLE Movimentacao_Colaborador (
    Movimentacao_id bigint PRIMARY KEY,
    Data_movimentacao date NOT NULL,
    Tipo_movimentacao enum_movimentacao NOT NULL,
    FK_colaborador_id bigint NOT NULL,
    FK_cargo_anterior bigint NOT NULL,
    FK_cargo_novo bigint NOT NULL,
    FK_departamento_anterior bigint NOT NULL,
    FK_departamento_novo bigint NOT NULL
);

CREATE TABLE Ausencia (
    Ausencia_id bigint PRIMARY KEY,
    Tipo_ausencia enum_tipo_ausencia NOT NULL,
    Status_aprovacao enum_aprovacao NOT NULL,
    Data_inicio date NOT NULL,
    Data_fim date NOT NULL,
    FK_colaborador_id bigint NOT NULL
);

CREATE TABLE Recrutamento_Vaga (
    Vaga_id bigint PRIMARY KEY,
    Titulo varchar(200) NOT NULL,
    Data_abertura date NOT NULL,
    Quantidade_vagas int NOT NULL,
    Status enum_status_vaga NOT NULL,
    FK_departamento_id bigint NOT NULL
);

CREATE TABLE Candidatura (
    Candidatura_id bigint PRIMARY KEY,
    Nome_candidato varchar(150) NOT NULL,
    Email_candidato varchar(200) NOT NULL,
    Status_candidatura enum_status_candidatura NOT NULL,
    Origem_candidatura enum_origem_candidatura NOT NULL,
    FK_vaga_id bigint NOT NULL
);
 
ALTER TABLE Colaborador ADD CONSTRAINT Ocupa
    FOREIGN KEY (FK_cargo_id)
    REFERENCES Cargo (Cargo_id);
 
ALTER TABLE Colaborador ADD CONSTRAINT Alocado
    FOREIGN KEY (FK_departamento_id)
    REFERENCES Departamento (Departamento_id);
 
ALTER TABLE Colaborador ADD CONSTRAINT Gerencia
    FOREIGN KEY (FK_gestor_id)
    REFERENCES Colaborador (Colaborador_id);
 
ALTER TABLE Movimentacao_Colaborador ADD CONSTRAINT Sai_de
    FOREIGN KEY (FK_departamento_anterior)
    REFERENCES Departamento (Departamento_id);
 
ALTER TABLE Movimentacao_Colaborador ADD CONSTRAINT Vai_para
    FOREIGN KEY (FK_departamento_novo)
    REFERENCES Departamento (Departamento_id);
 
ALTER TABLE Movimentacao_Colaborador ADD CONSTRAINT Associada
    FOREIGN KEY (FK_colaborador_id)
    REFERENCES Colaborador (Colaborador_id);
 
ALTER TABLE Movimentacao_Colaborador ADD CONSTRAINT Tinha
    FOREIGN KEY (FK_cargo_anterior)
    REFERENCES Cargo (Cargo_id);
 
ALTER TABLE Movimentacao_Colaborador ADD CONSTRAINT Assume
    FOREIGN KEY (FK_cargo_novo)
    REFERENCES Cargo (Cargo_id);
 
ALTER TABLE Ausencia ADD CONSTRAINT Registra
    FOREIGN KEY (FK_colaborador_id)
    REFERENCES Colaborador (Colaborador_id);
 
ALTER TABLE Recrutamento_Vaga ADD CONSTRAINT Oferta
    FOREIGN KEY (FK_departamento_id)
    REFERENCES Departamento (Departamento_id);
 
ALTER TABLE Candidatura ADD CONSTRAINT Recebe
    FOREIGN KEY (FK_vaga_id)
    REFERENCES Recrutamento_Vaga (Vaga_id);