select * from City where Name = 'Morn';
select * from City where Name >= 'Morn' and Name <= 'Tbessa';
create index name_idx on City (Name);
select * from City where Name = 'Morn';
select * from City where Name >= 'Morn' and Name <= 'Tbessa';

insert into City values (101,'Godoy Cruz','ARG','Mendoza',206998);
insert into City values (102,'Posadas','ARG','Misiones',201273);
insert into City values (103,'Guaymalln','ARG','Mendoza',200595);
insert into City values (104,'Santiago del Estero','ARG','Santiago del Estero',189947);
insert into City values (105,'San Salvador de Jujuy','ARG','Jujuy',178748);
select * from City where Name = 'Godoy Cruz';
drop index name_idx on City;
