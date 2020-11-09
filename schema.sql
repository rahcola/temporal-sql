create extension btree_gist;

create table account (
    id bigserial primary key,
    name text not null
);

create table huippis (
    id bigserial primary key,
    name text not null
);

create table employment (
    id bigserial primary key,
    huippis_id bigint not null references huippis (id),
    valid_at tstzrange not null,
    constraint non_overlapping_employment
    exclude using gist (huippis_id with =, valid_at with &&)
);

create table staffing (
    id bigserial constraint staffing_primary_key primary key,
    account_id bigint not null references account (id),
    huippis_id bigint not null references huippis (id),
    valid_at tstzrange not null,
    constraint non_overlapping_staffings
    exclude using gist (account_id with =, huippis_id with =, valid_at with &&)
);

-- make employment bitemporal by adding system time and creating a history table

alter table employment
    add column system_time tstzrange not null;

create table employment_history (
    id bigint not null,
    huippis_id bigint not null references huippis (id),
    valid_at tstzrange not null,
    system_time tstzrange not null
);

-- allow only current time inserts and updates. needs to be disabled during
-- data restore

create function set_system_time() returns trigger as
$$
begin
    new.system_time := tstzrange(now(), null, '[)');

    return new;
end;
$$ language plpgsql;

create trigger set_employment_system_time
before insert or update on employment
for each row
execute procedure set_system_time();

-- archive rows before modification by inserting them to the history table

create function archive_employment() returns trigger as
$$
begin
    insert into employment_history (
        id,
        huippis_id,
        valid_at,
        system_time
    ) values (
        old.id,
        old.huippis_id,
        old.valid_at,
        tstzrange(lower(old.system_time),
                  coalesce(lower(new.system_time), now()),
                  '[)')
    );

    return null;
end;
$$ language plpgsql security definer;

create trigger archive_employment
after update or delete on employment
for each row
execute procedure archive_employment();

-- add temporal foreign key constraint staffing -> employment

create function check_employed_during_staffing() returns trigger as
$$
begin
    if (select current_setting('transaction_isolation') <> 'serializable') then
        raise exception 'Serializable transaction isolation required';
    end if;

    if (not exists (select 1
                    from employment as e
                    where e.huippis_id = new.huippis_id and
                          e.valid_at && new.valid_at and
                          new.valid_at &> e.valid_at) or
        exists (select 1
                from employment as e1
                where e1.huippis_id = new.huippis_id and
                      e1.valid_at && new.valid_at and
                      not new.valid_at &< e1.valid_at and
                      not exists (select 1
                                  from employment as e2
                                  where e2.huippis_id = e1.huippis_id and
                                        e2.valid_at -|- e1.valid_at and
                                        e2.valid_at >> e1.valid_at))) then
        raise foreign_key_violation
              using message = 'insert or update on table "staffing" violates foreign key constraint "staffing_references_employment"',
                     detail = ('Key (huippis_id,valid_at)=(' ||
                              new.huippis_id || ',' || new.valid_at ||
                              ') is not present in table "employment"');
    end if;

    return null;
end;
$$ language plpgsql;

create constraint trigger staffing_references_employment
after insert or update on staffing
for each row
execute procedure check_employed_during_staffing();

create function check_no_dangling_staffing() returns trigger as
$$
begin
    if (select current_setting('transaction_isolation') <> 'serializable') then
        raise exception 'Serializable transaction isolation required';
    end if;

    if exists (select
               from staffing as s
               where s.huippis_id = old.huippis_id and
                     s.valid_at && old.valid_at and
                     (not exists (select 1
                                  from employment as e
                                  where e.huippis_id = s.huippis_id and
                                        e.valid_at && s.valid_at and
                                        s.valid_at &> e.valid_at) or
                      exists (select 1
                              from employment as e1
                              where e1.huippis_id = s.huippis_id and
                                    e1.valid_at && s.valid_at and
                                    not s.valid_at &< e1.valid_at and
                                    not exists (select 1
                                                from employment as e2
                                                where e2.huippis_id = e1.huippis_id and
                                                      e2.valid_at -|- e1.valid_at and
                                                      e2.valid_at >> e1.valid_at)))) then
        raise foreign_key_violation
              using message = 'update or delete on table "employment" violates foreign key constraint "employment_referenced_by_staffings"',
                     detail = ('Key (huippis_id,valid_at)=(' ||
                              old.huippis_id || ',' || old.valid_at ||
                              ') is still referenced from table "staffing"');
    end if;

    return null;
end;
$$ language plpgsql;

create constraint trigger employment_referenced_by_staffings
after update or delete on employment
deferrable
for each row
execute procedure check_no_dangling_staffing();

-- extra tricks

-- make staffing bitemporal by adding system time without a history table

-- alter table staffing
--     add column system_time tstzrange not null default tstzrange(now(), null, '[)'),
--     drop constraint staffing_primary_key,
--     drop constraint non_overlapping_staffings,
--     add constraint staffing_primary_key
--     exclude using gist (id with =, system_time with &&)
--     deferrable,
--     add constraint non_overlapping_staffings
--     exclude using gist (account_id with =, huippis_id with =, valid_at with &&, system_time with &&)
--     deferrable;

-- permit only current time modifications to the staffing table

-- create function only_current_time_operations_permitted() returns trigger as
-- $$
-- begin
--     raise exception 'Only current time operations permitted';
-- end;
-- $$ language plpgsql;

-- create constraint trigger only_current_time_inserts_to_staffing
-- after insert on staffing
-- for each row
-- when (not new.system_time = tstzrange(now(), null, '[)'))
-- execute procedure only_current_time_operations_permitted();

-- create constraint trigger only_current_time_updates_to_staffing
-- after update on staffing
-- for each row
-- when (not (upper_inf(old.system_time) and
--            new.system_time = tstzrange(lower(old.system_time), now(), '[)') and
--            new.id = old.id and
--            new.account_id = old.account_id and
--            new.huippis_id = old.huippis_id and
--            new.valid_at = old.valid_at))
-- execute procedure only_current_time_operations_permitted();

-- create constraint trigger only_current_time_deletes_to_staffing
-- after delete on staffing
-- for each row
-- execute procedure only_current_time_operations_permitted();

-- modify "normal" update to behave like a current time update

-- create function current_time_update_to_staffing() returns trigger as
-- $$
-- begin
--     set constraints non_overlapping_staffings deferred;
--     set constraints staffing_primary_key deferred;

--     insert into staffing (
--         id,
--         account_id,
--         huippis_id,
--         valid_at
--     ) values (
--         new.id,
--         new.account_id,
--         new.huippis_id,
--         new.valid_at
--     );

--     update staffing as s
--     set system_time = tstzrange(lower(s.system_time), now(), '[)')
--     where s = old;

--     set constraints non_overlapping_staffings immediate;
--     set constraints staffing_primary_key immediate;

--     return null;
-- end;
-- $$ language plpgsql;

-- create trigger current_time_update_to_staffing
-- before update on staffing
-- for each row
-- when (upper_inf(old.system_time) and
--       upper_inf(new.system_time) and
--       new.id = old.id)
-- execute procedure current_time_update_to_staffing();

-- modify "normal" delete to behave like a current time delete

-- create function current_time_delete_to_staffing() returns trigger as
-- $$
-- begin
--     update staffing as s
--     set system_time = tstzrange(lower(s.system_time), now(), '[)')
--     where s = old;

--     return null;
-- end;
-- $$ language plpgsql;

-- create trigger current_time_delete_to_staffing
-- before delete on staffing
-- for each row
-- when (upper_inf(old.system_time))
-- execute procedure current_time_delete_to_staffing();

-- create aggregate range_merge (tstzrange) (
--     sfunc = range_merge,
--     stype = tstzrange,
--     initcond = 'empty'
-- );

-- create function ensure_continuous_employment() returns trigger as
-- $$
-- begin
--     set constraints non_overlapping_employment deferred;
--     new.valid_at := range_merge(new.valid_at,
--                                (select range_merge(e.valid_at)
--                                 from employment as e
--                                 where e.huippis_id = new.huippis_id and
--                                       (e.valid_at && new.valid_at or
--                                        e.valid_at -|- new.valid_at)));
--     return new;
-- end;
-- $$ language plpgsql;

-- create function remove_overlapping_employment() returns trigger as
-- $$
-- begin
--     delete from employment as e
--     where e.huippis_id = new.huippis_id and
--           e.valid_at && new.valid_at and
--           e.valid_at <> new.valid_at;
--     return new;
-- end;
-- $$ language plpgsql;

-- create trigger ensure_continuous_employment
-- before insert or update on employment
-- for each row
-- execute procedure ensure_continuous_employment();

-- create trigger remove_overlapping_employment
-- after insert or update on employment
-- for each row
-- execute procedure remove_overlapping_employment();

-- create or replace function check_employed_during_staffing() returns trigger as
-- $$
-- begin
--     if (select upper_inf(new.system_time) and
--                not exists (select 1
--                            from employment as e
--                            where e.huippis_id = new.huippis_id and
--                                  e.valid_at @> new.valid_at)) then
--         raise exception 'Huippis % is/was not employed for the whole staffing', new.huippis_id;
--     end if;
--     return new;
-- end;
-- $$ language plpgsql;

-- create or replace function check_no_dangling_staffing() returns trigger as
-- $$
-- begin
--     if (select exists (select 1
--                        from staffing as s
--                        where s.huippis_id = old.huippis_id and
--                              old.valid_at @> s.valid_at and
--                              upper_inf(s.system_time) and
--                              not exists (select 1
--                                          from employment as e
--                                          where e.huippis_id = s.huippis_id and
--                                                e.valid_at @> s.valid_at))) then
--         raise exception 'Huippis % is/was staffed during the employment', old.huippis_id;
--     end if;
--     return new;
-- end;
-- $$ language plpgsql;

insert into account (name) values ('Adidas');
insert into huippis (name) values ('K. K. Konsultti');

begin transaction isolation level serializable;
insert into employment (huippis_id, valid_at)
(select id, '[2018-05-01, 2018-09-01)' from huippis);

insert into staffing (account_id, huippis_id, valid_at)
(select a.id, h.id, '[2018-06-01, 2018-08-01)'
 from account as a, huippis as h);
commit;

begin transaction isolation level serializable;
update employment
set valid_at = tstzrange(lower(valid_at), null, '[)');
commit;

begin transaction isolation level serializable;
update staffing
set valid_at = tstzrange(lower(valid_at), '2019-01-01', '[)');
commit;
