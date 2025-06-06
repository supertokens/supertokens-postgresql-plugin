docker run --rm --name postgres \
    -e 'POSTGRES_USER=root' \
    -e 'POSTGRES_PASSWORD=root' \
    -d -p 5432:5432 \
    -v ~/Desktop/db/pstgres:/var/lib/postgresql/data \
    postgres \
    -c 'max_connections=1000' \
    -c 'autovacuum_naptime=1' \
    -c 'autovacuum_vacuum_threshold=10' \
    -c 'autovacuum_analyze_threshold=10'

sleep 30

docker exec postgres psql -U root root -c 'create database supertokens;'
docker exec postgres psql -U root root -c 'create database st0;'
docker exec postgres psql -U root root -c 'create database st1;'
docker exec postgres psql -U root root -c 'create database st2;'
docker exec postgres psql -U root root -c 'create database st3;'
docker exec postgres psql -U root root -c 'create database st4;'
docker exec postgres psql -U root root -c 'create database st5;'
docker exec postgres psql -U root root -c 'create database st6;'
docker exec postgres psql -U root root -c 'create database st7;'
docker exec postgres psql -U root root -c 'create database st8;'
docker exec postgres psql -U root root -c 'create database st9;'
docker exec postgres psql -U root root -c 'create database st10;'
docker exec postgres psql -U root root -c 'create database st11;'
docker exec postgres psql -U root root -c 'create database st12;'
docker exec postgres psql -U root root -c 'create database st13;'
docker exec postgres psql -U root root -c 'create database st14;'
docker exec postgres psql -U root root -c 'create database st15;'
docker exec postgres psql -U root root -c 'create database st16;'
docker exec postgres psql -U root root -c 'create database st17;'
docker exec postgres psql -U root root -c 'create database st18;'
docker exec postgres psql -U root root -c 'create database st19;'
docker exec postgres psql -U root root -c 'create database st20;'
docker exec postgres psql -U root root -c 'create database st21;'
docker exec postgres psql -U root root -c 'create database st22;'
docker exec postgres psql -U root root -c 'create database st23;'
docker exec postgres psql -U root root -c 'create database st24;'
docker exec postgres psql -U root root -c 'create database st25;'
docker exec postgres psql -U root root -c 'create database st26;'
docker exec postgres psql -U root root -c 'create database st27;'
docker exec postgres psql -U root root -c 'create database st28;'
docker exec postgres psql -U root root -c 'create database st29;'
docker exec postgres psql -U root root -c 'create database st30;'
docker exec postgres psql -U root root -c 'create database st31;'
docker exec postgres psql -U root root -c 'create database st32;'
docker exec postgres psql -U root root -c 'create database st33;'
docker exec postgres psql -U root root -c 'create database st34;'
docker exec postgres psql -U root root -c 'create database st35;'
docker exec postgres psql -U root root -c 'create database st36;'
docker exec postgres psql -U root root -c 'create database st37;'
docker exec postgres psql -U root root -c 'create database st38;'
docker exec postgres psql -U root root -c 'create database st39;'
docker exec postgres psql -U root root -c 'create database st40;'
docker exec postgres psql -U root root -c 'create database st41;'
docker exec postgres psql -U root root -c 'create database st42;'
docker exec postgres psql -U root root -c 'create database st43;'
docker exec postgres psql -U root root -c 'create database st44;'
docker exec postgres psql -U root root -c 'create database st45;'
docker exec postgres psql -U root root -c 'create database st46;'
docker exec postgres psql -U root root -c 'create database st47;'
docker exec postgres psql -U root root -c 'create database st48;'
docker exec postgres psql -U root root -c 'create database st49;'
docker exec postgres psql -U root root -c 'create database st50;'
