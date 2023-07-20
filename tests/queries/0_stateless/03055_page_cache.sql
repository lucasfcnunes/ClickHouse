-- Tags: no-parallel
--       ^ because we look at server-wide counters about page cache usage

set force_enable_page_cache = 1;
set page_cache_inject_eviction = 0;
set enable_filesystem_cache = 0;

create temporary table e as select * from system.events;
create view events_diff as
    -- round all stats to 70 MiB to leave a lot of leeway for overhead
    with if(event like '%Bytes%', 70*1024*1024, 35) as granularity
    select event, intDiv(new.value - old.value, granularity) as diff
    from system.events new
    left outer join e old
    on old.event = new.event
    where diff != 0 and
          event in ('PageCacheChunkMisses', 'PageCacheChunkShared', 'PageCacheChunkDataHits', 'PageCacheChunkDataPartialHits', 'PageCacheChunkDataMisses', 'PageCacheBytesUnpinnedRoundedToPages', 'PageCacheBytesUnpinnedRoundedToHugePages', 'ReadBufferFromS3Bytes')
    order by event;

drop table if exists page_cache_03055;
create table page_cache_03055 (k Int64 CODEC(NONE)) engine MergeTree order by k settings storage_policy = 's3_cache';

-- Write an 80 MiB file (40 x 2 MiB chunks), and a few small files.
insert into page_cache_03055 select * from numbers(10485760) settings max_block_size=100000000, preferred_block_size_bytes=1000000000;

select * from events_diff;

-- Cold read, should miss cache. (Populating cache on write is not implemented yet.)

select sum(k) from page_cache_03055;

select * from events_diff;
truncate table e;
insert into e select * from system.events;

-- Repeat read, should hit cache.

select sum(k) from page_cache_03055;

select * from events_diff;
truncate table e;
insert into e select * from system.events;

-- Drop cache and read again, should miss. Also don't write to cache.

system drop page cache;

select sum(k) from page_cache_03055 settings read_from_page_cache_if_exists_otherwise_bypass_cache = 1;

select * from events_diff;
truncate table e;
insert into e select * from system.events;

-- Repeat read, should still miss, but populate cache.

select sum(k) from page_cache_03055;

select * from events_diff;
truncate table e;
insert into e select * from system.events;

-- Read again, hit the cache.

select sum(k) from page_cache_03055 settings read_from_page_cache_if_exists_otherwise_bypass_cache = 1;

select * from events_diff;
truncate table e;
insert into e select * from system.events;


drop table page_cache_03055;
drop view events_diff;
