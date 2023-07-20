#include "CachedInMemoryReadBufferFromFile.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_END_OF_FILE;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
}

CachedInMemoryReadBufferFromFile::CachedInMemoryReadBufferFromFile(
    FileChunkAddress cache_key_, PageCachePtr cache_, std::unique_ptr<ReadBufferFromFileBase> in_, const ReadSettings & settings_)
    : ReadBufferFromFileBase(0, nullptr, 0, in_->getFileSize()), cache_key(cache_key_), cache(cache_), settings(settings_), in(std::move(in_))
    , read_until_position(file_size.value())
{
    chassert(file_size.has_value());
    cache_key.offset = 0;
}

String CachedInMemoryReadBufferFromFile::getFileName() const
{
    return in->getFileName();
}

off_t CachedInMemoryReadBufferFromFile::seek(off_t off, int whence)
{
    if (whence != SEEK_SET)
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Only SEEK_SET mode is allowed.");

    size_t offset = static_cast<size_t>(off);
    if (offset > file_size.value())
        throw Exception(ErrorCodes::SEEK_POSITION_OUT_OF_BOUND, "Seek position is out of bounds. Offset: {}", off);

    if (offset >= file_offset_of_buffer_end - working_buffer.size() && offset <= file_offset_of_buffer_end)
    {
        pos = working_buffer.end() - (file_offset_of_buffer_end - offset);
        chassert(getPosition() == off);
        return off;
    }

    resetWorkingBuffer();

    file_offset_of_buffer_end = offset;
    chunk.reset();

    return off;
}

off_t CachedInMemoryReadBufferFromFile::getPosition()
{
    return file_offset_of_buffer_end - available();
}

size_t CachedInMemoryReadBufferFromFile::getFileOffsetOfBufferEnd() const
{
    return file_offset_of_buffer_end;
}

void CachedInMemoryReadBufferFromFile::setReadUntilPosition(size_t position)
{
    read_until_position = position;
    if (position < static_cast<size_t>(getPosition()))
    {
        resetWorkingBuffer();
        chunk.reset();
    }
    else if (position < file_offset_of_buffer_end)
    {
        size_t diff = file_offset_of_buffer_end - position;
        working_buffer.resize(working_buffer.size() - diff);
        file_offset_of_buffer_end -= diff;
    }
}

void CachedInMemoryReadBufferFromFile::setReadUntilEnd()
{
    setReadUntilPosition(file_size.value());
}

bool CachedInMemoryReadBufferFromFile::nextImpl()
{
    chassert(read_until_position <= file_size.value());
    if (file_offset_of_buffer_end >= read_until_position)
        return false;

    if (chunk.has_value() && file_offset_of_buffer_end >= cache_key.offset + cache->chunkSize())
    {
        chassert(file_offset_of_buffer_end == cache_key.offset + cache->chunkSize());
        chunk.reset();
    }

    if (!chunk.has_value())
    {
        cache_key.offset = file_offset_of_buffer_end / cache->chunkSize() * cache->chunkSize();
        chunk = cache->getOrSet(cache_key.hash(), settings.read_from_page_cache_if_exists_otherwise_bypass_cache, settings.page_cache_inject_eviction);

        size_t chunk_size = std::min(cache->chunkSize(), file_size.value() - cache_key.offset);

        std::unique_lock download_lock(chunk->chunk()->state.download_mutex);

        if (!chunk->isPrefixPopulated(chunk_size))
        {
            /// A few things could be improved here, which may or may not be worth the added complexity:
            ///  * If the next file chunk is in cache, use in->setReadUntilPosition() to limit the read to
            ///    just one chunk. More generally, look ahead in the cache to count how many next chunks
            ///    need to be downloaded. (Up to some limit? And avoid changing `in`'s until-position if
            ///    it's already reasonable; otherwise we'd increase it by one chunk every chunk, discarding
            ///    a half-completed HTTP request every time.)
            ///  * If only a subset of pages are missing from this chunk, download only them.
            ///    (With some threshold for avoiding short seeks.)
            ///    This is probably not actually useful in practice because our PageCache normally uses
            ///    2 MiB transparent huge pages (because MADV_FREE is too slow without them), with
            ///    just one OS page per chunk.
            ///  * If our [position, read_until_position) covers only part of the chunk, we could download
            ///    just that part. (Which would be bad if someone else needs the rest of the chunk and has
            ///    to do a whole new HTTP request to get it. Unclear what the policy should be.)
            ///  * Instead of doing in->readInto() in a loop until we get the whole chunk, we could return the
            ///    results as soon as in->readInto() produces them.
            ///    (But this would make the download_mutex situation much more complex, similar to the
            ///    FileSegment::State::PARTIALLY_DOWNLOADED and FileSegment::setRemoteFileReader() stuff.)

            size_t pos = 0;
            while (pos < chunk_size)
            {
                auto r = in->readInto(chunk->chunk()->data + pos, chunk_size - pos, cache_key.offset + pos, /* ignore */ 0);
                chassert(r.offset + r.size <= chunk_size - pos);

                if (r.size <= r.offset)
                    throw Exception(ErrorCodes::UNEXPECTED_END_OF_FILE, "File {} ended after {} bytes, but we expected {}",
                        getFileName(), cache_key.offset + pos, file_size.value());

                if (r.offset != 0)
                    memmove(chunk->chunk()->data + pos, chunk->chunk()->data + pos + r.offset, r.size - r.offset);

                pos += r.size - r.offset;
            }

            chunk->markPrefixPopulated(chunk_size);
        }
    }

    nextimpl_working_buffer_offset = file_offset_of_buffer_end - cache_key.offset;
    BufferBase::set(
        chunk->chunk()->data,
        std::min(chunk->chunk()->size, read_until_position - cache_key.offset),
        nextimpl_working_buffer_offset);
    file_offset_of_buffer_end += available();

    return true;
}

IAsynchronousReader::Result CachedInMemoryReadBufferFromFile::readInto(char * data, size_t size, size_t offset, size_t ignore)
{
    IAsynchronousReader::Result res;

    seek(static_cast<off_t>(offset + ignore), SEEK_SET);

    if (available() != 0 || next())
    {
        res.size = std::min(available(), size);

        /// TODO: Find a way to avoid this memcpy. AsynchronousBoundedReadBuffer will need to be able to
        ///       hold PinnedPageChunk-s (probably wrapped in a shared_ptr<Buffer> for type erasure).
        memcpy(data, position(), res.size);
    }

    return res;
}

}
